import {
  Connection,
  TYPE_ADD,
  TYPE_ARITH,
  TYPE_CAS,
  TYPE_DELETE,
  TYPE_GET,
  TYPE_GETS,
  TYPE_NOOP,
  TYPE_SET,
  TYPE_STATS,
  TYPE_VERSION,
  type ClientOptions
} from './connection.ts'
import { ValidationError } from './errors.ts'

const DEFAULT_PORT = 11211
const CRLF = '\r\n'

// Printable ASCII, no whitespace or control characters, at most 250 bytes
const KEY_EXPRESSION = /^[\x21-\x7e]{1,250}$/
// A stats subcommand is a single printable ASCII token
const STATS_SUBCOMMAND_EXPRESSION = /^[\x21-\x7e]{1,250}$/
const CAS_EXPRESSION = /^\d+$/

export interface ServerAddress {
  host?: string
  port?: number
}

export interface StoreOptions {
  /**
   * Expiration time in seconds. 0 (the default) means the item never expires.
   * Memcached only supports second granularity: round sub-second TTLs up.
   * Values greater than 30 days are interpreted by the server as absolute
   * Unix timestamps.
   */
  ttl?: number
}

export interface DeleteOptions {
  /**
   * When provided, the item is only deleted if its current CAS token matches.
   */
  cas?: string | number | bigint
}

export interface GetsResult {
  value: Buffer
  /**
   * Opaque CAS token to pass to cas() or delete().
   */
  cas: string
}

function parseAddress (url: string | ServerAddress): { host: string, port: number, secure: boolean } {
  if (typeof url === 'object' && url !== null) {
    return { host: url.host ?? 'localhost', port: Number(url.port ?? DEFAULT_PORT), secure: false }
  }

  if (typeof url !== 'string' || url.length === 0) {
    throw new ValidationError('The url must be a string or an object with host and port properties')
  }

  let address = url
  let secure = false
  if (address.startsWith('memcached://')) {
    address = address.slice(12)
  } else if (address.startsWith('memcacheds://')) {
    address = address.slice(13)
    secure = true
  }

  let parsed
  try {
    parsed = new URL('memcached://' + address)
  } catch (cause) {
    throw new ValidationError(`Invalid server address: ${url}`, { cause })
  }

  let host = parsed.hostname
  if (host.length === 0) {
    throw new ValidationError(`Invalid server address: ${url}`)
  }

  // net.connect wants IPv6 addresses without brackets
  if (host.charCodeAt(0) === 91) {
    host = host.slice(1, -1)
  }

  return { host, port: parsed.port.length > 0 ? Number(parsed.port) : DEFAULT_PORT, secure }
}

function validateKey (key: string): void {
  if (typeof key !== 'string' || !KEY_EXPRESSION.test(key)) {
    throw new ValidationError(
      'Keys must be non-empty strings of at most 250 printable ASCII characters and cannot contain whitespace or control characters'
    )
  }
}

function validateValue (value: Buffer | string): Buffer {
  if (Buffer.isBuffer(value)) {
    return value
  }

  if (typeof value === 'string') {
    return Buffer.from(value)
  }

  throw new ValidationError('Values must be buffers or strings')
}

function validateTTL (ttl: number | undefined): number {
  if (ttl === undefined) {
    return 0
  }

  if (!Number.isSafeInteger(ttl) || ttl < 0) {
    throw new ValidationError('The ttl must be a non-negative integer number of seconds')
  }

  return ttl
}

function validateCas (cas: string | number | bigint): string {
  if (typeof cas === 'number' || typeof cas === 'bigint') {
    cas = cas.toString()
  }

  if (typeof cas !== 'string' || !CAS_EXPRESSION.test(cas)) {
    throw new ValidationError('The cas token must be a string of digits (as returned by gets), a number or a bigint')
  }

  return cas
}

function validateDelta (delta: number | bigint): string {
  if ((typeof delta !== 'number' && typeof delta !== 'bigint') || (typeof delta === 'number' && !Number.isSafeInteger(delta))) {
    throw new ValidationError('The delta must be an integer number or a bigint')
  }

  if (delta < 1) {
    throw new ValidationError('The delta must be a positive integer')
  }

  return delta.toString()
}

export class Client {
  #connection: Connection
  #opaque = 0

  /**
   * Creates a client connected to a single memcached server.
   *
   * @param url `'host:port'`, `'memcached://host:port'`,
   *            `'memcacheds://host:port'` (TLS) or `{ host, port }`.
   *            Defaults to `localhost:11211`.
   */
  constructor (url: string | ServerAddress = 'localhost:11211', options: ClientOptions = {}) {
    const { host, port, secure } = parseAddress(url)

    // The memcacheds:// scheme is shorthand for tls: true. An explicit tls
    // options object still applies, so certificates can be configured.
    if (secure && (typeof options.tls !== 'object' || options.tls === null)) {
      options = { ...options, tls: true }
    }

    this.#connection = new Connection(host, port, options)
  }

  // Internal, exposed for tests only
  get connection (): Connection {
    return this.#connection
  }

  /**
   * Returns the value for the key, or `null` on a miss.
   */
  get (key: string): Promise<Buffer | null> {
    validateKey(key)
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_GET, `mg ${key} v O${opaque}${CRLF}`, opaque)
  }

  /**
   * Returns the value and its CAS token, or `null` on a miss.
   */
  gets (key: string): Promise<GetsResult | null> {
    validateKey(key)
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_GETS, `mg ${key} v c O${opaque}${CRLF}`, opaque)
  }

  /**
   * Unconditionally stores the value. Throws on failure.
   */
  set (key: string, value: Buffer | string, options?: StoreOptions): Promise<void> {
    return this.#store(TYPE_SET, '', key, value, options)
  }

  /**
   * Stores the value only if the key does not exist yet.
   * Returns `false` if the key already exists.
   */
  add (key: string, value: Buffer | string, options?: StoreOptions): Promise<boolean> {
    return this.#store(TYPE_ADD, ' ME', key, value, options)
  }

  /**
   * Stores the value only if the current CAS token matches.
   * Returns `false` on CAS mismatch or if the key does not exist.
   */
  cas (key: string, value: Buffer | string, cas: string | number | bigint, options?: StoreOptions): Promise<boolean> {
    return this.#store(TYPE_CAS, ` C${validateCas(cas)}`, key, value, options)
  }

  /**
   * Deletes the key. Returns `false` on a miss, or on CAS mismatch when
   * `options.cas` is provided.
   */
  delete (key: string, options?: DeleteOptions): Promise<boolean> {
    validateKey(key)
    const cas = options?.cas !== undefined ? ` C${validateCas(options.cas)}` : ''
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_DELETE, `md ${key}${cas} O${opaque}${CRLF}`, opaque)
  }

  /**
   * Increments the numeric value of the key by delta (default 1).
   * Returns the new value, or `null` if the key does not exist.
   */
  incr (key: string, delta: number | bigint = 1): Promise<bigint | null> {
    return this.#arithmetic('I', key, delta)
  }

  /**
   * Decrements the numeric value of the key by delta (default 1), clamping
   * at 0. Returns the new value, or `null` if the key does not exist.
   */
  decr (key: string, delta: number | bigint = 1): Promise<bigint | null> {
    return this.#arithmetic('D', key, delta)
  }

  /**
   * Sends a meta no-op, useful as a pipeline fence.
   */
  noop (): Promise<void> {
    return this.#connection.execute(TYPE_NOOP, `mn${CRLF}`)
  }

  /**
   * Returns the server version string, useful as a health check.
   */
  version (): Promise<string> {
    return this.#connection.execute(TYPE_VERSION, `version${CRLF}`)
  }

  /**
   * Returns server statistics as a name/value map, useful for observability
   * (connection counts, evictions, hit/miss ratios, memory usage).
   *
   * An optional subcommand selects a specific domain, e.g. `'items'`,
   * `'slabs'` or `'settings'`. Only `END`-terminated subcommands are
   * supported.
   */
  stats (subcommand?: string): Promise<Record<string, string>> {
    if (subcommand === undefined) {
      return this.#connection.execute(TYPE_STATS, `stats${CRLF}`)
    }

    if (typeof subcommand !== 'string' || !STATS_SUBCOMMAND_EXPRESSION.test(subcommand)) {
      throw new ValidationError(
        'The stats subcommand must be a non-empty string of at most 250 printable ASCII characters and cannot contain whitespace or control characters'
      )
    }

    return this.#connection.execute(TYPE_STATS, `stats ${subcommand}${CRLF}`)
  }

  /**
   * Waits for in-flight commands to settle, then closes the connection.
   */
  close (): Promise<void> {
    return this.#connection.close()
  }

  #store<T> (type: number, extra: string, key: string, value: Buffer | string, options?: StoreOptions): Promise<T> {
    validateKey(key)
    const data = validateValue(value)
    const ttl = validateTTL(options?.ttl)
    const opaque = this.#nextOpaque()

    // Single buffer for header, data block and terminator: one socket write
    const header = `ms ${key} ${data.length}${extra} T${ttl} O${opaque}${CRLF}`
    const payload = Buffer.allocUnsafe(header.length + data.length + 2)
    payload.write(header, 0, 'latin1')
    data.copy(payload, header.length)
    payload[payload.length - 2] = 13
    payload[payload.length - 1] = 10

    return this.#connection.execute(type, payload, opaque)
  }

  #arithmetic (mode: 'I' | 'D', key: string, delta: number | bigint): Promise<bigint | null> {
    validateKey(key)
    const encoded = validateDelta(delta)
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_ARITH, `ma ${key} v M${mode} D${encoded} O${opaque}${CRLF}`, opaque)
  }

  #nextOpaque (): string {
    this.#opaque = (this.#opaque + 1) & 0x3fffffff
    return this.#opaque.toString()
  }
}
