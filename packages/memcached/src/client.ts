import {
  Connection,
  TYPE_ADD,
  TYPE_ARITH,
  TYPE_CAS,
  TYPE_DELETE,
  TYPE_GET,
  TYPE_GETS,
  TYPE_CACHEDUMP,
  TYPE_NOOP,
  TYPE_SET,
  TYPE_STATS,
  TYPE_STATS_RESET,
  TYPE_VERSION,
  type CachedumpItem,
  type ClientMetrics,
  type ClientOptions
} from './connection.ts'
import { ValidationError } from './errors.ts'
import { HashRing } from './ring.ts'

const DEFAULT_PORT = 11211
const CRLF = '\r\n'

// Printable ASCII, no whitespace or control characters, at most 250 bytes
const KEY_EXPRESSION = /^[\x21-\x7e]{1,250}$/
// A stats subcommand is a single printable ASCII token
const STATS_SUBCOMMAND_EXPRESSION = /^[\x21-\x7e]{1,250}$/
const CAS_EXPRESSION = /^\d+$/
// The authentication payload is space-delimited and line-terminated, so
// credentials must be printable ASCII without whitespace or control characters
const CREDENTIAL_EXPRESSION = /^[\x21-\x7e]+$/

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

export interface ServerStats {
  host: string
  port: number
  /**
   * The server's statistics, or `null` when the query failed.
   */
  stats: Record<string, string> | null
  /**
   * The failure reason, or `null` when the query succeeded.
   */
  error: Error | null
}

interface ParsedAddress {
  host: string
  port: number
  secure: boolean
  username?: string
  password?: string
}

function parseAddress (url: string | ServerAddress): ParsedAddress {
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

  let username
  let password
  if (parsed.username.length > 0 || parsed.password.length > 0) {
    try {
      username = decodeURIComponent(parsed.username)
      password = decodeURIComponent(parsed.password)
    } catch (cause) {
      throw new ValidationError(`Invalid credentials in server address: ${url}`, { cause })
    }
  }

  return { host, port: parsed.port.length > 0 ? Number(parsed.port) : DEFAULT_PORT, secure, username, password }
}

function validateCredentials (username: string | undefined, password: string | undefined): { username?: string, password?: string } {
  if (username === undefined && password === undefined) {
    return {}
  }

  if (username === undefined || password === undefined) {
    throw new ValidationError('The username and password must be provided together')
  }

  if (
    typeof username !== 'string' || !CREDENTIAL_EXPRESSION.test(username) ||
    typeof password !== 'string' || !CREDENTIAL_EXPRESSION.test(password)
  ) {
    throw new ValidationError(
      'Credentials must be non-empty strings of printable ASCII characters and cannot contain whitespace or control characters'
    )
  }

  return { username, password }
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

function statsCommand (subcommand: string | undefined): string {
  if (subcommand === undefined) {
    return `stats${CRLF}`
  }

  if (typeof subcommand !== 'string' || !STATS_SUBCOMMAND_EXPRESSION.test(subcommand)) {
    throw new ValidationError(
      'The stats subcommand must be a non-empty string of at most 250 printable ASCII characters and cannot contain whitespace or control characters'
    )
  }

  // These two subcommands do not answer with STAT lines and an END
  // terminator, so they need the dedicated methods to parse correctly
  if (subcommand === 'reset') {
    throw new ValidationError("The 'reset' stats subcommand is not END-terminated: use resetStats() instead")
  }

  if (subcommand === 'cachedump') {
    throw new ValidationError("The 'cachedump' stats subcommand is not END-terminated: use cachedump(slab, limit) instead")
  }

  return `stats ${subcommand}${CRLF}`
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
  // All connections, node-major: the pool for node n starts at n * poolSize
  #connections: Connection[]
  #poolSize: number
  #ring: HashRing | null
  #opaque = 0

  /**
   * Creates a client. Pass a single address to talk to one server, or an
   * array of addresses to shard keys across several servers with
   * ketama-style consistent hashing (see the README for the routing and
   * failure semantics).
   *
   * @param servers `'host:port'`, `'memcached://host:port'`,
   *                `'memcacheds://host:port'` (TLS),
   *                `'memcached://user:pass@host:port'`, `{ host, port }`
   *                or an array of those. Defaults to `localhost:11211`.
   */
  constructor (servers: string | ServerAddress | Array<string | ServerAddress> = 'localhost:11211', options: ClientOptions = {}) {
    const list = Array.isArray(servers) ? servers : [servers]

    if (list.length === 0) {
      throw new ValidationError('At least one server address must be provided')
    }

    const addresses = list.map(parseAddress)
    const seen = new Set<string>()

    for (const { host, port } of addresses) {
      const id = `${host}:${port}`

      if (seen.has(id)) {
        throw new ValidationError(`Duplicate server address: ${id}`)
      }

      seen.add(id)
    }

    const poolSize = options.poolSize ?? 1
    if (!Number.isSafeInteger(poolSize) || poolSize < 1) {
      throw new ValidationError('The poolSize option must be a positive integer')
    }

    this.#poolSize = poolSize
    this.#connections = []

    for (const { host, port, secure, username, password } of addresses) {
      let connectionOptions = options

      // The memcacheds:// scheme is shorthand for tls: true, per address so
      // mixed fleets are possible. An explicit tls options object still
      // applies, so certificates can be configured.
      if (secure && (typeof connectionOptions.tls !== 'object' || connectionOptions.tls === null)) {
        connectionOptions = { ...connectionOptions, tls: true }
      }

      // Explicit options take precedence over credentials embedded in the URL
      const credentials =
        options.username !== undefined || options.password !== undefined
          ? validateCredentials(options.username, options.password)
          : validateCredentials(username, password)

      for (let i = 0; i < poolSize; i++) {
        this.#connections.push(new Connection(host, port, { ...connectionOptions, ...credentials }))
      }
    }

    this.#ring = addresses.length > 1 ? new HashRing(addresses) : null
  }

  // Internal, exposed for tests only
  get connection (): Connection {
    return this.#connections[0]
  }

  // Internal, exposed for tests only
  get connections (): Connection[] {
    return this.#connections
  }

  /**
   * Returns the value for the key, or `null` on a miss.
   */
  get (key: string): Promise<Buffer | null> {
    validateKey(key)
    const opaque = this.#nextOpaque()
    return this.#connectionFor(key).execute(TYPE_GET, `mg ${key} v O${opaque}${CRLF}`, opaque, key)
  }

  /**
   * Returns the value and its CAS token, or `null` on a miss.
   */
  gets (key: string): Promise<GetsResult | null> {
    validateKey(key)
    const opaque = this.#nextOpaque()
    return this.#connectionFor(key).execute(TYPE_GETS, `mg ${key} v c O${opaque}${CRLF}`, opaque, key)
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
    return this.#connectionFor(key).execute(TYPE_DELETE, `md ${key}${cas} O${opaque}${CRLF}`, opaque, key)
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
   * Sends a meta no-op to every connection of every server, useful as a
   * pipeline fence.
   */
  async noop (): Promise<void> {
    await Promise.all(this.#connections.map(connection => connection.execute<void>(TYPE_NOOP, `mn${CRLF}`)))
  }

  /**
   * Returns the server version string, useful as a health check. Every
   * connection of every server is queried (so a single unreachable node
   * makes this reject) and the first server's version is returned.
   */
  async version (): Promise<string> {
    const versions = await Promise.all(
      this.#connections.map(connection => connection.execute<string>(TYPE_VERSION, `version${CRLF}`))
    )
    return versions[0]
  }

  /**
   * Returns a snapshot of client metrics: monotonic counters plus the
   * current pending-queue depth, aggregated across every connection of
   * every server (all pool members of all nodes).
   */
  metrics (): ClientMetrics {
    const snapshot: ClientMetrics = {
      commands: { issued: 0, completed: 0, failed: 0, byVerb: {} },
      pipeline: { pendingDepth: 0, writes: 0, flushes: 0 },
      connection: { connects: 0, disconnects: 0, reconnectAttempts: 0 },
      bytes: { read: 0, written: 0 }
    }

    for (const connection of this.#connections) {
      connection.collectMetrics(snapshot)
    }

    return snapshot
  }

  /**
   * Returns server statistics as a name/value map, useful for observability
   * (connection counts, evictions, hit/miss ratios, memory usage).
   *
   * An optional subcommand selects a specific domain, e.g. `'items'`,
   * `'slabs'` or `'settings'`. Only `END`-terminated subcommands are
   * supported: use `resetStats()` and `cachedump()` for the two subcommands
   * with a different response shape. With multiple servers, the first
   * server's stats are returned; use statsAll() for per-node visibility.
   */
  stats (subcommand?: string): Promise<Record<string, string>> {
    return this.#connections[0].execute(TYPE_STATS, statsCommand(subcommand))
  }

  /**
   * Returns statistics for every server, one entry per node in constructor
   * order. Nodes are queried concurrently and failures are reported per
   * entry: `stats` is the name/value map and `error` is `null` on success,
   * while on failure `stats` is `null` and `error` carries the reason. The
   * promise never rejects because of an unreachable node, so a dashboard
   * still sees the healthy part of the fleet. Takes the same optional
   * subcommand as stats() and throws `ValidationError` synchronously when it
   * is invalid.
   */
  statsAll (subcommand?: string): Promise<ServerStats[]> {
    const payload = statsCommand(subcommand)
    const nodes = this.#connections.length / this.#poolSize
    const queries: Array<Promise<ServerStats>> = []

    // Stats are per server, not per connection: query each node's first pool member
    for (let i = 0; i < nodes; i++) {
      const connection = this.#connections[i * this.#poolSize]

      queries.push(
        connection.execute<Record<string, string>>(TYPE_STATS, payload).then(
          stats => ({ host: connection.host, port: connection.port, stats, error: null }),
          error => ({ host: connection.host, port: connection.port, stats: null, error })
        )
      )
    }

    return Promise.all(queries)
  }

  /**
   * Resets the server statistics counters (`stats reset`): `get_hits`,
   * `get_misses`, `cmd_get`, eviction counters and so on go back to zero.
   * Gauges like `curr_connections` or `bytes` are unaffected. With multiple
   * servers, only the first server is reset.
   */
  resetStats (): Promise<void> {
    return this.#connections[0].execute(TYPE_STATS_RESET, `stats reset${CRLF}`)
  }

  /**
   * Dumps the keys stored in a slab class (`stats cachedump`), returning
   * for each item its key, value size in bytes and expiration time as an
   * absolute Unix timestamp (0 when the item never expires). `limit` caps
   * the number of returned items; 0 (the default) means no limit. Slab class
   * ids can be discovered via `stats('items')` or `stats('slabs')`.
   *
   * Caveats: `cachedump` is an unofficial debugging command that may change
   * or disappear in any memcached release. The dump is capped server-side
   * (about 2MB of response data), so it is not guaranteed to list every key,
   * and newly stored items may not appear until the LRU maintainer has
   * processed them. On old servers it holds the cache lock while dumping, so
   * do not use it against busy production servers. With multiple servers,
   * only the first server is dumped.
   */
  cachedump (slab: number, limit: number = 0): Promise<CachedumpItem[]> {
    if (!Number.isSafeInteger(slab) || slab < 1) {
      throw new ValidationError('The slab class id must be a positive integer')
    }

    if (!Number.isSafeInteger(limit) || limit < 0) {
      throw new ValidationError('The limit must be a non-negative integer')
    }

    return this.#connections[0].execute(TYPE_CACHEDUMP, `stats cachedump ${slab} ${limit}${CRLF}`)
  }

  /**
   * Waits for in-flight commands to settle, then closes all connections.
   */
  async close (): Promise<void> {
    await Promise.all(this.#connections.map(connection => connection.close()))
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

    return this.#connectionFor(key).execute(type, payload, opaque, key)
  }

  #arithmetic (mode: 'I' | 'D', key: string, delta: number | bigint): Promise<bigint | null> {
    validateKey(key)
    const encoded = validateDelta(delta)
    const opaque = this.#nextOpaque()
    return this.#connectionFor(key).execute(TYPE_ARITH, `ma ${key} v M${mode} D${encoded} O${opaque}${CRLF}`, opaque, key)
  }

  // Key to node routing: a single server bypasses hashing entirely. Within a
  // node's pool the connection with the fewest outstanding commands is
  // picked, so small operations do not queue behind large value transfers.
  #connectionFor (key: string): Connection {
    const node = this.#ring === null ? 0 : this.#ring.lookup(key)

    if (this.#poolSize === 1) {
      return this.#connections[node]
    }

    const base = node * this.#poolSize
    let best = this.#connections[base]

    for (let i = 1; i < this.#poolSize; i++) {
      const candidate = this.#connections[base + i]

      if (candidate.pending < best.pending) {
        best = candidate
      }
    }

    return best
  }

  #nextOpaque (): string {
    this.#opaque = (this.#opaque + 1) & 0x3fffffff
    return this.#opaque.toString()
  }
}
