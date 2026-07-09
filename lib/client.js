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
  TYPE_VERSION
} from './connection.js'
import { ValidationError } from './errors.js'

const DEFAULT_PORT = 11211
const CRLF = '\r\n'

// Printable ASCII, no whitespace or control characters, at most 250 bytes
const KEY_EXPRESSION = /^[\x21-\x7e]{1,250}$/
const CAS_EXPRESSION = /^\d+$/

function parseAddress (url) {
  if (typeof url === 'object' && url !== null) {
    return { host: url.host ?? 'localhost', port: Number(url.port ?? DEFAULT_PORT) }
  }

  if (typeof url !== 'string' || url.length === 0) {
    throw new ValidationError('The url must be a string or an object with host and port properties')
  }

  let address = url
  if (address.startsWith('memcached://')) {
    address = address.slice(12)
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

  return { host, port: parsed.port.length > 0 ? Number(parsed.port) : DEFAULT_PORT }
}

function validateKey (key) {
  if (typeof key !== 'string' || !KEY_EXPRESSION.test(key)) {
    throw new ValidationError(
      'Keys must be non-empty strings of at most 250 printable ASCII characters and cannot contain whitespace or control characters'
    )
  }
}

function validateValue (value) {
  if (Buffer.isBuffer(value)) {
    return value
  }

  if (typeof value === 'string') {
    return Buffer.from(value)
  }

  throw new ValidationError('Values must be buffers or strings')
}

function validateTTL (ttl) {
  if (ttl === undefined) {
    return 0
  }

  if (!Number.isSafeInteger(ttl) || ttl < 0) {
    throw new ValidationError('The ttl must be a non-negative integer number of seconds')
  }

  return ttl
}

function validateCas (cas) {
  if (typeof cas === 'number' || typeof cas === 'bigint') {
    cas = cas.toString()
  }

  if (typeof cas !== 'string' || !CAS_EXPRESSION.test(cas)) {
    throw new ValidationError('The cas token must be a string of digits (as returned by gets), a number or a bigint')
  }

  return cas
}

function validateDelta (delta) {
  if ((typeof delta !== 'number' && typeof delta !== 'bigint') || (typeof delta === 'number' && !Number.isSafeInteger(delta))) {
    throw new ValidationError('The delta must be an integer number or a bigint')
  }

  if (delta < 1) {
    throw new ValidationError('The delta must be a positive integer')
  }

  return delta.toString()
}

export class Client {
  #connection
  #opaque = 0

  constructor (url = 'localhost:11211', options = {}) {
    const { host, port } = parseAddress(url)
    this.#connection = new Connection(host, port, options)
  }

  // Internal, exposed for tests only
  get connection () {
    return this.#connection
  }

  get (key) {
    validateKey(key)
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_GET, `mg ${key} v O${opaque}${CRLF}`, opaque)
  }

  gets (key) {
    validateKey(key)
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_GETS, `mg ${key} v c O${opaque}${CRLF}`, opaque)
  }

  set (key, value, options) {
    return this.#store(TYPE_SET, '', key, value, options)
  }

  add (key, value, options) {
    return this.#store(TYPE_ADD, ' ME', key, value, options)
  }

  cas (key, value, cas, options) {
    return this.#store(TYPE_CAS, ` C${validateCas(cas)}`, key, value, options)
  }

  delete (key, options) {
    validateKey(key)
    const cas = options?.cas !== undefined ? ` C${validateCas(options.cas)}` : ''
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_DELETE, `md ${key}${cas} O${opaque}${CRLF}`, opaque)
  }

  incr (key, delta = 1) {
    return this.#arithmetic('I', key, delta)
  }

  decr (key, delta = 1) {
    return this.#arithmetic('D', key, delta)
  }

  noop () {
    return this.#connection.execute(TYPE_NOOP, `mn${CRLF}`)
  }

  version () {
    return this.#connection.execute(TYPE_VERSION, `version${CRLF}`)
  }

  close () {
    return this.#connection.close()
  }

  #store (type, extra, key, value, options) {
    validateKey(key)
    value = validateValue(value)
    const ttl = validateTTL(options?.ttl)
    const opaque = this.#nextOpaque()

    // Single buffer for header, data block and terminator: one socket write
    const header = `ms ${key} ${value.length}${extra} T${ttl} O${opaque}${CRLF}`
    const payload = Buffer.allocUnsafe(header.length + value.length + 2)
    payload.write(header, 0, 'latin1')
    value.copy(payload, header.length)
    payload[payload.length - 2] = 13
    payload[payload.length - 1] = 10

    return this.#connection.execute(type, payload, opaque)
  }

  #arithmetic (mode, key, delta) {
    validateKey(key)
    delta = validateDelta(delta)
    const opaque = this.#nextOpaque()
    return this.#connection.execute(TYPE_ARITH, `ma ${key} v M${mode} D${delta} O${opaque}${CRLF}`, opaque)
  }

  #nextOpaque () {
    this.#opaque = (this.#opaque + 1) & 0x3fffffff
    return this.#opaque.toString()
  }
}
