export class MemcachedError extends Error {
  constructor (message, code, options) {
    super(message, options)
    this.code = code
  }

  get name () {
    return this.constructor.name
  }
}

export class ConnectionError extends MemcachedError {
  constructor (message, options) {
    super(message, 'PLT_MEMCACHED_CONNECTION_ERROR', options)
  }
}

export class ProtocolError extends MemcachedError {
  constructor (message, options) {
    super(message, 'PLT_MEMCACHED_PROTOCOL_ERROR', options)
  }
}

export class ValidationError extends MemcachedError {
  constructor (message, options) {
    super(message, 'PLT_MEMCACHED_VALIDATION_ERROR', options)
  }
}
