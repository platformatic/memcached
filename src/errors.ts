export class MemcachedError extends Error {
  code: string

  constructor (message: string, code: string, options?: ErrorOptions) {
    super(message, options)
    this.code = code
  }

  get name (): string {
    return this.constructor.name
  }
}

export class ConnectionError extends MemcachedError {
  constructor (message: string, options?: ErrorOptions) {
    super(message, 'PLT_MEMCACHED_CONNECTION_ERROR', options)
  }
}

export class ProtocolError extends MemcachedError {
  constructor (message: string, options?: ErrorOptions) {
    super(message, 'PLT_MEMCACHED_PROTOCOL_ERROR', options)
  }
}

export class ValidationError extends MemcachedError {
  constructor (message: string, options?: ErrorOptions) {
    super(message, 'PLT_MEMCACHED_VALIDATION_ERROR', options)
  }
}
