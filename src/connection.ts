import { EventEmitter } from 'node:events'
import { connect, type Socket } from 'node:net'
import { ConnectionError, MemcachedError, ProtocolError, ValidationError } from './errors.ts'

export const TYPE_GET = 0
export const TYPE_GETS = 1
export const TYPE_SET = 2
export const TYPE_ADD = 3
export const TYPE_CAS = 4
export const TYPE_DELETE = 5
export const TYPE_ARITH = 6
export const TYPE_NOOP = 7
export const TYPE_VERSION = 8

const STATUS_CONNECTING = 0
const STATUS_READY = 1
const STATUS_CLOSED = 2

const STATE_LINE = 0
const STATE_VALUE = 1

const EMPTY = Buffer.alloc(0)
const CR = 13
const LF = 10

const DEFAULT_CONNECT_TIMEOUT = 5000
const DEFAULT_RECONNECT_DELAY = 100
const DEFAULT_MAX_RECONNECT_DELAY = 5000

export interface ClientOptions {
  /**
   * Milliseconds to wait for the TCP connection to be established.
   * @default 5000
   */
  connectTimeout?: number

  /**
   * Initial reconnection delay in milliseconds. The delay doubles after each
   * failed attempt.
   * @default 100
   */
  reconnectDelay?: number

  /**
   * Maximum reconnection delay in milliseconds.
   * @default 5000
   */
  maxReconnectDelay?: number

  /**
   * When commands corked for pipelining are flushed to the socket.
   *
   * - `'microtask'` (the default): at the next microtask checkpoint.
   *   Coalesces commands issued in the same synchronous block and their
   *   microtask cascade. Lowest latency for sparse traffic.
   * - `'tick'` (or `true`): in the check phase (setImmediate) of the current
   *   event loop iteration. Also coalesces commands issued from independent
   *   async contexts (e.g. concurrent request handlers resuming from await)
   *   into a single socket write, at the cost of slightly higher
   *   per-command latency.
   *
   * `false` is an alias for `'microtask'`. Response ordering and correlation
   * are identical in both modes.
   *
   * @default 'microtask'
   */
  autoPipelining?: 'microtask' | 'tick' | boolean
}

type Payload = string | Buffer

// A single in-flight command. Memcached processes commands in order on a
// connection, so a FIFO linked list of these is enough to correlate
// responses. The opaque token is used to defensively verify correlation.
class Pending {
  type: number
  opaque: string | null
  payload: Payload | null
  sent = false
  resolve!: (value: unknown) => void
  reject!: (error: Error) => void
  next: Pending | null = null

  constructor (type: number, opaque: string | null, payload: Payload) {
    this.type = type
    this.opaque = opaque
    this.payload = payload
  }
}

export class Connection extends EventEmitter {
  #host: string
  #port: number
  #connectTimeout: number
  #reconnectDelay: number
  #maxReconnectDelay: number

  #socket: Socket | null = null
  #status = STATUS_CONNECTING
  #lastError: Error | null = null
  #reconnectAttempts = 0
  #connectTimer: NodeJS.Timeout | undefined = undefined
  #reconnectTimer: NodeJS.Timeout | undefined = undefined
  #corked = false
  #tickFlush = false
  #flushImmediate: NodeJS.Immediate | null = null

  // Diagnostic counters: socket writes and actual flushes performed. Their
  // ratio is the average number of commands coalesced per syscall.
  #writeCount = 0
  #flushCount = 0

  // FIFO of commands: a prefix of sent (in-flight) commands followed by
  // unsent ones, queued while the socket is not ready and flushed on connect
  #head: Pending | null = null
  #tail: Pending | null = null
  #wasReady = false

  // Incremental response parser state
  #buffer: Buffer = EMPTY
  #state = STATE_LINE
  #valueLine: string | null = null
  #valueNeeded = 0

  #drainResolve: (() => void) | null = null
  #closeResolve: (() => void) | null = null

  constructor (host: string, port: number, options: ClientOptions = {}) {
    super()

    this.#host = host
    this.#port = port
    this.#connectTimeout = options.connectTimeout ?? DEFAULT_CONNECT_TIMEOUT
    this.#reconnectDelay = options.reconnectDelay ?? DEFAULT_RECONNECT_DELAY
    this.#maxReconnectDelay = options.maxReconnectDelay ?? DEFAULT_MAX_RECONNECT_DELAY

    const autoPipelining = options.autoPipelining ?? 'microtask'
    if (autoPipelining === true || autoPipelining === 'tick') {
      this.#tickFlush = true
    } else if (autoPipelining === false || autoPipelining === 'microtask') {
      this.#tickFlush = false
    } else {
      throw new ValidationError("The autoPipelining option must be 'microtask' or 'tick'")
    }

    this.#connect()
  }

  get socket (): Socket | null {
    return this.#socket
  }

  get connected (): boolean {
    return this.#status === STATUS_READY
  }

  get closed (): boolean {
    return this.#status === STATUS_CLOSED
  }

  get writes (): number {
    return this.#writeCount
  }

  get flushes (): number {
    return this.#flushCount
  }

  // Sends a command and returns a promise settled when its response arrives.
  // payload is a latin1 string (line commands) or a Buffer (ms with data).
  execute<T> (type: number, payload: Payload, opaque: string | null = null): Promise<T> {
    if (this.#status === STATUS_CLOSED) {
      return Promise.reject(new ConnectionError('Connection is closed'))
    }

    const pending = new Pending(type, opaque, payload)
    const promise = new Promise<T>((resolve, reject) => {
      pending.resolve = resolve as (value: unknown) => void
      pending.reject = reject
    })

    if (this.#tail === null) {
      this.#head = pending
    } else {
      this.#tail.next = pending
    }
    this.#tail = pending

    if (this.#status === STATUS_READY && this.#socket !== null && !this.#socket.destroyed) {
      this.#write(payload)
      pending.sent = true
      pending.payload = null
    }

    return promise
  }

  async close (): Promise<void> {
    if (this.#status === STATUS_CLOSED) {
      return
    }

    this.#status = STATUS_CLOSED
    clearTimeout(this.#reconnectTimer)
    clearTimeout(this.#connectTimer)

    if (this.#socket === null) {
      // In a reconnection backoff window: no new connection will be
      // attempted, so queued commands can never be delivered.
      this.#rejectAll(new ConnectionError('Connection is closed'))
      return
    }

    // Let in-flight commands settle before closing the socket
    if (this.#head !== null) {
      await new Promise<void>(resolve => {
        this.#drainResolve = resolve
      })
    }

    if (this.#socket !== null) {
      const closed = new Promise<void>(resolve => {
        this.#closeResolve = resolve
      })

      this.#socket.destroySoon()
      await closed
    }
  }

  #rejectAll (error: Error) {
    let node = this.#head
    this.#head = null
    this.#tail = null

    while (node !== null) {
      const next = node.next
      node.reject(error)
      node = next
    }

    if (this.#drainResolve !== null) {
      const resolve = this.#drainResolve
      this.#drainResolve = null
      resolve()
    }
  }

  #connect () {
    this.#status = STATUS_CONNECTING
    this.#wasReady = false

    const socket = connect({ host: this.#host, port: this.#port, noDelay: true })
    this.#socket = socket

    this.#connectTimer = setTimeout(() => {
      socket.destroy(new ConnectionError(`Connection to ${this.#host}:${this.#port} timed out after ${this.#connectTimeout}ms`))
    }, this.#connectTimeout)
    this.#connectTimer.unref()

    socket.on('connect', this.#onConnect)
    socket.on('data', this.#onData)
    socket.on('error', this.#onError)
    socket.on('close', this.#onClose)
  }

  #onConnect = () => {
    clearTimeout(this.#connectTimer)

    // close() might have been called while connecting: keep the closed status
    // but still flush queued commands so they can settle before teardown.
    if (this.#status !== STATUS_CLOSED) {
      this.#status = STATUS_READY
    }

    this.#wasReady = true
    this.#reconnectAttempts = 0

    // Flush commands queued while the socket was not ready
    for (let node = this.#head; node !== null; node = node.next) {
      if (!node.sent) {
        this.#write(node.payload!)
        node.sent = true
        node.payload = null
      }
    }

    this.emit('connect')
  }

  #onError = (error: Error) => {
    this.#lastError = error
  }

  #onClose = () => {
    clearTimeout(this.#connectTimer)

    // Data corked on the old socket is gone with it: reset the flush state so
    // writes on the next socket cork and schedule a new flush from scratch.
    // Commands already written while corked were marked as sent and are
    // rejected below, so nothing is ever left corked forever.
    if (this.#flushImmediate !== null) {
      clearImmediate(this.#flushImmediate)
      this.#flushImmediate = null
    }
    this.#corked = false
    this.#socket = null

    // Reset parser state, a new connection starts a new stream
    this.#buffer = EMPTY
    this.#state = STATE_LINE
    this.#valueLine = null
    this.#valueNeeded = 0

    const cause = this.#lastError
    this.#lastError = null
    const error =
      cause instanceof MemcachedError
        ? cause
        : new ConnectionError(`Connection to ${this.#host}:${this.#port} closed`, cause ? { cause } : undefined)

    // Reject every command already sent: memcached may have partially
    // processed them, so retrying transparently would not be safe. Commands
    // still queued (issued while disconnected) are kept and will be flushed
    // by the next connection attempt, unless this attempt itself failed or
    // the connection is closing - then no attempt is coming and they must
    // reject as well.
    if (this.#status === STATUS_CLOSED || !this.#wasReady) {
      this.#rejectAll(error)
    } else {
      let node = this.#head
      this.#head = null
      this.#tail = null

      while (node !== null) {
        const next = node.next

        if (node.sent) {
          node.reject(error)
        } else {
          node.next = null

          if (this.#tail === null) {
            this.#head = node
          } else {
            this.#tail.next = node
          }
          this.#tail = node
        }

        node = next
      }

      if (this.#head === null && this.#drainResolve !== null) {
        const resolve = this.#drainResolve
        this.#drainResolve = null
        resolve()
      }
    }

    if (this.#status === STATUS_CLOSED) {
      if (this.#closeResolve !== null) {
        const resolve = this.#closeResolve
        this.#closeResolve = null
        resolve()
      }

      this.emit('close')
      return
    }

    this.emit('disconnect', error)

    // The timer is deliberately not unref'd: like an established connection,
    // a client waiting to reconnect keeps the process alive until close().
    // This also guarantees queued commands are not abandoned mid-flight.
    const delay = Math.min(this.#reconnectDelay * 2 ** this.#reconnectAttempts, this.#maxReconnectDelay)
    this.#reconnectAttempts++
    this.#reconnectTimer = setTimeout(() => this.#connect(), delay)
  }

  // Writes are corked and flushed with a single writev. In 'microtask' mode
  // the flush happens at the next microtask checkpoint, coalescing commands
  // issued in the same synchronous block. In 'tick' mode it happens in the
  // check phase (setImmediate) of the current event loop iteration, also
  // coalescing commands issued from independent async contexts, like
  // concurrent request handlers resuming from await.
  #write (payload: Payload) {
    const socket = this.#socket!

    if (!this.#corked) {
      this.#corked = true
      socket.cork()

      if (this.#tickFlush) {
        this.#flushImmediate = setImmediate(this.#uncork)
      } else {
        queueMicrotask(this.#uncork)
      }
    }

    this.#writeCount++

    if (typeof payload === 'string') {
      socket.write(payload, 'latin1')
    } else {
      socket.write(payload)
    }
  }

  #uncork = () => {
    this.#corked = false
    this.#flushImmediate = null
    const socket = this.#socket

    if (socket !== null && !socket.destroyed) {
      this.#flushCount++
      socket.uncork()
    }
  }

  // Incremental parser: accumulates partial frames across TCP chunks and
  // never scans value bytes (which may contain \r\n) for line terminators.
  #onData = (chunk: Buffer) => {
    const socket = this.#socket!
    const buf = this.#buffer.length === 0 ? chunk : Buffer.concat([this.#buffer, chunk])
    const len = buf.length
    let offset = 0

    while (offset < len) {
      if (this.#state === STATE_LINE) {
        const idx = buf.indexOf(LF, offset)

        if (idx === -1) {
          break
        }

        if (idx === offset || buf[idx - 1] !== CR) {
          socket.destroy(new ProtocolError('Malformed response line'))
          return
        }

        const line = buf.toString('latin1', offset, idx - 1)
        offset = idx + 1

        if (line.charCodeAt(0) === 86 && line.charCodeAt(1) === 65 && line.charCodeAt(2) === 32) {
          // "VA <size> <flags>*" - a data block follows
          const sizeEnd = line.indexOf(' ', 3)
          const size = Number.parseInt(sizeEnd === -1 ? line.slice(3) : line.slice(3, sizeEnd), 10)

          if (!Number.isSafeInteger(size) || size < 0) {
            socket.destroy(new ProtocolError(`Malformed value size in response: ${line}`))
            return
          }

          this.#state = STATE_VALUE
          this.#valueLine = line
          this.#valueNeeded = size
        } else {
          this.#complete(line, null)
        }
      } else {
        const end = offset + this.#valueNeeded

        if (len < end + 2) {
          break
        }

        if (buf[end] !== CR || buf[end + 1] !== LF) {
          socket.destroy(new ProtocolError('Missing data block terminator'))
          return
        }

        // Copy the value out so the (potentially large) network buffer is not retained
        const value = Buffer.allocUnsafe(this.#valueNeeded)
        buf.copy(value, 0, offset, end)
        offset = end + 2

        const line = this.#valueLine!
        this.#state = STATE_LINE
        this.#valueLine = null
        this.#valueNeeded = 0

        this.#complete(line, value)
      }

      if (socket.destroyed) {
        return
      }
    }

    if (offset >= len) {
      this.#buffer = EMPTY
    } else {
      this.#buffer = offset === 0 ? buf : buf.subarray(offset)
    }
  }

  #complete (line: string, value: Buffer | null) {
    const pending = this.#head

    if (pending === null) {
      this.#socket!.destroy(new ProtocolError(`Received unexpected response: ${line}`))
      return
    }

    this.#head = pending.next

    if (this.#head === null) {
      this.#tail = null

      if (this.#drainResolve !== null) {
        const resolve = this.#drainResolve
        this.#drainResolve = null
        resolve()
      }
    }

    const tokens = line.split(' ')
    const code = tokens[0]

    // Defensive correlation check: the O token must be mirrored back
    if (pending.opaque !== null) {
      let mirrored = null

      for (let i = 1; i < tokens.length; i++) {
        if (tokens[i].charCodeAt(0) === 79) {
          // 'O'
          mirrored = tokens[i].slice(1)
          break
        }
      }

      if (mirrored !== null && mirrored !== pending.opaque) {
        const error = new ProtocolError(
          `Response correlation mismatch: expected opaque ${pending.opaque}, received ${mirrored}`
        )
        pending.reject(error)
        this.#socket!.destroy(error)
        return
      }
    }

    switch (code) {
      case 'HD':
        if (pending.type === TYPE_SET) {
          pending.resolve(undefined)
        } else if (pending.type === TYPE_ARITH) {
          pending.resolve(null)
        } else {
          pending.resolve(true)
        }
        break
      case 'VA':
        if (pending.type === TYPE_GET) {
          pending.resolve(value)
        } else if (pending.type === TYPE_GETS) {
          let cas = null

          for (let i = 2; i < tokens.length; i++) {
            if (tokens[i].charCodeAt(0) === 99) {
              // 'c'
              cas = tokens[i].slice(1)
              break
            }
          }

          pending.resolve({ value, cas })
        } else if (pending.type === TYPE_ARITH) {
          pending.resolve(BigInt(value!.toString('latin1')))
        } else {
          pending.reject(new ProtocolError(`Unexpected value response for command: ${line}`))
        }
        break
      case 'EN': // miss
        pending.resolve(null)
        break
      case 'NF': // not found
        if (pending.type === TYPE_ARITH) {
          pending.resolve(null)
        } else if (pending.type === TYPE_SET) {
          pending.reject(new MemcachedError('Item not found', 'PLT_MEMCACHED_NOT_STORED'))
        } else {
          pending.resolve(false)
        }
        break
      case 'NS': // not stored
        if (pending.type === TYPE_SET) {
          pending.reject(new MemcachedError('Item not stored', 'PLT_MEMCACHED_NOT_STORED'))
        } else {
          pending.resolve(false)
        }
        break
      case 'EX': // exists, CAS mismatch
        if (pending.type === TYPE_SET) {
          pending.reject(new MemcachedError('Item exists', 'PLT_MEMCACHED_NOT_STORED'))
        } else {
          pending.resolve(false)
        }
        break
      case 'MN':
        pending.resolve(undefined)
        break
      case 'VERSION':
        pending.resolve(line.slice(8))
        break
      case 'ERROR':
      case 'CLIENT_ERROR':
      case 'SERVER_ERROR':
        pending.reject(new ProtocolError(`Server returned an error: ${line}`))
        break
      default: {
        const error = new ProtocolError(`Received unknown response: ${line}`)
        pending.reject(error)
        this.#socket!.destroy(error)
      }
    }
  }
}
