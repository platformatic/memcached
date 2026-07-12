import { channel, tracingChannel } from 'node:diagnostics_channel'
import { EventEmitter } from 'node:events'
import { connect, type Socket } from 'node:net'
import { connect as tlsConnect, type ConnectionOptions } from 'node:tls'
import { AuthenticationError, ConnectionError, MemcachedError, ProtocolError, ValidationError } from './errors.ts'

export const TYPE_GET = 0
export const TYPE_GETS = 1
export const TYPE_SET = 2
export const TYPE_ADD = 3
export const TYPE_CAS = 4
export const TYPE_DELETE = 5
export const TYPE_ARITH = 6
export const TYPE_NOOP = 7
export const TYPE_VERSION = 8
export const TYPE_STATS = 9
export const TYPE_AUTH = 10

// Wire verb labels indexed by command type, used for metrics and diagnostics
const VERBS = ['mg', 'mg', 'ms', 'ms', 'ms', 'md', 'ma', 'mn', 'version', 'stats', 'auth']

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

/**
 * Per-command payload published on the `platformatic.memcached.command`
 * tracing channel. The same object flows through the whole event sequence
 * of a command; `outcome`, `durationMs`, `responseSize` and `error` are set
 * before the settlement events (`asyncStart`, `asyncEnd`, `error`) fire.
 */
export interface CommandDiagnosticsContext {
  /** Wire verb: 'mg', 'ms', 'md', 'ma', 'mn' or 'version'. */
  verb: string
  host: string
  port: number
  /** Size in bytes of the serialized command, including data block. */
  requestSize: number
  /** Only present when the client was created with `diagnosticsIncludeKeys: true`. */
  key?: string
  /**
   * 'success', 'miss' (key not found, or a conditional store/delete that did
   * not apply) or 'error' (rejection).
   */
  outcome?: 'success' | 'miss' | 'error'
  /** Wall-clock milliseconds from issuing the command to its settlement. */
  durationMs?: number
  /** Size in bytes of the returned value, when the command returned one. */
  responseSize?: number
  /** Only present when outcome is 'error'. */
  error?: Error
}

/**
 * Payload published on the `platformatic.memcached.connection.*` channels.
 */
export interface ConnectionDiagnosticsEvent {
  host: string
  port: number
  /** disconnect only: why the connection closed. */
  error?: Error
  /** reconnect only: 1-based attempt number since the last successful connect. */
  attempt?: number
  /** reconnect only: backoff delay before the attempt, in milliseconds. */
  delayMs?: number
}

export interface VerbMetrics {
  issued: number
  completed: number
  failed: number
}

/**
 * Snapshot returned by `client.metrics()`: monotonic counters plus the
 * `pendingDepth` gauge, aggregated across the client's connections.
 */
export interface ClientMetrics {
  commands: {
    issued: number
    completed: number
    failed: number
    byVerb: Record<string, VerbMetrics>
  }
  pipeline: {
    pendingDepth: number
    writes: number
    flushes: number
  }
  connection: {
    connects: number
    disconnects: number
    reconnectAttempts: number
  }
  bytes: {
    read: number
    written: number
  }
}

// Diagnostics channels. Channel names and payload shapes are a stable API,
// shared between metrics consumers and tracing integrations. Payloads are
// only allocated when at least one subscriber is present.
const commandChannel = tracingChannel<unknown, CommandDiagnosticsContext>('platformatic.memcached.command')
const connectChannel = channel('platformatic.memcached.connection.connect')
const disconnectChannel = channel('platformatic.memcached.connection.disconnect')
const reconnectChannel = channel('platformatic.memcached.connection.reconnect')

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

  /**
   * Connect over TLS. Pass `true` to use the default TLS configuration, or a
   * `node:tls` connect options object (`ca`, `cert`, `key`, `servername`,
   * `rejectUnauthorized`, ...). Note that when connecting to an IP address
   * the certificate hostname is not inferred: set `servername` explicitly.
   * @default false
   */
  tls?: boolean | ConnectionOptions

  /**
   * Username for ASCII (authfile) authentication, available on
   * memcached >= 1.6.6 started with `-Y <authfile>`. Must be provided
   * together with `password`. Credentials are sent before any other command
   * on every (re)connection. Printable ASCII only, no whitespace.
   */
  username?: string

  /**
   * Password for ASCII (authfile) authentication. Must be provided together
   * with `username`. Printable ASCII only, no whitespace.
   */
  password?: string

  /**
   * Number of connections opened to each server. Commands for a node are
   * dispatched to the pool member with the fewest outstanding requests,
   * so small operations do not queue behind large value transfers
   * (head-of-line blocking).
   * @default 1
   */
  poolSize?: number

  /**
   * Include the command key in the payloads published on the
   * `platformatic.memcached.command` diagnostics channel. Off by default
   * because keys may carry sensitive data.
   * @default false
   */
  diagnosticsIncludeKeys?: boolean
}

type Payload = string | Buffer

// Settlement callbacks for the internal authentication command: nobody awaits
// it, success is silent and failure is propagated by rejecting user commands.
function noop () {}

// A single in-flight command. Memcached processes commands in order on a
// connection, so a FIFO linked list of these is enough to correlate
// responses. The opaque token is used to defensively verify correlation.
class Pending {
  type: number
  opaque: string | null
  payload: Payload | null
  sent = false
  // Accumulator for multi-line responses (classic "stats"), created lazily
  stats: Record<string, string> | null = null
  resolve!: (value: unknown) => void
  reject!: (error: Error) => void
  next: Pending | null = null

  // Set only when the command tracing channel has subscribers
  ctx: CommandDiagnosticsContext | null = null
  startTime = 0

  constructor (type: number, opaque: string | null, payload: Payload) {
    this.type = type
    this.opaque = opaque
    this.payload = payload
  }
}

export class Connection extends EventEmitter {
  #host: string
  #port: number
  #tls: ConnectionOptions | null
  #authPayload: string | null = null
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

  // Metrics: plain integer counters incremented on paths already executed
  // per command, so the overhead is near zero. #typeCounters is indexed by
  // command type; #pendingDepth is a gauge tracking the FIFO length.
  #typeCounters: VerbMetrics[] = []
  #pendingDepth = 0
  #connectCount = 0
  #disconnectCount = 0
  #reconnectTotal = 0
  #bytesRead = 0
  #bytesWritten = 0
  #includeKeys: boolean

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
    const tls = options.tls ?? false
    if (tls === true) {
      this.#tls = {}
    } else if (tls === false) {
      this.#tls = null
    } else if (typeof tls === 'object' && tls !== null) {
      this.#tls = tls
    } else {
      throw new ValidationError('The tls option must be a boolean or an object of TLS connect options')
    }

    this.#connectTimeout = options.connectTimeout ?? DEFAULT_CONNECT_TIMEOUT
    this.#reconnectDelay = options.reconnectDelay ?? DEFAULT_RECONNECT_DELAY
    this.#maxReconnectDelay = options.maxReconnectDelay ?? DEFAULT_MAX_RECONNECT_DELAY
    this.#includeKeys = options.diagnosticsIncludeKeys === true

    for (let i = 0; i < VERBS.length; i++) {
      this.#typeCounters.push({ issued: 0, completed: 0, failed: 0 })
    }

    if (options.username !== undefined && options.password !== undefined) {
      // ASCII (authfile) authentication rides a classic set command whose
      // data block is "username password". The key is ignored by the server.
      // Credentials are validated to printable ASCII by the client, so the
      // latin1 string length equals the byte length.
      const data = `${options.username} ${options.password}`
      this.#authPayload = `set auth 0 0 ${data.length}\r\n${data}\r\n`
    }

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

  get host (): string {
    return this.#host
  }

  get port (): number {
    return this.#port
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

  // Number of commands enqueued and not yet settled: the FIFO length. Used
  // by the client for least-outstanding-requests dispatch across a pool.
  get pending (): number {
    return this.#pendingDepth
  }

  get flushes (): number {
    return this.#flushCount
  }

  // Sends a command and returns a promise settled when its response arrives.
  // payload is a latin1 string (line commands) or a Buffer (ms with data).
  // The key is only used for diagnostics and only when opted in.
  execute<T> (type: number, payload: Payload, opaque: string | null = null, key: string | null = null): Promise<T> {
    const counters = this.#typeCounters[type]
    counters.issued++

    if (this.#status === STATUS_CLOSED) {
      counters.failed++
      const error = new ConnectionError('Connection is closed')

      if (commandChannel.start.hasSubscribers) {
        const ctx = this.#commandContext(type, payload, key)
        commandChannel.start.publish(ctx)
        commandChannel.end.publish(ctx)
        ctx.outcome = 'error'
        ctx.durationMs = 0
        ctx.error = error
        commandChannel.error.publish(ctx)
        commandChannel.asyncStart.publish(ctx)
        commandChannel.asyncEnd.publish(ctx)
      }

      return Promise.reject(error)
    }

    const pending = new Pending(type, opaque, payload)
    const promise = new Promise<T>((resolve, reject) => {
      pending.resolve = resolve as (value: unknown) => void
      pending.reject = reject
    })

    this.#pendingDepth++

    if (commandChannel.start.hasSubscribers) {
      pending.ctx = this.#commandContext(type, payload, key)
      pending.startTime = performance.now()
      commandChannel.start.publish(pending.ctx)
    }

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

    if (pending.ctx !== null) {
      commandChannel.end.publish(pending.ctx)
    }

    return promise
  }

  // Adds this connection's counters into the snapshot. Implemented as an
  // aggregation so a future multi-connection client can sum across
  // connections without changing the shape.
  collectMetrics (target: ClientMetrics): void {
    for (let i = 0; i < VERBS.length; i++) {
      const source = this.#typeCounters[i]
      const verb = VERBS[i]
      let entry = target.commands.byVerb[verb]

      if (entry === undefined) {
        entry = { issued: 0, completed: 0, failed: 0 }
        target.commands.byVerb[verb] = entry
      }

      entry.issued += source.issued
      entry.completed += source.completed
      entry.failed += source.failed
      target.commands.issued += source.issued
      target.commands.completed += source.completed
      target.commands.failed += source.failed
    }

    target.pipeline.pendingDepth += this.#pendingDepth
    target.pipeline.writes += this.#writeCount
    target.pipeline.flushes += this.#flushCount
    target.connection.connects += this.#connectCount
    target.connection.disconnects += this.#disconnectCount
    target.connection.reconnectAttempts += this.#reconnectTotal
    target.bytes.read += this.#bytesRead
    target.bytes.written += this.#bytesWritten
  }

  #commandContext (type: number, payload: Payload, key: string | null): CommandDiagnosticsContext {
    const ctx: CommandDiagnosticsContext = {
      verb: VERBS[type],
      host: this.#host,
      port: this.#port,
      requestSize: payload.length
    }

    if (this.#includeKeys && key !== null) {
      ctx.key = key
    }

    return ctx
  }

  // Every command settles exactly once through one of these two methods,
  // whatever the path: response parsing, connection failure or teardown.
  #settleResolve (pending: Pending, value: unknown) {
    this.#pendingDepth--
    this.#typeCounters[pending.type].completed++

    const ctx = pending.ctx
    if (ctx !== null) {
      pending.ctx = null
      ctx.outcome = value === null || value === false ? 'miss' : 'success'
      ctx.durationMs = performance.now() - pending.startTime

      if (Buffer.isBuffer(value)) {
        ctx.responseSize = value.length
      } else if (typeof value === 'object' && value !== null && Buffer.isBuffer((value as { value?: unknown }).value)) {
        ctx.responseSize = (value as { value: Buffer }).value.length
      }

      commandChannel.asyncStart.publish(ctx)
      commandChannel.asyncEnd.publish(ctx)
    }

    pending.resolve(value)
  }

  #settleReject (pending: Pending, error: Error) {
    this.#pendingDepth--
    this.#typeCounters[pending.type].failed++

    const ctx = pending.ctx
    if (ctx !== null) {
      pending.ctx = null
      ctx.outcome = 'error'
      ctx.durationMs = performance.now() - pending.startTime
      ctx.error = error
      commandChannel.error.publish(ctx)
      commandChannel.asyncStart.publish(ctx)
      commandChannel.asyncEnd.publish(ctx)
    }

    pending.reject(error)
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
      this.#settleReject(node, error)
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

    let socket: Socket
    if (this.#tls !== null) {
      socket = tlsConnect({ host: this.#host, port: this.#port, ...this.#tls })
      socket.setNoDelay(true)
    } else {
      socket = connect({ host: this.#host, port: this.#port, noDelay: true })
    }
    this.#socket = socket

    this.#connectTimer = setTimeout(() => {
      socket.destroy(new ConnectionError(`Connection to ${this.#host}:${this.#port} timed out after ${this.#connectTimeout}ms`))
    }, this.#connectTimeout)
    this.#connectTimer.unref()

    // TLS sockets become usable once the handshake completes ('secureConnect'),
    // not when the TCP connection is established ('connect')
    socket.on(this.#tls !== null ? 'secureConnect' : 'connect', this.#onConnect)
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
    this.#connectCount++

    if (connectChannel.hasSubscribers) {
      connectChannel.publish({ host: this.#host, port: this.#port } satisfies ConnectionDiagnosticsEvent)
    }

    // Authenticate before anything else: a server started with an authfile
    // rejects every other command until the connection is authenticated, so
    // the credentials must be the first command on every (re)connection,
    // ahead of commands queued while disconnected.
    if (this.#authPayload !== null) {
      const auth = new Pending(TYPE_AUTH, null, this.#authPayload)
      auth.resolve = noop
      auth.reject = noop
      auth.sent = true
      auth.payload = null
      auth.next = this.#head
      this.#head = auth

      if (this.#tail === null) {
        this.#tail = auth
      }

      // The auth command bypasses execute(), so it is counted as issued here
      // to keep the metrics invariant (it settles through the settle helpers)
      this.#typeCounters[TYPE_AUTH].issued++
      this.#pendingDepth++

      this.#write(this.#authPayload)
    }

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

    // An established connection was lost (or deliberately closed). Failed
    // connection attempts are not disconnects: they surface as reconnects.
    if (this.#wasReady) {
      this.#disconnectCount++

      if (disconnectChannel.hasSubscribers) {
        disconnectChannel.publish({ host: this.#host, port: this.#port, error } satisfies ConnectionDiagnosticsEvent)
      }
    }

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
          this.#settleReject(node, error)
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
    this.#reconnectTotal++

    if (reconnectChannel.hasSubscribers) {
      reconnectChannel.publish({
        host: this.#host,
        port: this.#port,
        attempt: this.#reconnectAttempts,
        delayMs: delay
      } satisfies ConnectionDiagnosticsEvent)
    }

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
    // Payload strings are latin1, so length is the byte size in both cases
    this.#bytesWritten += payload.length

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
    this.#bytesRead += chunk.length

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

  // Dequeues the head command once its response is complete
  #dequeue (pending: Pending) {
    this.#head = pending.next

    if (this.#head === null) {
      this.#tail = null

      if (this.#drainResolve !== null) {
        const resolve = this.#drainResolve
        this.#drainResolve = null
        resolve()
      }
    }
  }

  #complete (line: string, value: Buffer | null) {
    const pending = this.#head

    if (pending === null) {
      this.#socket!.destroy(new ProtocolError(`Received unexpected response: ${line}`))
      return
    }

    // Multi-line response: "stats" produces zero or more "STAT <name> <value>"
    // lines terminated by "END". Accumulate them into the pending command and
    // keep it at the head of the FIFO until the terminator settles it below.
    if (pending.type === TYPE_STATS && line.startsWith('STAT ')) {
      const nameEnd = line.indexOf(' ', 5)
      const stats = (pending.stats ??= Object.create(null) as Record<string, string>)

      // Values may themselves contain spaces (e.g. in "stats settings"),
      // so only the first two tokens are split off
      if (nameEnd === -1) {
        stats[line.slice(5)] = ''
      } else {
        stats[line.slice(5, nameEnd)] = line.slice(nameEnd + 1)
      }

      return
    }

    this.#dequeue(pending)

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
        this.#settleReject(pending, error)
        this.#socket!.destroy(error)
        return
      }
    }

    if (pending.type === TYPE_AUTH) {
      if (code === 'STORED') {
        this.#settleResolve(pending, undefined)
      } else {
        // Fail fast: the server discards commands pipelined behind a failed
        // authentication, so no responses are coming for them. Destroying
        // the socket rejects every command already sent with this error.
        const error = new AuthenticationError(`Authentication failed: ${line}`)
        this.#settleReject(pending, error)
        this.#socket!.destroy(error)
      }

      return
    }

    switch (code) {
      case 'HD':
        if (pending.type === TYPE_SET) {
          this.#settleResolve(pending, undefined)
        } else if (pending.type === TYPE_ARITH) {
          this.#settleResolve(pending, null)
        } else {
          this.#settleResolve(pending, true)
        }
        break
      case 'VA':
        if (pending.type === TYPE_GET) {
          this.#settleResolve(pending, value)
        } else if (pending.type === TYPE_GETS) {
          let cas = null

          for (let i = 2; i < tokens.length; i++) {
            if (tokens[i].charCodeAt(0) === 99) {
              // 'c'
              cas = tokens[i].slice(1)
              break
            }
          }

          this.#settleResolve(pending, { value, cas })
        } else if (pending.type === TYPE_ARITH) {
          this.#settleResolve(pending, BigInt(value!.toString('latin1')))
        } else {
          this.#settleReject(pending, new ProtocolError(`Unexpected value response for command: ${line}`))
        }
        break
      case 'EN': // miss
        this.#settleResolve(pending, null)
        break
      case 'NF': // not found
        if (pending.type === TYPE_ARITH) {
          this.#settleResolve(pending, null)
        } else if (pending.type === TYPE_SET) {
          this.#settleReject(pending, new MemcachedError('Item not found', 'PLT_MEMCACHED_NOT_STORED'))
        } else {
          this.#settleResolve(pending, false)
        }
        break
      case 'NS': // not stored
        if (pending.type === TYPE_SET) {
          this.#settleReject(pending, new MemcachedError('Item not stored', 'PLT_MEMCACHED_NOT_STORED'))
        } else {
          this.#settleResolve(pending, false)
        }
        break
      case 'EX': // exists, CAS mismatch
        if (pending.type === TYPE_SET) {
          this.#settleReject(pending, new MemcachedError('Item exists', 'PLT_MEMCACHED_NOT_STORED'))
        } else {
          this.#settleResolve(pending, false)
        }
        break
      case 'MN':
        this.#settleResolve(pending, undefined)
        break
      case 'VERSION':
        this.#settleResolve(pending, line.slice(8))
        break
      case 'END':
        if (pending.type === TYPE_STATS) {
          pending.resolve(pending.stats ?? Object.create(null))
        } else {
          const error = new ProtocolError(`Received unexpected response: ${line}`)
          pending.reject(error)
          this.#socket!.destroy(error)
        }
        break
      case 'ERROR':
      case 'CLIENT_ERROR':
      case 'SERVER_ERROR':
        this.#settleReject(pending, new ProtocolError(`Server returned an error: ${line}`))
        break
      default: {
        const error = new ProtocolError(`Received unknown response: ${line}`)
        this.#settleReject(pending, error)
        this.#socket!.destroy(error)
      }
    }
  }
}
