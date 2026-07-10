/// <reference types="node" />

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

export declare class Client {
  /**
   * Creates a client connected to a single memcached server.
   *
   * @param url `'host:port'`, `'memcached://host:port'` or `{ host, port }`.
   *            Defaults to `localhost:11211`.
   */
  constructor (url?: string | ServerAddress, options?: ClientOptions)

  /**
   * Returns the value for the key, or `null` on a miss.
   */
  get (key: string): Promise<Buffer | null>

  /**
   * Returns the value and its CAS token, or `null` on a miss.
   */
  gets (key: string): Promise<GetsResult | null>

  /**
   * Unconditionally stores the value. Throws on failure.
   */
  set (key: string, value: Buffer | string, options?: StoreOptions): Promise<void>

  /**
   * Stores the value only if the key does not exist yet.
   * Returns `false` if the key already exists.
   */
  add (key: string, value: Buffer | string, options?: StoreOptions): Promise<boolean>

  /**
   * Stores the value only if the current CAS token matches.
   * Returns `false` on CAS mismatch or if the key does not exist.
   */
  cas (key: string, value: Buffer | string, cas: string | number | bigint, options?: StoreOptions): Promise<boolean>

  /**
   * Deletes the key. Returns `false` on a miss, or on CAS mismatch when
   * `options.cas` is provided.
   */
  delete (key: string, options?: DeleteOptions): Promise<boolean>

  /**
   * Increments the numeric value of the key by delta (default 1).
   * Returns the new value, or `null` if the key does not exist.
   */
  incr (key: string, delta?: number | bigint): Promise<bigint | null>

  /**
   * Decrements the numeric value of the key by delta (default 1), clamping
   * at 0. Returns the new value, or `null` if the key does not exist.
   */
  decr (key: string, delta?: number | bigint): Promise<bigint | null>

  /**
   * Sends a meta no-op, useful as a pipeline fence.
   */
  noop (): Promise<void>

  /**
   * Returns the server version string, useful as a health check.
   */
  version (): Promise<string>

  /**
   * Waits for in-flight commands to settle, then closes the connection.
   */
  close (): Promise<void>
}

export declare class MemcachedError extends Error {
  code: string
}

export declare class ConnectionError extends MemcachedError {}
export declare class ProtocolError extends MemcachedError {}
export declare class ValidationError extends MemcachedError {}
