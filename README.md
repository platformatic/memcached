# @platformatic/memcached

A minimal, high-performance [memcached](https://memcached.org/) client for Node.js, built on the
[meta text protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt).

> **Status**: this package is currently **private and experimental**. It backs the memcached
> storage adapter for Platformatic gateway request deduplication. APIs may change before a
> public release.

## Why the meta protocol?

memcached ships three protocols. The classic text protocol is verbose and has ambiguous
responses; the binary protocol is **deprecated** upstream. The meta protocol (memcached >= 1.6)
is the recommended replacement: compact single-line commands and responses, explicit flags,
length-prefixed data blocks (binary-safe values), CAS on every command including delete, and
opaque tokens for defensive response correlation. This client implements only the meta commands
(`mg`, `ms`, `md`, `ma`, `mn`) plus `version` for health checks — nothing else.

## Design

The performance approach follows [@platformatic/kafka](https://github.com/platformatic/kafka):

- A single TCP connection per server with **full request pipelining**. memcached processes
  commands on a connection strictly in order, so responses are correlated through a FIFO queue
  of pending operations — no per-request locking or connection pooling needed.
- Every command carries an **opaque token** (`O` flag) which the server mirrors back; the client
  verifies it to detect protocol desynchronization instead of silently returning wrong data.
- An **incremental, Buffer-based response parser**: partial frames are carried across TCP chunks
  and value bytes are consumed by length, never scanned — values containing `\r\n` are safe.
- Writes issued in the same synchronous block are coalesced into a single `writev` via
  cork/uncork.
- Zero runtime dependencies: `node:net` and nothing else.

## Requirements

- Node.js >= 22.12.0 (both `import` and `require` work)
- memcached >= 1.6

## Installation

```bash
npm install @platformatic/memcached
```

## Quick start

```js
import { Client } from '@platformatic/memcached'

const client = new Client('localhost:11211')

await client.set('greeting', 'hello', { ttl: 60 })

const value = await client.get('greeting')
console.log(value.toString()) // 'hello'

// Lock pattern: set-if-not-exists plus token-checked unlock
const acquired = await client.add('lock:job', 'my-token', { ttl: 30 })
if (acquired) {
  try {
    // ... critical section ...
  } finally {
    const current = await client.gets('lock:job')
    if (current?.value.toString() === 'my-token') {
      await client.delete('lock:job', { cas: current.cas })
    }
  }
}

await client.close()
```

## API

### `new Client(url, options)`

- `url`: `'host:port'`, `'memcached://host:port'` or `{ host, port }`. Defaults to
  `'localhost:11211'`.
- `options.connectTimeout`: milliseconds to wait for the TCP connection (default `5000`).
- `options.reconnectDelay`: initial reconnection backoff in milliseconds, doubled after each
  failed attempt (default `100`).
- `options.maxReconnectDelay`: backoff cap in milliseconds (default `5000`).

The constructor connects immediately in the background. Commands issued before the connection
is established are queued and flushed on connect. On socket errors, all in-flight commands are
rejected with `ConnectionError` (memcached may have partially processed them, so transparent
retry would be unsafe) and the client reconnects automatically with exponential backoff.

Values are `Buffer`s in and out — no implicit serialization. `string` values are accepted and
converted with UTF-8. Keys must be printable ASCII without whitespace, at most 250 bytes.

### `client.get(key)` → `Promise<Buffer | null>`

Returns the value, or `null` on a miss.

### `client.gets(key)` → `Promise<{ value: Buffer, cas: string } | null>`

Returns the value and its CAS token (an opaque string), or `null` on a miss.

### `client.set(key, value, { ttl })` → `Promise<void>`

Unconditionally stores the value. Throws on failure.

### `client.add(key, value, { ttl })` → `Promise<boolean>`

Stores the value only if the key does not exist (memcached `ms` with mode `E`).
Returns `false` if the key already exists; does not throw on conflict.

### `client.cas(key, value, cas, { ttl })` → `Promise<boolean>`

Stores the value only if the item's CAS token still matches. Returns `false` on CAS mismatch
or if the key does not exist; does not throw on conflict.

### `client.delete(key, { cas })` → `Promise<boolean>`

Deletes the key. When `cas` is provided the delete only happens if the token still matches.
Returns `false` on a miss or CAS mismatch; does not throw on conflict.

### `client.incr(key, delta)` / `client.decr(key, delta)` → `Promise<bigint | null>`

Increments/decrements a numeric value (`ma`). `delta` defaults to `1`. Returns the new value
as a `bigint` (memcached counters are unsigned 64-bit), or `null` if the key does not exist.
`decr` clamps at 0.

### `client.noop()` → `Promise<void>`

Sends `mn`, useful as a pipeline fence.

### `client.version()` → `Promise<string>`

Returns the server version string, useful as a health check.

### `client.close()` → `Promise<void>`

Waits for in-flight commands to settle, then closes the connection. Idempotent. Commands
issued after `close()` reject with `ConnectionError`.

### Errors

All errors extend `MemcachedError` and carry a `code`:

- `ConnectionError` (`PLT_MEMCACHED_CONNECTION_ERROR`): socket failures, connect timeout,
  commands after `close()`.
- `ProtocolError` (`PLT_MEMCACHED_PROTOCOL_ERROR`): malformed or uncorrelated responses,
  `CLIENT_ERROR`/`SERVER_ERROR` from the server.
- `ValidationError` (`PLT_MEMCACHED_VALIDATION_ERROR`): invalid keys, values, TTLs, CAS tokens.

## TTLs

TTLs are expressed in **seconds** — that is the granularity memcached supports. `0` (the
default) means the item never expires. Sub-second TTLs are not possible: if you work in
milliseconds, round up (`Math.ceil(ms / 1000)`). Per memcached semantics, TTL values larger
than 30 days are interpreted as absolute Unix timestamps.

## Performance notes

- All commands on a connection are pipelined: issue them concurrently (e.g. `Promise.all`)
  and they share socket writes and round trips. There is no artificial batching layer and
  none is needed.
- Run `npm run benchmark` (with a local memcached) to compare pipelined SET/GET throughput
  against [memjs](https://github.com/memcachier/memjs).

## Testing

Tests run against a real memcached via Docker. `npm test` starts a `memcached:alpine`
container automatically (and stops it afterwards), or you can manage one yourself:

```bash
docker run -d --rm --name memcached -p 11211:11211 memcached:alpine
npm test
```

## Roadmap

- Consistent hashing across multiple servers (multi-node support).
- Optional connection pooling per server.
- TLS support.

## License

Apache-2.0 — see [LICENSE](LICENSE).
