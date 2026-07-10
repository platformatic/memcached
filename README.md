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
(`mg`, `ms`, `md`, `ma`, `mn`) plus `version` and `stats` for health checks and observability —
nothing else.

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

The source is TypeScript under `src/`, written in the erasable subset so it runs directly on
Node.js via [type stripping](https://nodejs.org/api/typescript.html) during development. The
published package ships plain JavaScript plus declaration files compiled to `dist/`, so the
runtime requirement for consumers stays Node.js >= 22.12.0.

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

- `url`: `'host:port'`, `'memcached://host:port'`, `'memcacheds://host:port'` (TLS),
  `'memcached://user:pass@host:port'` or `{ host, port }`. Defaults to `'localhost:11211'`.
- `options.connectTimeout`: milliseconds to wait for the TCP connection (default `5000`).
- `options.reconnectDelay`: initial reconnection backoff in milliseconds, doubled after each
  failed attempt (default `100`).
- `options.maxReconnectDelay`: backoff cap in milliseconds (default `5000`).
- `options.tls`: connect over TLS. Pass `true` for the default TLS configuration, or a
  [`node:tls` connect options](https://nodejs.org/api/tls.html#tlsconnectoptions-callback)
  object (`ca`, `cert`, `key`, `servername`, `rejectUnauthorized`, ...). The `memcacheds://`
  URL scheme is shorthand for `tls: true`; an explicit options object still applies, so
  certificates can be configured either way. When connecting to an IP address the certificate
  hostname is not inferred: set `servername` explicitly. Default: `false` (plaintext).
- `options.username` / `options.password`: credentials for ASCII (authfile)
  authentication — see [Authentication](#authentication). Must be provided together, as
  non-empty printable ASCII strings without whitespace. Credentials can also be embedded
  in the URL (`memcached://user:pass@host:port`, percent-encoded); explicit options take
  precedence over URL credentials.

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

### `client.stats([subcommand])` → `Promise<Record<string, string>>`

Returns server statistics as a name/value map, useful for observability: connection counts,
evictions, `get_hits`/`get_misses`, memory usage and so on. An optional subcommand selects a
specific domain, e.g. `stats('items')`, `stats('slabs')` or `stats('settings')`. Only
`END`-terminated subcommands are supported (notably not `reset` or `cachedump`).

### `client.close()` → `Promise<void>`

Waits for in-flight commands to settle, then closes the connection. Idempotent. Commands
issued after `close()` reject with `ConnectionError`.

### Errors

All errors extend `MemcachedError` and carry a `code`:

- `ConnectionError` (`PLT_MEMCACHED_CONNECTION_ERROR`): socket failures, connect timeout,
  commands after `close()`.
- `ProtocolError` (`PLT_MEMCACHED_PROTOCOL_ERROR`): malformed or uncorrelated responses,
  `CLIENT_ERROR`/`SERVER_ERROR` from the server.
- `ValidationError` (`PLT_MEMCACHED_VALIDATION_ERROR`): invalid keys, values, TTLs, CAS
  tokens, credentials.
- `AuthenticationError` (`PLT_MEMCACHED_AUTH_ERROR`): the server rejected the configured
  credentials.

## Authentication

memcached >= 1.6.6 supports ASCII authentication ("authfile mode"): start the server with
`memcached -Y /path/to/authfile`, where the authfile contains `username:password` lines.
Configure the client with matching credentials, either as options or in the URL:

```js
const client = new Client('localhost:11211', { username: 'user', password: 'secret' })
// equivalent:
const client2 = new Client('memcached://user:secret@localhost:11211')
```

The client authenticates as the first command on every connection — including automatic
reconnections — before any queued command is flushed. If the server rejects the
credentials, pending commands fail with `AuthenticationError` and the client keeps
retrying in the background with the usual reconnection backoff.

Two caveats:

- SASL authentication rides the deprecated binary protocol and is intentionally **not**
  supported; providers that only offer SASL will not work with this client.
- Credentials travel in plaintext on the wire. In production, pair authentication with
  TLS or a trusted network.

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
  against [memjs](https://github.com/memcachier/memjs), and
  `node benchmarks/autopipelining.js` to compare the flush scheduling modes below under
  concurrent independent issuers.

### Auto-pipelining modes

Outgoing commands are corked and flushed as a single `writev`. The `autoPipelining` option
controls *when* the flush happens:

- `'microtask'` (default): flush at the next microtask checkpoint. Commands issued in the
  same synchronous block (and the microtask cascade it spawns) share one socket write.
  A lone command is flushed practically immediately, so this is the lowest-latency choice
  for sparse or bursty traffic.
- `'tick'` (or `true`): flush in the check phase (`setImmediate`) of the current event loop
  iteration, like ioredis' `enableAutoPipelining`. Commands issued from *independent* async
  contexts in the same iteration — e.g. hundreds of concurrent request handlers resuming
  from `await`, each issuing one `get` — coalesce into a single syscall instead of one write
  per macrotask cascade. The trade-off is per-command latency: every command waits for the
  remaining callbacks of the current iteration before hitting the wire, which is only worth
  it when many concurrent issuers are active. With few in-flight commands prefer
  `'microtask'`.

Response ordering, FIFO correlation and opaque verification are identical in both modes;
only the flush scheduling changes.

```js
const client = new Client('localhost:11211', { autoPipelining: 'tick' })
```

## Testing

Tests run against a real memcached via Docker. `npm test` builds `dist/`, starts a
`memcached:alpine` container automatically (and stops it afterwards), or you can manage
the container yourself:

```bash
docker run -d --rm --name memcached -p 11211:11211 memcached:alpine
npm test
```

Tests are TypeScript executed directly by `node --test` through type stripping, so
development requires Node.js >= 22.18.0 (or any 23.6+/24+); consuming the published
package does not.

## Roadmap

- Consistent hashing across multiple servers (multi-node support).
- Optional connection pooling per server.

## License

Apache-2.0 — see [LICENSE](LICENSE).
