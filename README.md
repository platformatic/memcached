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

- A single TCP connection per server by default (optionally a small pool, see
  [Connection pooling](#connection-pooling)) with **full request pipelining**. memcached
  processes commands on a connection strictly in order, so responses are correlated through a
  FIFO queue of pending operations — no per-request locking needed.
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

### `new Client(servers, options)`

- `servers`: `'host:port'`, `'memcached://host:port'`, `{ host, port }` or an array of those
  for client-side sharding (see [Multiple servers](#multiple-servers-client-side-sharding)).
  Defaults to `'localhost:11211'`.
- `options.connectTimeout`: milliseconds to wait for the TCP connection (default `5000`).
- `options.reconnectDelay`: initial reconnection backoff in milliseconds, doubled after each
  failed attempt (default `100`).
- `options.maxReconnectDelay`: backoff cap in milliseconds (default `5000`).
- `options.poolSize`: connections opened per server (default `1`, see
  [Connection pooling](#connection-pooling)).

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

Sends `mn` to every server, useful as a pipeline fence.

### `client.version()` → `Promise<string>`

Returns the server version string, useful as a health check. With multiple servers, all of
them are queried (a single unreachable node makes this reject) and the first server's
version is returned.

### `client.close()` → `Promise<void>`

Waits for in-flight commands to settle, then closes all connections. Idempotent. Commands
issued after `close()` reject with `ConnectionError`.

### Errors

All errors extend `MemcachedError` and carry a `code`:

- `ConnectionError` (`PLT_MEMCACHED_CONNECTION_ERROR`): socket failures, connect timeout,
  commands after `close()`.
- `ProtocolError` (`PLT_MEMCACHED_PROTOCOL_ERROR`): malformed or uncorrelated responses,
  `CLIENT_ERROR`/`SERVER_ERROR` from the server.
- `ValidationError` (`PLT_MEMCACHED_VALIDATION_ERROR`): invalid keys, values, TTLs, CAS tokens.

## Multiple servers (client-side sharding)

memcached has no server-side clustering protocol — nodes are share-nothing and unaware of
each other — so sharding is a client concern. Pass an array of addresses to spread keys
across nodes:

```js
const client = new Client(['cache1:11211', 'cache2:11211', 'cache3:11211'])
```

- Keys are routed with **ketama-style consistent hashing** (160 points per node on a 32-bit
  md5 ring): adding or removing a node remaps only ~1/N of the keyspace, every other key
  keeps its node.
- One connection per node (or `poolSize` connections, see
  [Connection pooling](#connection-pooling)), each with its own pipelining, reconnection and
  backoff — exactly as in single-server mode. A single server skips hashing entirely.
- Routing is deterministic: every client instance given the same address list routes every
  key to the same node, across processes and restarts.
- **Node-down behavior is fail-fast per key range**: commands for keys owned by an
  unreachable node reject with `ConnectionError` while the node's connection backs off and
  reconnects; other nodes are unaffected. Keys are deliberately **not** rehashed to
  surviving nodes, since that causes stale reads when the node comes back.
- There is no cross-key atomicity — each key lives on exactly one node. A single hot key
  still pins to one node by construction; sharding spreads aggregate load only.

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

### Connection pooling

The memcached protocol has no request multiplexing: responses come back strictly in request
order, so a pipelined connection is subject to **head-of-line blocking** — a single large
value transfer holds the line while sub-millisecond gets queue behind it. Since memcached
binds each connection to one worker thread but is multithreaded across connections, opening
several connections also buys genuine server-side parallelism.

Set `poolSize` to open that many connections per server:

```js
const client = new Client('localhost:11211', { poolSize: 4 })
```

- Each command is dispatched to the pool member with the **fewest outstanding requests**, so
  small operations flow around a connection busy with a bulk transfer. FIFO correlation and
  opaque verification are unchanged per connection.
- Pooling composes with sharding: `poolSize` connections are opened per node, and keys are
  still routed to nodes first.
- Auto-pipelining batching happens per pooled connection, after dispatch.
- The default of `1` keeps today's behavior and memory footprint. Mind fleet sizing: each
  server sees `clients × poolSize` connections, and memcached's connection limit (`-c`,
  default 1024) must be raised accordingly.

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

- ElastiCache Auto Discovery (`config get cluster`) for dynamic node lists.
- TLS support.

## License

Apache-2.0 — see [LICENSE](LICENSE).
