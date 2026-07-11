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

- `servers`: `'host:port'`, `'memcached://host:port'`, `'memcacheds://host:port'` (TLS),
  `'memcached://user:pass@host:port'`, `{ host, port }` or an array of those for client-side
  sharding (see [Multiple servers](#multiple-servers-client-side-sharding)). Defaults to
  `'localhost:11211'`.
- `options.connectTimeout`: milliseconds to wait for the TCP connection (default `5000`).
- `options.reconnectDelay`: initial reconnection backoff in milliseconds, doubled after each
  failed attempt (default `100`).
- `options.maxReconnectDelay`: backoff cap in milliseconds (default `5000`).
- `options.poolSize`: connections opened per server (default `1`, see
  [Connection pooling](#connection-pooling)).
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
- `options.diagnosticsIncludeKeys`: include the command key in diagnostics channel payloads
  (default `false`, keys may carry sensitive data — see
  [Metrics and diagnostics](#metrics-and-diagnostics)).

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

### `client.stats([subcommand])` → `Promise<Record<string, string>>`

Returns server statistics as a name/value map, useful for observability: connection counts,
evictions, `get_hits`/`get_misses`, memory usage and so on. An optional subcommand selects a
specific domain, e.g. `stats('items')`, `stats('slabs')` or `stats('settings')`. Only
`END`-terminated subcommands are supported (notably not `reset` or `cachedump`). With
multiple servers, the first server's stats are returned; per-node visibility needs a
client per node.

### `client.metrics()` → `ClientMetrics`

Returns a snapshot of client-side metrics (plain object, synchronous). See
[Metrics and diagnostics](#metrics-and-diagnostics) for the full field reference.

### `client.close()` → `Promise<void>`

Waits for in-flight commands to settle, then closes all connections. Idempotent. Commands
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
- Run `pnpm run benchmark` (with a local memcached) to compare pipelined SET/GET throughput
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

## Metrics and diagnostics

The client exposes observability data through two exporter-agnostic surfaces, both nearly
free when unused. Wiring them into Prometheus, OpenTelemetry or anything else is up to you;
the package keeps zero runtime dependencies.

### `client.metrics()`

Returns a plain-object snapshot of monotonic counters and current gauges, aggregated across
the client's connections. The cost of collection is plain integer increments on code paths
the client already executes, so it is always on.

```js
{
  commands: {
    issued: 0,     // accepted by the client (past argument validation)
    completed: 0,  // settled successfully, including misses
    failed: 0,     // rejected (connection, protocol or server errors)
    byVerb: {      // the same three counters per wire verb; all verbs are
      mg: { issued: 0, completed: 0, failed: 0 },  // always present
      ms: { issued: 0, completed: 0, failed: 0 },
      md: { issued: 0, completed: 0, failed: 0 },
      ma: { issued: 0, completed: 0, failed: 0 },
      mn: { issued: 0, completed: 0, failed: 0 },
      version: { issued: 0, completed: 0, failed: 0 },
      stats: { issued: 0, completed: 0, failed: 0 },
      auth: { issued: 0, completed: 0, failed: 0 }
    }
  },
  pipeline: {
    pendingDepth: 0,  // gauge: commands in flight or queued right now
    writes: 0,        // socket write() calls
    flushes: 0        // cork/uncork flushes (writev syscalls)
  },
  connection: {
    connects: 0,           // successful TCP connects
    disconnects: 0,        // closes of established connections
    reconnectAttempts: 0   // reconnection attempts scheduled
  },
  bytes: {
    read: 0,     // bytes received from the server
    written: 0   // bytes written to the socket
  }
}
```

Field semantics:

- `commands.issued` counts every command accepted by the client, including commands queued
  while disconnected and commands rejected because the client is closed. Argument validation
  errors (invalid key, TTL, CAS, ...) throw before the command exists and are not counted.
- `commands.completed` counts resolved commands: hits, misses and conditional stores/deletes
  that did not apply all complete. `commands.failed` counts rejections. The invariant
  `issued === completed + failed + pendingDepth` always holds.
- `pipeline.pendingDepth` is a gauge (not monotonic): the current length of the pending
  FIFO, i.e. commands awaiting a response plus commands queued while disconnected. A value
  that stays high indicates head-of-line blocking or an unreachable server.
- `pipeline.writes / pipeline.flushes` is the average number of commands coalesced per
  syscall by [auto-pipelining](#auto-pipelining-modes).
- `connection.disconnects` counts closes of *established* connections (including the one
  performed by `close()`); failed connection attempts surface in
  `connection.reconnectAttempts` instead.
- Latencies are deliberately not aggregated here: per-command durations flow through the
  diagnostics channels below, so the consumer chooses buckets, summaries or spans.

Snapshot fields, channel names and payload shapes are a **stable API**. Future multi-node
and pooling support will add labels/dimensions rather than change this shape.

A Prometheus recipe, reading the snapshot lazily at scrape time via `collect()`:

```js
import { Gauge } from 'prom-client'
import { Client } from '@platformatic/memcached'

const memcached = new Client('localhost:11211')

new Gauge({
  name: 'memcached_client_commands_total',
  help: 'Commands issued by the memcached client',
  labelNames: ['verb', 'state'],
  collect () {
    const { commands } = memcached.metrics()
    for (const [verb, counters] of Object.entries(commands.byVerb)) {
      this.labels(verb, 'completed').set(counters.completed)
      this.labels(verb, 'failed').set(counters.failed)
    }
  }
})

new Gauge({
  name: 'memcached_client_pending_depth',
  help: 'Commands awaiting a response',
  collect () {
    this.set(memcached.metrics().pipeline.pendingDepth)
  }
})
```

### Diagnostics channels

Per-operation events are published on [`node:diagnostics_channel`](https://nodejs.org/api/diagnostics_channel.html),
so subscribers only pay when subscribed: with no subscribers the client skips all payload
allocation and publishing.

Commands use a **tracing channel** named `platformatic.memcached.command`, which exposes the
five standard channels:

- `tracing:platformatic.memcached.command:start` — the command was issued.
- `tracing:platformatic.memcached.command:end` — the synchronous issue phase finished.
- `tracing:platformatic.memcached.command:error` — the command failed (published before the
  settlement events).
- `tracing:platformatic.memcached.command:asyncStart` / `:asyncEnd` — the command settled.

The same payload object flows through every event of a command, following
`diagnostics_channel.tracingChannel()` semantics — tracing integrations (e.g. OpenTelemetry
spans via [`@platformatic/memcached-otel`](../memcached-otel)) can attach state to it
and get exactly one logical span per command, regardless of how auto-pipelining batches
writes. The payload shape (`CommandDiagnosticsContext`):

| Field | Set on | Description |
| --- | --- | --- |
| `verb` | start | Wire verb: `mg`, `ms`, `md`, `ma`, `mn` or `version` |
| `host`, `port` | start | Server address |
| `requestSize` | start | Serialized command size in bytes, data block included |
| `key` | start | Only with `diagnosticsIncludeKeys: true` |
| `outcome` | settlement | `'success'`, `'miss'` (not found, or conditional store/delete not applied) or `'error'` |
| `durationMs` | settlement | Milliseconds from issue to settlement (`performance.now()` based) |
| `responseSize` | settlement | Value size in bytes, when the response carried one |
| `error` | settlement | The rejection error, when `outcome` is `'error'` |

Keys and values may carry sensitive data, so payloads never include values, and include keys
only when the client is created with `diagnosticsIncludeKeys: true`.

Connection lifecycle events are published on plain channels, with `{ host, port }` payloads:

- `platformatic.memcached.connection.connect` — the connection is established and ready.
- `platformatic.memcached.connection.disconnect` — an established connection closed; the
  payload also carries `error` (why it closed, a `ConnectionError` for deliberate `close()`).
- `platformatic.memcached.connection.reconnect` — a reconnection attempt was scheduled; the
  payload also carries `attempt` (1-based, resets on success) and `delayMs` (backoff before
  the attempt). Time-to-reconnect is the gap between this event and the next `connect`.

An OpenTelemetry recipe recording a latency histogram (spans are [`@platformatic/memcached-otel`](../memcached-otel) territory; histograms
only need one subscription):

```js
import { subscribe } from 'node:diagnostics_channel'
import { metrics } from '@opentelemetry/api'

const meter = metrics.getMeter('memcached-client')
const duration = meter.createHistogram('memcached.client.operation.duration', { unit: 'ms' })

subscribe('tracing:platformatic.memcached.command:asyncEnd', ctx => {
  duration.record(ctx.durationMs, {
    'db.operation.name': ctx.verb,
    'server.address': ctx.host,
    'server.port': ctx.port,
    'db.response.status': ctx.outcome
  })
})
```

## Testing

Tests run against a real memcached via Docker. `pnpm test` builds `dist/`, starts a
`memcached:alpine` container automatically (and stops it afterwards), or you can manage
the container yourself:

```bash
docker run -d --rm --name memcached -p 11211:11211 memcached:alpine
pnpm test
```

Tests are TypeScript executed directly by `node --test` through type stripping, so
development requires Node.js >= 22.18.0 (or any 23.6+/24+); consuming the published
package does not.

## Roadmap

- ElastiCache Auto Discovery (`config get cluster`) for dynamic node lists.

## License

Apache-2.0 — see [LICENSE](LICENSE).
