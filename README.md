# @platformatic/memcached

A minimal, high-performance [memcached](https://memcached.org/) client for Node.js, built on the
[meta text protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt).

This repository is a [pnpm](https://pnpm.io/) workspace.

## Packages

| Package | Documentation | Description |
| --- | --- | --- |
| [`@platformatic/memcached`](https://www.npmjs.com/package/@platformatic/memcached) | [Client README](./packages/memcached/README.md) | The memcached client: meta protocol, full request pipelining, zero runtime dependencies. |
| [`@platformatic/memcached-otel`](https://www.npmjs.com/package/@platformatic/memcached-otel) | [OpenTelemetry README](./packages/memcached-otel/README.md) | OpenTelemetry tracing instrumentation for the client, built on its diagnostics channels. |

## Quick start

```bash
npm install @platformatic/memcached
```

```js
import { Client } from '@platformatic/memcached'

const client = new Client('localhost:11211')

await client.set('greeting', 'hello', { ttl: 60 })
const value = await client.get('greeting')

console.log(value?.toString()) // 'hello'

await client.close()
```

For complete usage and API details, see the client documentation:

- [Installation](./packages/memcached/README.md#installation) and
  [quick start](./packages/memcached/README.md#quick-start)
- [Client API](./packages/memcached/README.md#api)
- [Authentication](./packages/memcached/README.md#authentication),
  [sharding](./packages/memcached/README.md#multiple-servers-client-side-sharding), and
  [ElastiCache Auto Discovery](./packages/memcached/README.md#elasticache-auto-discovery)
- [Metrics and diagnostics](./packages/memcached/README.md#metrics-and-diagnostics)
- [OpenTelemetry instrumentation](./packages/memcached-otel/README.md#usage)

## Performance

SET/GET throughput against the notable Node.js memcached clients, each running with its
best-known configuration — this client and [memjs](https://github.com/memcachier/memjs)
pipeline on a single connection, while
[memcache-client](https://github.com/electrode-io/memcache) and
[memcached](https://github.com/3rd-Eden/memcached) (3rd-Eden) get a 10-connection pool.
Median of 3 runs, 50,000 operations at concurrency 500, `memcached:alpine` on loopback,
Node.js 24, Linux:

| ops/s | @platformatic/memcached | memjs | memcache-client (pool 10) | memcached (pool 10) |
| --- | ---: | ---: | ---: | ---: |
| SET 64 B | **352,000** | 111,000 | 138,000 | 43,000 |
| GET 64 B | **369,000** | 137,000 | 113,000 | 59,000 |
| SET 4 KiB | **168,000** | 80,000 | 92,000 | 31,000 |
| GET 4 KiB | **135,000** | 92,000 | 87,000 | 10,000 |

Absolute numbers vary by machine: reproduce with
`node packages/memcached/benchmarks/compare-all.js` (details in the
[client's performance notes](./packages/memcached#performance-notes)).

## Development

```bash
pnpm install
pnpm build        # build every package
pnpm lint         # lint the whole workspace
pnpm typecheck    # typecheck every package
pnpm test         # run every package's tests
```

Tests run against a real memcached; `pnpm test` starts a `memcached:alpine` Docker
container automatically (and stops it afterwards). Source is TypeScript in the erasable
subset, executed directly by Node.js >= 22.18.0 via type stripping; published packages
ship compiled JavaScript and declaration files.

## License

Apache-2.0 — see [LICENSE](LICENSE).
