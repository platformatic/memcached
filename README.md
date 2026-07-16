# @platformatic/memcached

A minimal, high-performance [memcached](https://memcached.org/) client for Node.js, built on the
[meta text protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt).

> **Status**: this package is currently **private and experimental**. It backs the memcached
> storage adapter for Platformatic gateway request deduplication. APIs may change before a
> public release.

This repository is a [pnpm](https://pnpm.io/) workspace.

## Packages

| Package | Description |
| --- | --- |
| [`@platformatic/memcached`](./packages/memcached) | The memcached client: meta protocol, full request pipelining, zero runtime dependencies. |
| [`@platformatic/memcached-otel`](./packages/memcached-otel) | OpenTelemetry tracing instrumentation for the client, built on its diagnostics channels. |

See each package's README for full documentation.

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
