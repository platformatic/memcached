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
