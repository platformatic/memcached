# @platformatic/memcached-otel

OpenTelemetry tracing for [`@platformatic/memcached`](../memcached).

The client publishes one set of [`node:diagnostics_channel`](https://nodejs.org/api/diagnostics_channel.html)
tracing events per logical command on the `platformatic.memcached.command` channel. This
package subscribes to those events and turns each command into a single `CLIENT` span
following the OpenTelemetry [database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/):
no module patching, no monkey-patching — it works however the client is loaded (ESM, CJS,
bundled) and adds zero overhead to commands when disabled.

## Installation

```bash
npm install @platformatic/memcached-otel @opentelemetry/api
```

`@opentelemetry/api` and `@platformatic/memcached` are peer dependencies.

## Usage

`MemcachedInstrumentation` is a standard [`Instrumentation`](https://open-telemetry.github.io/opentelemetry-js/interfaces/_opentelemetry_instrumentation.Instrumentation.html)
subclass, so it plugs into the usual SDK setup:

```js
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { registerInstrumentations } from '@opentelemetry/instrumentation'
import { MemcachedInstrumentation } from '@platformatic/memcached-otel'

const provider = new NodeTracerProvider()
provider.register()

registerInstrumentations({
  instrumentations: [new MemcachedInstrumentation()]
})
```

Or standalone, without the registry:

```js
const instrumentation = new MemcachedInstrumentation()
instrumentation.setTracerProvider(provider) // omit to use the global provider

// later
instrumentation.disable()
```

It also composes with `@opentelemetry/auto-instrumentations-node`: add it to the
`instrumentations` array next to the bundled ones.

Because subscription happens through diagnostics channels, the instrumentation traces every
`Client` in the process, including clients created before it was enabled. `disable()`
unsubscribes and stops span creation immediately; when nothing is subscribed the client
skips all diagnostics payload allocation.

## Spans

One span per logical command, regardless of how auto-pipelining batches socket writes:

- **Name**: the wire verb (`mg`, `ms`, `md`, `ma`, `mn` or `version`).
- **Kind**: `CLIENT`.
- **Attributes**: `db.system` = `memcached`, `db.operation.name` = the verb,
  `server.address` and `server.port`.
- **Key**: when the client is created with `diagnosticsIncludeKeys: true`, the key is added
  as `db.memcached.key`. Keys may carry sensitive data, so this is off by default — see the
  client's [diagnostics docs](../memcached/README.md#diagnostics-channels).
- **Status**: errors (connection, protocol or server errors) set span status `ERROR` and
  record the exception. Misses and conditional stores/deletes that did not apply are not
  errors: the span ends with status unset, like a hit.

The span starts when the command is issued (queueing and reconnection wait time are part of
the span) and ends exactly once when the command settles.

memcached has no header mechanism, so no trace context is propagated to the server: these
are **leaf spans** by nature. They parent to whatever span is active in the current context
when the command is issued.

## License

Apache-2.0 — see [LICENSE](LICENSE).
