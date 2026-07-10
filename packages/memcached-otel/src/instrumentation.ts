import { SpanKind, SpanStatusCode, type Span } from '@opentelemetry/api'
import { InstrumentationBase, type InstrumentationConfig } from '@opentelemetry/instrumentation'
import type { CommandDiagnosticsContext } from '@platformatic/memcached'
import { subscribe, unsubscribe, type ChannelListener } from 'node:diagnostics_channel'

// Keep in sync with package.json
const PACKAGE_NAME = '@platformatic/memcached-otel'
const PACKAGE_VERSION = '0.1.0'

// The five channels of diagnostics_channel.tracingChannel('platformatic.memcached.command')
// published by @platformatic/memcached. The same payload object flows through every event
// of a command: `start` fires when the command is issued, `error` fires on failure (before
// settlement) and `asyncEnd` fires exactly once when the command settles, whatever the
// path (response, connection failure or client already closed).
const START_CHANNEL = 'tracing:platformatic.memcached.command:start'
const ERROR_CHANNEL = 'tracing:platformatic.memcached.command:error'
const ASYNC_END_CHANNEL = 'tracing:platformatic.memcached.command:asyncEnd'

export interface MemcachedInstrumentationConfig extends InstrumentationConfig {}

interface ChannelSubscription {
  name: string
  onMessage: ChannelListener
}

/**
 * OpenTelemetry instrumentation for @platformatic/memcached.
 *
 * The client publishes one set of tracing events per logical command on the
 * `platformatic.memcached.command` diagnostics channel; this instrumentation
 * turns each set into a single CLIENT span following the OpenTelemetry
 * database semantic conventions. There is no module patching involved, so it
 * works with any way of loading the client (ESM, CJS, bundled).
 *
 * memcached has no header mechanism, so no trace context is propagated to the
 * server: the spans are leaf spans by nature.
 */
export class MemcachedInstrumentation extends InstrumentationBase<MemcachedInstrumentationConfig> {
  // The base class constructor calls enable() before subclass field
  // initializers run, so enable is deferred to this constructor instead:
  // super() is always called with `enabled: false`.
  #subscriptions: ChannelSubscription[] = []
  #spans = new WeakMap<CommandDiagnosticsContext, Span>()

  constructor (config: MemcachedInstrumentationConfig = {}) {
    super(PACKAGE_NAME, PACKAGE_VERSION, { ...config, enabled: false })

    if (config.enabled !== false) {
      this.enable()
    }
  }

  // The client is instrumented through diagnostics channels, not by patching
  // its module exports.
  protected init (): void {}

  override enable (): void {
    super.enable()

    if (this.#subscriptions.length > 0) {
      return
    }

    this._config.enabled = true

    this.#subscribe(START_CHANNEL, message => this.#onStart(message as CommandDiagnosticsContext))
    this.#subscribe(ERROR_CHANNEL, message => this.#onError(message as CommandDiagnosticsContext))
    this.#subscribe(ASYNC_END_CHANNEL, message => this.#onAsyncEnd(message as CommandDiagnosticsContext))
  }

  override disable (): void {
    super.disable()
    this._config.enabled = false

    for (const { name, onMessage } of this.#subscriptions) {
      unsubscribe(name, onMessage)
    }

    this.#subscriptions = []
  }

  #subscribe (name: string, onMessage: ChannelListener): void {
    subscribe(name, onMessage)
    this.#subscriptions.push({ name, onMessage })
  }

  #onStart (ctx: CommandDiagnosticsContext): void {
    const span = this.tracer.startSpan(ctx.verb, {
      kind: SpanKind.CLIENT,
      attributes: {
        'db.system': 'memcached',
        'db.operation.name': ctx.verb,
        'server.address': ctx.host,
        'server.port': ctx.port
      }
    })

    // Only present when the client was created with diagnosticsIncludeKeys: true
    if (ctx.key !== undefined) {
      span.setAttribute('db.memcached.key', ctx.key)
    }

    this.#spans.set(ctx, span)
  }

  #onError (ctx: CommandDiagnosticsContext): void {
    const span = this.#spans.get(ctx)

    if (span === undefined || ctx.error === undefined) {
      return
    }

    span.recordException(ctx.error)
    span.setStatus({ code: SpanStatusCode.ERROR, message: ctx.error.message })
  }

  // asyncEnd is the settlement event: it fires exactly once per command, for
  // hits, misses and errors alike, so the span is ended here and only here.
  #onAsyncEnd (ctx: CommandDiagnosticsContext): void {
    const span = this.#spans.get(ctx)

    if (span === undefined) {
      return
    }

    this.#spans.delete(ctx)
    span.end()
  }
}
