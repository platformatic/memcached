import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { subscribe, tracingChannel, unsubscribe } from 'node:diagnostics_channel'
import { once } from 'node:events'
import { before, test, type TestContext } from 'node:test'
import { Client, ConnectionError, type CommandDiagnosticsContext, type ConnectionDiagnosticsEvent } from '../src/index.ts'
import { createClient, SERVER, testKey, waitForServer } from './helper.ts'

before(() => waitForServer())

const commandChannel = tracingChannel<unknown, CommandDiagnosticsContext>('platformatic.memcached.command')

interface RecordedEvent {
  name: string
  ctx: CommandDiagnosticsContext
}

function recordCommandEvents (t: TestContext): RecordedEvent[] {
  const events: RecordedEvent[] = []

  const handlers = {
    start: (ctx: CommandDiagnosticsContext) => events.push({ name: 'start', ctx }),
    end: (ctx: CommandDiagnosticsContext) => events.push({ name: 'end', ctx }),
    asyncStart: (ctx: CommandDiagnosticsContext) => events.push({ name: 'asyncStart', ctx }),
    asyncEnd: (ctx: CommandDiagnosticsContext) => events.push({ name: 'asyncEnd', ctx }),
    error: (ctx: CommandDiagnosticsContext) => events.push({ name: 'error', ctx })
  }

  commandChannel.subscribe(handlers)
  t.after(() => commandChannel.unsubscribe(handlers))

  return events
}

test('metrics() reports commands, pipeline, connection and bytes counters', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  deepStrictEqual(await client.get(key), Buffer.from('value'))
  strictEqual(await client.get(testKey()), null) // miss

  const metrics = client.metrics()

  strictEqual(metrics.commands.issued, 3)
  strictEqual(metrics.commands.completed, 3)
  strictEqual(metrics.commands.failed, 0)
  strictEqual(metrics.commands.byVerb.ms.issued, 1)
  strictEqual(metrics.commands.byVerb.ms.completed, 1)
  strictEqual(metrics.commands.byVerb.mg.issued, 2)
  strictEqual(metrics.commands.byVerb.mg.completed, 2)
  strictEqual(metrics.commands.byVerb.mg.failed, 0)

  // The shape is stable: every verb is always present
  deepStrictEqual(Object.keys(metrics.commands.byVerb).sort(), ['auth', 'ma', 'md', 'mg', 'mn', 'ms', 'stats', 'version'])

  strictEqual(metrics.pipeline.pendingDepth, 0)
  ok(metrics.pipeline.writes >= 3)
  ok(metrics.pipeline.flushes >= 1)
  ok(metrics.pipeline.writes >= metrics.pipeline.flushes)

  strictEqual(metrics.connection.connects, 1)
  strictEqual(metrics.connection.disconnects, 0)
  strictEqual(metrics.connection.reconnectAttempts, 0)

  ok(metrics.bytes.written > 0)
  ok(metrics.bytes.read > 0)
})

test('failed commands are counted, by verb too', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')

  const inflight = client.get(key)
  client.connection.socket!.destroy()
  await rejects(inflight, ConnectionError)

  const metrics = client.metrics()

  strictEqual(metrics.commands.issued, 2)
  strictEqual(metrics.commands.completed, 1)
  strictEqual(metrics.commands.failed, 1)
  strictEqual(metrics.commands.byVerb.mg.failed, 1)
  strictEqual(metrics.pipeline.pendingDepth, 0)
  strictEqual(metrics.connection.disconnects, 1)
  ok(metrics.connection.reconnectAttempts >= 1)
})

test('commands rejected after close count as issued and failed', async t => {
  const client = createClient(t)

  await client.version()
  await client.close()

  await rejects(client.get(testKey()), ConnectionError)

  const metrics = client.metrics()
  strictEqual(metrics.commands.issued, 2)
  strictEqual(metrics.commands.completed, 1)
  strictEqual(metrics.commands.failed, 1)
  strictEqual(metrics.commands.byVerb.mg.failed, 1)
})

test('pendingDepth tracks in-flight commands and returns to zero', async t => {
  const client = createClient(t)

  await client.version()

  const inflight = [client.get(testKey()), client.get(testKey()), client.get(testKey())]
  strictEqual(client.metrics().pipeline.pendingDepth, 3)

  await Promise.all(inflight)
  strictEqual(client.metrics().pipeline.pendingDepth, 0)
})

test('tracing channel reports verb, outcome and duration exactly once per command', async t => {
  const events = recordCommandEvents(t)
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  await client.get(key)
  await client.get(testKey()) // miss

  const starts = events.filter(e => e.name === 'start')
  const ends = events.filter(e => e.name === 'end')
  const asyncStarts = events.filter(e => e.name === 'asyncStart')
  const asyncEnds = events.filter(e => e.name === 'asyncEnd')
  const errors = events.filter(e => e.name === 'error')

  strictEqual(starts.length, 3)
  strictEqual(ends.length, 3)
  strictEqual(asyncStarts.length, 3)
  strictEqual(asyncEnds.length, 3)
  strictEqual(errors.length, 0)

  deepStrictEqual(starts.map(e => e.ctx.verb), ['ms', 'mg', 'mg'])
  deepStrictEqual(asyncEnds.map(e => e.ctx.outcome), ['success', 'success', 'miss'])

  // The same context object flows through the whole event sequence
  deepStrictEqual(asyncEnds.map(e => e.ctx), starts.map(e => e.ctx))
  strictEqual(new Set(asyncEnds.map(e => e.ctx)).size, 3)

  for (const { ctx } of asyncEnds) {
    strictEqual(typeof ctx.host, 'string')
    strictEqual(typeof ctx.port, 'number')
    ok(ctx.requestSize > 0)
    ok(typeof ctx.durationMs === 'number' && ctx.durationMs >= 0)
    ok(!('key' in ctx), 'keys must not be in payloads by default')
  }

  // The hit carries the value size, the miss does not
  strictEqual(asyncEnds[1].ctx.responseSize, 5)
  strictEqual(asyncEnds[2].ctx.responseSize, undefined)
})

test('tracing channel reports errors on connection failure', async t => {
  const events = recordCommandEvents(t)
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')

  const inflight = client.get(key)
  client.connection.socket!.destroy()
  await rejects(inflight, ConnectionError)

  const errors = events.filter(e => e.name === 'error')
  const asyncEnds = events.filter(e => e.name === 'asyncEnd')

  strictEqual(errors.length, 1)
  strictEqual(errors[0].ctx.verb, 'mg')
  strictEqual(errors[0].ctx.outcome, 'error')
  ok(errors[0].ctx.error instanceof ConnectionError)
  ok(typeof errors[0].ctx.durationMs === 'number' && errors[0].ctx.durationMs >= 0)

  // Exactly one settlement per command, on the failure path too
  strictEqual(asyncEnds.length, 2)
  deepStrictEqual(asyncEnds.map(e => e.ctx.outcome), ['success', 'error'])
})

test('keys are included in command payloads only with diagnosticsIncludeKeys', async t => {
  const events = recordCommandEvents(t)
  const client = createClient(t, SERVER, { diagnosticsIncludeKeys: true })
  const key = testKey()

  await client.set(key, 'value')
  await client.get(key)

  const starts = events.filter(e => e.name === 'start')
  strictEqual(starts.length, 2)
  strictEqual(starts[0].ctx.key, key)
  strictEqual(starts[1].ctx.key, key)
})

test('connection lifecycle channels fire on connect, disconnect and reconnect', async t => {
  const connects: ConnectionDiagnosticsEvent[] = []
  const disconnects: ConnectionDiagnosticsEvent[] = []
  const reconnects: ConnectionDiagnosticsEvent[] = []

  const onConnect = (event: unknown) => connects.push(event as ConnectionDiagnosticsEvent)
  const onDisconnect = (event: unknown) => disconnects.push(event as ConnectionDiagnosticsEvent)
  const onReconnect = (event: unknown) => reconnects.push(event as ConnectionDiagnosticsEvent)

  subscribe('platformatic.memcached.connection.connect', onConnect)
  subscribe('platformatic.memcached.connection.disconnect', onDisconnect)
  subscribe('platformatic.memcached.connection.reconnect', onReconnect)
  t.after(() => {
    unsubscribe('platformatic.memcached.connection.connect', onConnect)
    unsubscribe('platformatic.memcached.connection.disconnect', onDisconnect)
    unsubscribe('platformatic.memcached.connection.reconnect', onReconnect)
  })

  const client = new Client(SERVER)
  t.after(() => client.close())

  await client.version()
  strictEqual(connects.length, 1)
  strictEqual(typeof connects[0].host, 'string')
  strictEqual(typeof connects[0].port, 'number')

  client.connection.socket!.destroy()
  await once(client.connection, 'connect')

  strictEqual(connects.length, 2)
  strictEqual(disconnects.length, 1)
  ok(disconnects[0].error instanceof Error)
  strictEqual(reconnects.length, 1)
  strictEqual(reconnects[0].attempt, 1)
  strictEqual(typeof reconnects[0].delayMs, 'number')

  await client.close()
  strictEqual(disconnects.length, 2)
})

test('operations are unaffected when nothing is subscribed', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  deepStrictEqual(await client.get(key), Buffer.from('value'))
  strictEqual(await client.delete(key), true)
  strictEqual(await client.get(key), null)

  const metrics = client.metrics()
  strictEqual(metrics.commands.issued, 4)
  strictEqual(metrics.commands.completed, 4)
})
