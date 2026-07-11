import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { before, test } from 'node:test'
import { SpanKind, SpanStatusCode } from '@opentelemetry/api'
import { ConnectionError } from '@platformatic/memcached'
import { createClient, createTracing, testKey, waitForServer } from './helper.ts'

before(() => waitForServer())

test('commands produce CLIENT spans with database attributes', async t => {
  const { exporter } = createTracing(t)
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  deepStrictEqual(await client.get(key), Buffer.from('value'))

  const spans = exporter.getFinishedSpans()
  deepStrictEqual(spans.map(span => span.name), ['ms', 'mg'])

  for (const span of spans) {
    strictEqual(span.kind, SpanKind.CLIENT)
    strictEqual(span.status.code, SpanStatusCode.UNSET)
    strictEqual(span.attributes['db.system'], 'memcached')
    strictEqual(span.attributes['db.operation.name'], span.name)
    strictEqual(span.attributes['server.address'], 'localhost')
    strictEqual(span.attributes['server.port'], 11211)
    strictEqual(span.attributes['db.memcached.key'], undefined)
    ok(span.endTime[0] > 0 || span.endTime[1] > 0)
  }
})

test('every verb gets its own span name', async t => {
  const { exporter } = createTracing(t)
  const client = createClient(t)
  const key = testKey()

  await client.set(key, '1')
  await client.get(key)
  await client.incr(key)
  await client.delete(key)
  await client.noop()
  await client.version()

  deepStrictEqual(
    exporter.getFinishedSpans().map(span => span.name),
    ['ms', 'mg', 'ma', 'md', 'mn', 'version']
  )
})

test('misses and successes both end the span without error status', async t => {
  const { exporter } = createTracing(t)
  const client = createClient(t)
  const key = testKey()

  strictEqual(await client.get(key), null) // miss
  await client.set(key, 'value')
  ok(await client.get(key)) // hit

  const spans = exporter.getFinishedSpans()
  strictEqual(spans.length, 3)

  for (const span of spans) {
    strictEqual(span.status.code, SpanStatusCode.UNSET)
    strictEqual(span.events.length, 0)
  }
})

test('a mid-flight socket destroy produces exactly one errored span', async t => {
  const { exporter } = createTracing(t)
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')

  const inflight = client.get(key)
  client.connection.socket!.destroy()

  await rejects(inflight, ConnectionError)

  const spans = exporter.getFinishedSpans().filter(span => span.name === 'mg')
  strictEqual(spans.length, 1)

  const [span] = spans
  strictEqual(span.status.code, SpanStatusCode.ERROR)
  ok(span.status.message!.length > 0)

  const exceptions = span.events.filter(event => event.name === 'exception')
  strictEqual(exceptions.length, 1)
  // recordException prefers error.code over error.name for exception.type
  strictEqual(exceptions[0].attributes!['exception.type'], 'PLT_MEMCACHED_CONNECTION_ERROR')
})

test('commands rejected because the client is closed still produce errored spans', async t => {
  const { exporter } = createTracing(t)
  const client = createClient(t)

  await client.close()
  await rejects(client.get(testKey()), ConnectionError)

  const spans = exporter.getFinishedSpans()
  strictEqual(spans.length, 1)
  strictEqual(spans[0].name, 'mg')
  strictEqual(spans[0].status.code, SpanStatusCode.ERROR)
})

test('the key is recorded only with diagnosticsIncludeKeys', async t => {
  const { exporter } = createTracing(t)
  const key = testKey()

  const plain = createClient(t)
  await plain.get(key)

  const withKeys = createClient(t, { diagnosticsIncludeKeys: true })
  await withKeys.get(key)

  const spans = exporter.getFinishedSpans()
  strictEqual(spans.length, 2)
  strictEqual(spans[0].attributes['db.memcached.key'], undefined)
  strictEqual(spans[1].attributes['db.memcached.key'], key)
})

test('disable stops span creation and enable resumes it', async t => {
  const { exporter, instrumentation } = createTracing(t)
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  strictEqual(exporter.getFinishedSpans().length, 1)

  instrumentation.disable()

  await client.get(key)
  strictEqual(exporter.getFinishedSpans().length, 1)

  instrumentation.enable()

  await client.get(key)
  strictEqual(exporter.getFinishedSpans().length, 2)
})

test('an instrumentation created with enabled: false does not subscribe', async t => {
  const { exporter } = createTracing(t, { enabled: false })
  const client = createClient(t)

  await client.get(testKey())
  strictEqual(exporter.getFinishedSpans().length, 0)
})

test('commands work and produce no spans when nothing is registered', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  deepStrictEqual(await client.get(key), Buffer.from('value'))
})
