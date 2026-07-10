import { deepStrictEqual, match, ok, strictEqual, throws } from 'node:assert'
import { before, test } from 'node:test'
import { ValidationError } from '../src/index.ts'
import { createClient, testKey, waitForServer } from './helper.ts'

before(() => waitForServer())

test('stats returns general server statistics', async t => {
  const client = createClient(t)

  const stats = await client.stats()

  ok(Object.keys(stats).length > 0)
  match(stats.version, /^\d+\.\d+/)
  match(stats.curr_connections, /^\d+$/)
  match(stats.get_hits, /^\d+$/)
  match(stats.get_misses, /^\d+$/)
  match(stats.evictions, /^\d+$/)
})

test('stats items and slabs resolve after storing an item', async t => {
  const client = createClient(t)

  // Populate at least one slab so the subcommands have something to report
  await client.set(testKey(), 'value')

  const items = await client.stats('items')
  const slabs = await client.stats('slabs')

  ok(Object.keys(items).length > 0)
  ok(Object.keys(slabs).length > 0)

  for (const value of Object.values(items)) {
    strictEqual(typeof value, 'string')
  }
})

test('stats settings returns configuration entries', async t => {
  const client = createClient(t)

  const settings = await client.stats('settings')

  ok(Object.keys(settings).length > 0)
  match(settings.maxconns, /^\d+$/)
})

test('stats rejects invalid subcommands', t => {
  const client = createClient(t)

  throws(() => client.stats(''), ValidationError)
  throws(() => client.stats('with space'), ValidationError)
  throws(() => client.stats('with\r\nnewline'), ValidationError)
  throws(() => client.stats('with\ttab'), ValidationError)
  throws(() => client.stats('a'.repeat(251)), ValidationError)
  throws(() => client.stats('non-ascii-é'), ValidationError)
  // @ts-expect-error - invalid on purpose
  throws(() => client.stats(42), ValidationError)
})

test('stats pipelines with other commands', async t => {
  const client = createClient(t)
  const key = testKey()

  const [, stats, value, version] = await Promise.all([
    client.set(key, 'pipelined'),
    client.stats(),
    client.get(key),
    client.version()
  ])

  ok(Object.keys(stats).length > 0)
  match(stats.curr_connections, /^\d+$/)
  deepStrictEqual(value, Buffer.from('pipelined'))
  match(version, /^\d+\.\d+/)
})

test('concurrent stats commands do not interleave', async t => {
  const client = createClient(t)

  const [general, settings, again] = await Promise.all([
    client.stats(),
    client.stats('settings'),
    client.stats()
  ])

  match(general.curr_connections, /^\d+$/)
  match(settings.maxconns, /^\d+$/)
  match(again.curr_connections, /^\d+$/)
  strictEqual('maxconns' in general, false)
  strictEqual('curr_connections' in settings, false)
})
