import { deepStrictEqual, strictEqual, match, ok } from 'node:assert'
import { randomBytes } from 'node:crypto'
import { before, test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { createClient, testKey, waitForServer } from './helper.ts'

before(() => waitForServer())

test('set and get round-trip with Buffer values', async t => {
  const client = createClient(t)
  const key = testKey()
  const value = Buffer.from('hello world')

  await client.set(key, value)
  const stored = await client.get(key)

  ok(Buffer.isBuffer(stored))
  deepStrictEqual(stored, value)
})

test('get returns null on a miss', async t => {
  const client = createClient(t)

  strictEqual(await client.get(testKey()), null)
})

test('string values are stored and returned as Buffers', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'plain string')
  deepStrictEqual(await client.get(key), Buffer.from('plain string'))
})

test('binary values containing CRLF sequences round-trip', async t => {
  const client = createClient(t)
  const key = testKey()
  const value = Buffer.concat([
    Buffer.from('EN\r\nHD O1\r\n'),
    Buffer.from([0, 13, 10, 255, 13, 10, 0]),
    Buffer.from('VA 5 O2\r\nvalue\r\n')
  ])

  await client.set(key, value)
  deepStrictEqual(await client.get(key), value)
})

test('large values spanning multiple TCP chunks round-trip', async t => {
  const client = createClient(t)
  const key = testKey()
  const value = randomBytes(512 * 1024)

  await client.set(key, value)
  deepStrictEqual(await client.get(key), value)
})

test('empty values round-trip', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, Buffer.alloc(0))
  deepStrictEqual(await client.get(key), Buffer.alloc(0))
})

test('add stores only when the key does not exist', async t => {
  const client = createClient(t)
  const key = testKey()

  strictEqual(await client.add(key, 'first'), true)
  strictEqual(await client.add(key, 'second'), false)
  deepStrictEqual(await client.get(key), Buffer.from('first'))
})

test('delete removes the key and reports misses', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  strictEqual(await client.delete(key), true)
  strictEqual(await client.get(key), null)
  strictEqual(await client.delete(key), false)
})

test('items expire after the TTL', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'ephemeral', { ttl: 1 })
  deepStrictEqual(await client.get(key), Buffer.from('ephemeral'))

  await sleep(2200)
  strictEqual(await client.get(key), null)
})

test('incr and decr update counters and return bigints', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, '10')
  strictEqual(await client.incr(key, 5), 15n)
  strictEqual(await client.incr(key), 16n)
  strictEqual(await client.decr(key, 6), 10n)
  strictEqual(await client.decr(key), 9n)
})

test('incr and decr return null on a miss', async t => {
  const client = createClient(t)

  strictEqual(await client.incr(testKey()), null)
  strictEqual(await client.decr(testKey(), 3), null)
})

test('version returns the server version', async t => {
  const client = createClient(t)

  match(await client.version(), /^\d+\.\d+/)
})

test('noop resolves', async t => {
  const client = createClient(t)

  strictEqual(await client.noop(), undefined)
})

test('accepts memcached:// URLs and address objects', async t => {
  const urlClient = createClient(t, 'memcached://localhost:11211')
  deepStrictEqual(await urlClient.version(), await urlClient.version())

  const objectClient = createClient(t, { host: 'localhost', port: 11211 })
  match(await objectClient.version(), /^\d+\.\d+/)
})
