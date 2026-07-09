import { deepStrictEqual, notStrictEqual, strictEqual, match, ok } from 'node:assert'
import { before, test } from 'node:test'
import { createClient, testKey, waitForServer } from './helper.js'

before(() => waitForServer())

test('gets returns the value and an opaque CAS token', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')
  const result = await client.gets(key)

  ok(Buffer.isBuffer(result.value))
  deepStrictEqual(result.value, Buffer.from('value'))
  strictEqual(typeof result.cas, 'string')
  match(result.cas, /^\d+$/)
})

test('gets returns null on a miss', async t => {
  const client = createClient(t)

  strictEqual(await client.gets(testKey()), null)
})

test('cas succeeds when the token matches', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'original')
  const { cas } = await client.gets(key)

  strictEqual(await client.cas(key, 'updated', cas), true)
  deepStrictEqual(await client.get(key), Buffer.from('updated'))
})

test('cas fails when the item was modified concurrently', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'original')
  const { cas } = await client.gets(key)

  // Simulate a concurrent writer
  await client.set(key, 'concurrent')

  strictEqual(await client.cas(key, 'updated', cas), false)
  deepStrictEqual(await client.get(key), Buffer.from('concurrent'))

  // The token changes after every write
  const updated = await client.gets(key)
  notStrictEqual(updated.cas, cas)
})

test('cas fails when the key does not exist', async t => {
  const client = createClient(t)

  strictEqual(await client.cas(testKey(), 'value', '12345'), false)
})

test('CAS-guarded delete removes the item only when the token matches', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'locked')
  const { cas } = await client.gets(key)

  strictEqual(await client.delete(key, { cas: '99999999' }), false)
  deepStrictEqual(await client.get(key), Buffer.from('locked'))

  strictEqual(await client.delete(key, { cas }), true)
  strictEqual(await client.get(key), null)
})

test('lock/unlock pattern: add + gets + CAS-guarded delete', async t => {
  const client = createClient(t)
  const key = testKey()

  // Acquire the lock
  strictEqual(await client.add(key, 'token-a', { ttl: 30 }), true)

  // A second contender cannot acquire it
  strictEqual(await client.add(key, 'token-b', { ttl: 30 }), false)

  // Unlock with token verification
  const current = await client.gets(key)
  deepStrictEqual(current.value, Buffer.from('token-a'))
  strictEqual(await client.delete(key, { cas: current.cas }), true)

  // Lock is free again
  strictEqual(await client.add(key, 'token-b', { ttl: 30 }), true)
})
