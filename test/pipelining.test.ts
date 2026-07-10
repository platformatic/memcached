import { deepStrictEqual, strictEqual } from 'node:assert'
import { before, test } from 'node:test'
import { createClient, testKey, waitForServer } from './helper.ts'

before(() => waitForServer())

test('1000 parallel sets and gets are pipelined correctly', async t => {
  const client = createClient(t)
  const prefix = testKey()
  const count = 1000

  const sets = new Array(count)
  for (let i = 0; i < count; i++) {
    sets[i] = client.set(`${prefix}:${i}`, `value-${i}`)
  }
  await Promise.all(sets)

  const gets = new Array(count)
  for (let i = 0; i < count; i++) {
    gets[i] = client.get(`${prefix}:${i}`)
  }
  const values = await Promise.all(gets)

  for (let i = 0; i < count; i++) {
    deepStrictEqual(values[i], Buffer.from(`value-${i}`), `mismatch for key ${i}`)
  }
})

test('mixed pipelined operations keep responses correlated', async t => {
  const client = createClient(t)
  const prefix = testKey()

  await client.set(`${prefix}:counter`, '0')

  const operations = []
  for (let i = 0; i < 250; i++) {
    operations.push(client.set(`${prefix}:${i}`, `v${i}`))
    operations.push(client.get(`${prefix}:missing:${i}`))
    operations.push(client.incr(`${prefix}:counter`, 1))
    operations.push(client.add(`${prefix}:${i}`, 'never'))
  }

  const results = await Promise.all(operations)

  for (let i = 0; i < 250; i++) {
    strictEqual(results[i * 4], undefined)
    strictEqual(results[i * 4 + 1], null)
    strictEqual(typeof results[i * 4 + 2], 'bigint')
    strictEqual(results[i * 4 + 3], false)
  }

  const counter = await client.incr(`${prefix}:counter`, 1)
  strictEqual(counter, 251n)
})

test('commands issued before the connection is established are flushed', async t => {
  const client = createClient(t)
  const key = testKey()

  // No awaiting connect: these must be queued and flushed on connect
  const results = await Promise.all([client.set(key, 'early'), client.get(key)])

  deepStrictEqual(results[1], Buffer.from('early'))
})
