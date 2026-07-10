import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { before, test } from 'node:test'
import { setImmediate as immediate } from 'node:timers/promises'
import { Client, ConnectionError, ValidationError } from '../index.js'
import { createClient, testKey, waitForServer } from './helper.js'

before(() => waitForServer())

test('true is an alias for tick mode and invalid values throw', t => {
  const client = createClient(t, undefined, { autoPipelining: true })
  ok(client instanceof Client)

  const explicit = createClient(t, undefined, { autoPipelining: 'microtask' })
  ok(explicit instanceof Client)

  throws(() => new Client('localhost:11211', { autoPipelining: 'sometimes' }), ValidationError)
  throws(() => new Client('localhost:11211', { autoPipelining: 42 }), ValidationError)
})

test('ordering is preserved under tick mode with interleaved async issuers', async t => {
  const client = createClient(t, undefined, { autoPipelining: 'tick' })
  const prefix = testKey()
  const issuers = 100
  const iterations = 20

  // Each issuer is an independent async context: every iteration resumes
  // from await in its own microtask cascade, not one synchronous loop.
  async function issuer (id) {
    const key = `${prefix}:${id}`

    for (let i = 0; i < iterations; i++) {
      await client.set(key, `value-${id}-${i}`)
      const readBack = await client.get(key)
      deepStrictEqual(readBack, Buffer.from(`value-${id}-${i}`), `issuer ${id} iteration ${i}`)

      // Stagger some issuers into different phases of the event loop
      if (i % 3 === id % 3) {
        await immediate()
      }
    }
  }

  const tasks = new Array(issuers)
  for (let i = 0; i < issuers; i++) {
    tasks[i] = issuer(i)
  }
  await Promise.all(tasks)
})

test('a command issued during the check phase still completes', async t => {
  const client = createClient(t, undefined, { autoPipelining: 'tick' })
  const key = testKey()

  await client.set(key, 'check-phase')

  // setImmediate callbacks run in the same phase the flush is scheduled in
  const value = await new Promise((resolve, reject) => {
    setImmediate(() => {
      client.get(key).then(resolve, reject)
    })
  })

  deepStrictEqual(value, Buffer.from('check-phase'))
})

test('close() while a flush is pending does not lose or double-settle commands', async t => {
  const client = createClient(t, undefined, { autoPipelining: 'tick' })
  const key = testKey()

  await client.set(key, 'pending-flush')

  // Issued and immediately followed by close() in the same synchronous
  // block: the command is corked and its flush is still scheduled.
  let settlements = 0
  const inflight = client.get(key).then(value => {
    settlements++
    return value
  })
  const closed = client.close()

  deepStrictEqual(await inflight, Buffer.from('pending-flush'))
  await closed
  strictEqual(settlements, 1)

  await rejects(client.get(key), ConnectionError)
})

test('reconnect while corked replays queued commands correctly', async t => {
  const client = createClient(t, undefined, { autoPipelining: 'tick' })
  const key = testKey()

  await client.set(key, 'before-drop')

  // The in-flight command is corked when the socket drops: it must reject,
  // while commands issued after the drop are queued and replayed.
  const inflight = client.get(key)
  client.connection.socket.destroy()

  const queued = [client.get(key), client.set(key, 'after-drop'), client.get(key)]

  await rejects(inflight, ConnectionError)

  const results = await Promise.all(queued)
  deepStrictEqual(results[0], Buffer.from('before-drop'))
  strictEqual(results[1], undefined)
  deepStrictEqual(results[2], Buffer.from('after-drop'))
})

test('tick mode coalesces issuers from separate macrotasks into fewer flushes', async t => {
  const microtaskClient = createClient(t, undefined, { autoPipelining: 'microtask' })
  const tickClient = createClient(t, undefined, { autoPipelining: 'tick' })
  const key = testKey()
  const count = 50

  await microtaskClient.set(key, 'x')
  // Ensures the tick client is connected so no flushes happen out-of-band
  await tickClient.version()

  // Each command is issued from its own setImmediate callback: a separate
  // macrotask with its own microtask checkpoint, like independent request
  // handlers. Microtask mode flushes once per callback, tick mode coalesces
  // the whole event loop iteration.
  async function load (client) {
    const before = { writes: client.connection.writes, flushes: client.connection.flushes }

    const commands = new Array(count)
    await new Promise(resolve => {
      let scheduled = 0

      for (let i = 0; i < count; i++) {
        setImmediate(() => {
          commands[i] = client.get(key)

          if (++scheduled === count) {
            resolve()
          }
        })
      }
    })

    const values = await Promise.all(commands)
    for (const value of values) {
      deepStrictEqual(value, Buffer.from('x'))
    }

    const writes = client.connection.writes - before.writes
    const flushes = client.connection.flushes - before.flushes
    return writes / flushes
  }

  const microtaskBatch = await load(microtaskClient)
  const tickBatch = await load(tickClient)

  // Microtask mode: one flush per macrotask. Tick mode: one flush for the
  // whole iteration (allow a bit of slack for scheduling boundaries).
  ok(microtaskBatch <= 2, `expected microtask mode to flush per macrotask, got ${microtaskBatch} commands per flush`)
  ok(tickBatch >= count / 2, `expected tick mode to coalesce the iteration, got ${tickBatch} commands per flush`)
})
