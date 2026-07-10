// Compares autoPipelining modes ('microtask' vs 'tick') and memjs under
// realistic concurrency: N independent async "handlers" issuing one small
// awaited GET per iteration, so commands come from separate microtask
// cascades, NOT one synchronous loop. Two arrival patterns are measured:
//
// - coalesced arrivals: handlers resume when responses arrive, so most
//   resumptions share the microtask cascade of a single socket 'data' event
// - scattered arrivals: each handler yields to the check phase between
//   operations (like handlers triggered by independent I/O events), so every
//   command is issued from its own macrotask
//
// Requires a memcached server on localhost:11211:
//   docker run -d --rm -p 11211:11211 memcached:alpine

import { randomBytes } from 'node:crypto'
import { setImmediate as immediate } from 'node:timers/promises'
import memjs from 'memjs'
import { Client } from '../index.js'

const HANDLERS = 500
const ITERATIONS = 100_000
const VALUE_SIZE = 64

async function run (iterations, handlers, task) {
  const start = process.hrtime.bigint()
  let remaining = iterations

  async function handler () {
    while (remaining-- > 0) {
      await task()
    }
  }

  const tasks = new Array(handlers)
  for (let i = 0; i < handlers; i++) {
    tasks[i] = handler()
  }
  await Promise.all(tasks)

  const elapsed = Number(process.hrtime.bigint() - start) / 1e9
  return iterations / elapsed
}

function report (label, opsPerSecond, batchSize) {
  const batch = batchSize === null ? '' : `  (avg ${batchSize.toFixed(1)} commands/write)`
  console.log(`  ${label.padEnd(36)} ${Math.round(opsPerSecond).toLocaleString('en-US').padStart(12)} ops/s${batch}`)
}

async function benchClient (label, client, scattered) {
  const before = { writes: client.connection.writes, flushes: client.connection.flushes }
  const task = scattered
    ? async () => {
      await immediate()
      await client.get('bench:auto')
    }
    : () => client.get('bench:auto')
  const opsPerSecond = await run(ITERATIONS, HANDLERS, task)
  const writes = client.connection.writes - before.writes
  const flushes = client.connection.flushes - before.flushes

  report(label, opsPerSecond, writes / flushes)
}

const value = randomBytes(VALUE_SIZE)

const microtaskClient = new Client('localhost:11211', { autoPipelining: 'microtask' })
const tickClient = new Client('localhost:11211', { autoPipelining: 'tick' })
const mjs = memjs.Client.create('localhost:11211')

await microtaskClient.set('bench:auto', value)

console.log(`${HANDLERS} concurrent handlers, one awaited GET per iteration, ${ITERATIONS} total ops, ${VALUE_SIZE}B values`)

console.log('\nCoalesced arrivals (handlers resume from response data events):')
await benchClient("@platformatic/memcached 'microtask'", microtaskClient, false)
await benchClient("@platformatic/memcached 'tick'", tickClient, false)
report('memjs', await run(ITERATIONS, HANDLERS, () => mjs.get('bench:auto')), null)

console.log('\nScattered arrivals (each command issued from its own macrotask):')
await benchClient("@platformatic/memcached 'microtask'", microtaskClient, true)
await benchClient("@platformatic/memcached 'tick'", tickClient, true)
report('memjs', await run(ITERATIONS, HANDLERS, async () => {
  await immediate()
  await mjs.get('bench:auto')
}), null)

await microtaskClient.close()
await tickClient.close()
mjs.close()
