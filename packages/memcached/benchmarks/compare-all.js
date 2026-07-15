// Compares SET and GET throughput across the notable Node.js memcached
// clients. Requires a memcached server on localhost:11211:
//   docker run -d --rm -p 11211:11211 memcached:alpine
//
// Each client runs with its best-known configuration, so the comparison is
// between what each library can actually deliver rather than its defaults:
// @platformatic/memcached and memjs pipeline on a single connection;
// memcached (3rd-Eden) and memcache-client do not pipeline the same way, so
// they get a 10-connection pool - their documented tuning for concurrency.

import { randomBytes } from 'node:crypto'
import { promisify } from 'node:util'
import { MemcacheClient } from 'memcache-client'
import Memcached from 'memcached'
import memjs from 'memjs'
import { Client } from '../src/index.ts'

const ITERATIONS = 50_000
const CONCURRENCY = 500
const VALUE_SIZES = [64, 4096]
const POOL_SIZE = 10

async function run (iterations, concurrency, task) {
  const start = process.hrtime.bigint()
  let index = 0

  async function worker () {
    while (true) {
      const i = index++
      if (i >= iterations) {
        return
      }

      await task(i)
    }
  }

  const workers = new Array(concurrency)
  for (let i = 0; i < concurrency; i++) {
    workers[i] = worker()
  }
  await Promise.all(workers)

  const elapsed = Number(process.hrtime.bigint() - start) / 1e9
  return iterations / elapsed
}

function report (label, opsPerSecond) {
  console.log(`  ${label.padEnd(34)} ${Math.round(opsPerSecond).toLocaleString('en-US').padStart(12)} ops/s`)
}

// Caps the number of operations concurrently handed to a client. The
// 3rd-Eden client's connection pool wedges nondeterministically when
// hundreds of operations are in flight at once, so its work is throttled to
// twice its pool - it queues internally anyway, and this is the
// configuration under which it performs best.
function throttle (limit, task) {
  let active = 0
  const queue = []

  return async (...args) => {
    if (active >= limit) {
      await new Promise(resolve => queue.push(resolve))
    }

    active++
    try {
      return await task(...args)
    } finally {
      active--
      queue.shift()?.()
    }
  }
}

const plt = new Client('localhost:11211')
const mjs = memjs.Client.create('localhost:11211')
const threed = new Memcached('localhost:11211', { poolSize: POOL_SIZE, timeout: 10000 })
const threedSet = promisify(threed.set.bind(threed))
const threedGet = promisify(threed.get.bind(threed))
const mcc = new MemcacheClient({ server: { server: 'localhost:11211', maxConnections: POOL_SIZE } })

const clients = [
  {
    label: '@platformatic/memcached',
    set: (key, value) => plt.set(key, value),
    get: key => plt.get(key)
  },
  {
    label: 'memjs',
    set: (key, value) => mjs.set(key, value, {}),
    get: key => mjs.get(key)
  },
  {
    label: `memcached (pool ${POOL_SIZE})`,
    set: throttle(POOL_SIZE * 2, (key, value) => threedSet(key, value, 0)),
    get: throttle(POOL_SIZE * 2, key => threedGet(key))
  },
  {
    label: `memcache-client (pool ${POOL_SIZE})`,
    set: (key, value) => mcc.set(key, value),
    get: key => mcc.get(key)
  }
]

for (const size of VALUE_SIZES) {
  const value = randomBytes(size)
  console.log(`\nValue size: ${size} bytes, ${ITERATIONS} operations, concurrency ${CONCURRENCY}`)

  // Warmup and key preparation
  for (let i = 0; i < CONCURRENCY; i++) {
    await plt.set(`bench:${i}`, value)
  }

  for (const client of clients) {
    report(`${client.label} SET`, await run(ITERATIONS, CONCURRENCY, i => client.set(`bench:${i % CONCURRENCY}`, value)))
  }

  for (const client of clients) {
    report(`${client.label} GET`, await run(ITERATIONS, CONCURRENCY, i => client.get(`bench:${i % CONCURRENCY}`)))
  }
}

await plt.close()
mjs.close()
threed.end()
mcc.shutdown()
