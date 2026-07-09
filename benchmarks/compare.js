// Compares pipelined SET and GET throughput against memjs.
// Requires a memcached server on localhost:11211:
//   docker run -d --rm -p 11211:11211 memcached:alpine

import { randomBytes } from 'node:crypto'
import memjs from 'memjs'
import { Client } from '../index.js'

const ITERATIONS = 50_000
const CONCURRENCY = 500
const VALUE_SIZES = [64, 4096]

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
  console.log(`  ${label.padEnd(28)} ${Math.round(opsPerSecond).toLocaleString('en-US').padStart(12)} ops/s`)
}

const plt = new Client('localhost:11211')
const mjs = memjs.Client.create('localhost:11211')

for (const size of VALUE_SIZES) {
  const value = randomBytes(size)
  console.log(`\nValue size: ${size} bytes, ${ITERATIONS} operations, concurrency ${CONCURRENCY}`)

  // Warmup and key preparation
  for (let i = 0; i < CONCURRENCY; i++) {
    await plt.set(`bench:${i}`, value)
  }

  report('@platformatic/memcached SET', await run(ITERATIONS, CONCURRENCY, i => plt.set(`bench:${i % CONCURRENCY}`, value)))
  report('memjs SET', await run(ITERATIONS, CONCURRENCY, i => mjs.set(`bench:${i % CONCURRENCY}`, value, {})))
  report('@platformatic/memcached GET', await run(ITERATIONS, CONCURRENCY, i => plt.get(`bench:${i % CONCURRENCY}`)))
  report('memjs GET', await run(ITERATIONS, CONCURRENCY, i => mjs.get(`bench:${i % CONCURRENCY}`)))
}

await plt.close()
mjs.close()
