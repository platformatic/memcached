// Bimodal workload (issue #22): many concurrent sub-millisecond GETs of
// small values while large (~1MB) SETs and GETs flow through the same
// process, measuring the latency distribution of the small operations.
// Small commands sharing a socket with a bulk transfer queue behind it
// (head-of-line blocking), so this compares:
//
// - a single connection per node (poolSize 1)
// - plain pools (poolSize 4 and 5) with least-outstanding-requests dispatch
// - the control/data split proposed in #22, modelled with two clients: a
//   poolSize-4 client carrying only the small operations and a dedicated
//   single-connection client carrying only the large transfers. This is
//   exactly the connection layout a largeValueThreshold option would
//   produce, without needing the option to exist.
//
// Both sides of the workload are paced to a fixed arrival rate so every
// scenario is compared under identical load; tune the pressure with
// LARGE_INTERVAL_MS (milliseconds between large SET+GET pairs per handler,
// 0 means unthrottled).
//
// Requires a memcached server on localhost:11211:
//   docker run -d --rm -p 11211:11211 memcached:alpine

import { randomBytes } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { Client } from '../src/index.ts'

const DURATION_MS = 5_000
const SMALL_HANDLERS = 32
const LARGE_HANDLERS = 4
const SMALL_SIZE = 100
// Just under memcached's default 1MB item size limit
const LARGE_SIZE = 1000 * 1024
const SMALL_KEYS = 100
const SMALL_INTERVAL_MS = 2
const LARGE_INTERVAL_MS = Number(process.env.LARGE_INTERVAL_MS ?? 50)

const smallValue = randomBytes(SMALL_SIZE)
const largeValue = randomBytes(LARGE_SIZE)

function percentile (sorted, p) {
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)]
}

// Runs small and large handlers concurrently for DURATION_MS and collects
// the latency of every small operation, in milliseconds
async function measure (small, large) {
  const latencies = []
  let largeOps = 0
  const start = process.hrtime.bigint()
  const deadline = performance.now() + DURATION_MS

  async function smallHandler (i) {
    while (performance.now() < deadline) {
      const next = performance.now() + SMALL_INTERVAL_MS
      const start = process.hrtime.bigint()
      await small(i)
      latencies.push(Number(process.hrtime.bigint() - start) / 1e6)

      const wait = next - performance.now()
      if (wait > 0) {
        await sleep(wait)
      }
    }
  }

  async function largeHandler (i) {
    while (performance.now() < deadline) {
      const next = performance.now() + LARGE_INTERVAL_MS
      await large(i)
      largeOps++

      const wait = next - performance.now()
      if (wait > 0) {
        await sleep(wait)
      }
    }
  }

  const tasks = []
  for (let i = 0; i < SMALL_HANDLERS; i++) {
    tasks.push(smallHandler(i))
  }
  for (let i = 0; i < LARGE_HANDLERS; i++) {
    tasks.push(largeHandler(i))
  }

  await Promise.all(tasks)
  const elapsed = Number(process.hrtime.bigint() - start) / 1e9

  latencies.sort((a, b) => a - b)
  return { latencies, smallRate: latencies.length / elapsed, largeRate: largeOps / elapsed }
}

async function bench (label, smallClient, largeClient) {
  // Seed the keys and warm up every connection of both clients
  for (let i = 0; i < SMALL_KEYS; i++) {
    await smallClient.set(`bimodal:small:${i}`, smallValue)
  }
  for (let i = 0; i < LARGE_HANDLERS; i++) {
    await largeClient.set(`bimodal:large:${i}`, largeValue)
  }
  await smallClient.noop()
  await largeClient.noop()

  const { latencies, smallRate, largeRate } = await measure(
    i => smallClient.get(`bimodal:small:${(i * 31) % SMALL_KEYS}`),
    async i => {
      await largeClient.set(`bimodal:large:${i}`, largeValue)
      await largeClient.get(`bimodal:large:${i}`)
    }
  )

  await smallClient.close()
  if (largeClient !== smallClient) {
    await largeClient.close()
  }

  const format = value => value.toFixed(3).padStart(8)
  console.log(
    `  ${label.padEnd(36)}` +
    ` ${format(percentile(latencies, 50))} ${format(percentile(latencies, 95))}` +
    ` ${format(percentile(latencies, 99))} ${format(percentile(latencies, 99.9))}` +
    ` ${format(latencies[latencies.length - 1])}` +
    ` ${Math.round(smallRate).toLocaleString('en-US').padStart(10)}` +
    ` ${Math.round((largeRate * LARGE_SIZE * 2) / (1024 * 1024)).toLocaleString('en-US').padStart(7)}`
  )
}

console.log(
  `${SMALL_HANDLERS} small GET handlers (${SMALL_SIZE}B values, one per ${SMALL_INTERVAL_MS}ms) racing ` +
  `${LARGE_HANDLERS} large SET+GET handlers (${LARGE_SIZE / 1024}KiB values, one pair per ${LARGE_INTERVAL_MS}ms), ` +
  `${DURATION_MS / 1000}s per scenario\n`
)
console.log(
  `  ${'small operation latency (ms)'.padEnd(36)} ${'p50'.padStart(8)} ${'p95'.padStart(8)}` +
  ` ${'p99'.padStart(8)} ${'p99.9'.padStart(8)} ${'max'.padStart(8)} ${'small/s'.padStart(10)} ${'MB/s'.padStart(7)}`
)

{
  const client = new Client('localhost:11211')
  await bench('single connection (poolSize 1)', client, client)
}
{
  const client = new Client('localhost:11211', { poolSize: 4 })
  await bench('poolSize 4', client, client)
}
{
  const client = new Client('localhost:11211', { poolSize: 5 })
  await bench('poolSize 5', client, client)
}
{
  const smallClient = new Client('localhost:11211', { poolSize: 4 })
  const largeClient = new Client('localhost:11211')
  await bench('split (poolSize 4 + data connection)', smallClient, largeClient)
}
