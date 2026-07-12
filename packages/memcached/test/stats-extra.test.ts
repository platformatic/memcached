import { deepStrictEqual, match, ok, rejects, strictEqual, throws } from 'node:assert'
import { before, test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { ProtocolError, ValidationError, type CachedumpItem } from '../src/index.ts'
import { createClient, testKey, waitForServer } from './helper.ts'

before(() => waitForServer())

test('resetStats clears server statistics counters', async t => {
  const client = createClient(t)
  const key = testKey()
  await client.set(key, 'value')

  // Push get_hits well above zero so the directional assertion below is
  // robust against concurrent test files moving the counter
  for (let i = 0; i < 25; i++) {
    await Promise.all(Array.from({ length: 8 }, () => client.get(key)))
  }

  const hitsBefore = Number((await client.stats()).get_hits)
  ok(hitsBefore >= 200)

  await client.resetStats()

  // Other test files run concurrently and bump the counter again, so assert
  // directionally: right after the reset the counter restarted from zero and
  // must be below the value our own gets pushed it to
  const hitsAfter = Number((await client.stats()).get_hits)
  ok(hitsAfter < hitsBefore, `expected get_hits to restart after reset, got ${hitsAfter} >= ${hitsBefore}`)
})

test('cachedump lists stored keys with size and exptime', async t => {
  const client = createClient(t)
  const persistent = testKey()
  const expiring = testKey()
  // An unusual size lands the keys in a sparse slab class: the busy classes
  // holding the small values of other test files can exceed the ~2MB
  // server-side dump cap, which would hide our keys from the dump
  const value = 'x'.repeat(7777)
  const start = Math.floor(Date.now() / 1000)

  await client.set(persistent, value)
  await client.set(expiring, value, { ttl: 300 })

  // Newly stored items only become visible to cachedump once the LRU
  // maintainer has moved them out of the hot LRU (typically within a couple
  // of seconds), so poll every slab class listed by "stats items". The dump
  // walks each LRU from its head (most recent first), so a generous limit
  // keeps the crowded classes cheap without hiding freshly moved items.
  const deadline = Date.now() + 15000
  const found = new Map<string, CachedumpItem>()
  const slabOf = new Map<string, number>()

  while (Date.now() < deadline) {
    const items = await client.stats('items')
    const slabs = new Set<number>()

    for (const name of Object.keys(items)) {
      const matched = /^items:(\d+):/.exec(name)

      if (matched !== null) {
        slabs.add(Number(matched[1]))
      }
    }

    found.clear()
    slabOf.clear()

    for (const slab of slabs) {
      for (const item of await client.cachedump(slab, 2000)) {
        found.set(item.key, item)
        slabOf.set(item.key, slab)
      }
    }

    if (found.has(persistent) && found.has(expiring)) {
      break
    }

    await sleep(250)
  }

  const persistentItem = found.get(persistent)
  const expiringItem = found.get(expiring)
  ok(persistentItem !== undefined, 'stored key not found in cachedump')
  ok(expiringItem !== undefined, 'stored TTL key not found in cachedump')

  strictEqual(persistentItem.size, value.length)
  strictEqual(persistentItem.exptime, 0)

  // The expiration is reported as an absolute Unix timestamp
  strictEqual(expiringItem.size, value.length)
  ok(expiringItem.exptime >= start + 300 - 60)
  ok(expiringItem.exptime <= start + 300 + 60)

  // Both keys have identical sizes, so they share a slab class holding at
  // least two items: a limit of 1 must cap the dump
  const slab = slabOf.get(persistent)!
  strictEqual(slabOf.get(expiring), slab)
  const limited = await client.cachedump(slab, 1)
  strictEqual(limited.length, 1)
})

test('cachedump returns an empty array for an unused slab class', async t => {
  const client = createClient(t)

  // With the default growth factor the highest allocated class is far below
  // 63, which the server still accepts as a valid (empty) slab class id
  deepStrictEqual(await client.cachedump(63), [])
})

test('cachedump surfaces server rejections without desyncing the connection', async t => {
  const client = createClient(t)

  // Slab class ids above the server's compile-time maximum are rejected
  // with "CLIENT_ERROR Illegal slab id"
  await rejects(client.cachedump(100000), ProtocolError)

  // The connection survives and stays correlated
  match(await client.version(), /^\d+\.\d+/)
})

test('unknown stats subcommands fail without desyncing the connection', async t => {
  const client = createClient(t)

  await rejects(client.stats('bogus'), ProtocolError)
  match(await client.version(), /^\d+\.\d+/)
})

test('stats rejects the reset and cachedump subcommands', t => {
  const client = createClient(t)

  throws(() => client.stats('reset'), ValidationError)
  throws(() => client.stats('cachedump'), ValidationError)
  throws(() => client.stats('cachedump 1 10'), ValidationError)
})

test('cachedump validates arguments', t => {
  const client = createClient(t)

  throws(() => client.cachedump(0), ValidationError)
  throws(() => client.cachedump(-1), ValidationError)
  throws(() => client.cachedump(1.5), ValidationError)
  throws(() => client.cachedump(Number.NaN), ValidationError)
  throws(() => client.cachedump(Number.POSITIVE_INFINITY), ValidationError)
  // @ts-expect-error - invalid on purpose
  throws(() => client.cachedump('1'), ValidationError)
  // @ts-expect-error - invalid on purpose
  throws(() => client.cachedump(), ValidationError)
  throws(() => client.cachedump(1, -1), ValidationError)
  throws(() => client.cachedump(1, 1.5), ValidationError)
  // @ts-expect-error - invalid on purpose
  throws(() => client.cachedump(1, '10'), ValidationError)
})

test('resetStats and cachedump pipeline safely with other commands', async t => {
  const client = createClient(t)
  const key = testKey()

  const [, stats, value, , dump, version] = await Promise.all([
    client.set(key, 'pipelined'),
    client.stats(),
    client.get(key),
    client.resetStats(),
    client.cachedump(1, 5),
    client.version()
  ])

  ok(Object.keys(stats).length > 0)
  deepStrictEqual(value, Buffer.from('pipelined'))
  ok(Array.isArray(dump))

  for (const item of dump) {
    strictEqual(typeof item.key, 'string')
    strictEqual(typeof item.size, 'number')
    strictEqual(typeof item.exptime, 'number')
  }

  match(version, /^\d+\.\d+/)

  // Every command settled exactly once through the settle helpers
  strictEqual(client.metrics().pipeline.pendingDepth, 0)
})
