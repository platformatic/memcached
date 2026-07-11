import { deepStrictEqual, notStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { test, type TestContext } from 'node:test'
import { Client, ConnectionError, ValidationError } from '../src/index.ts'
import { HashRing } from '../src/ring.ts'
import { FakeMemcached } from './fake.ts'

// These tests use in-process fake servers (see fake.ts) instead of the Docker
// memcached, so several nodes are available and per-node state is observable

async function createCluster (t: TestContext, size: number): Promise<{ servers: FakeMemcached[], client: Client }> {
  const servers = Array.from({ length: size }, () => new FakeMemcached())
  await Promise.all(servers.map(server => server.listen()))
  t.after(() => Promise.all(servers.map(server => server.close())))

  const client = new Client(servers.map(server => server.address), { reconnectDelay: 10, maxReconnectDelay: 20 })
  t.after(() => client.close())

  return { servers, client }
}

test('rejects an empty server list and duplicate servers', () => {
  throws(() => new Client([]), ValidationError)
  throws(() => new Client(['localhost:11211', 'localhost:11211']), ValidationError)
  throws(() => new Client([{ host: 'localhost', port: 11211 }, 'memcached://localhost:11211']), ValidationError)
})

test('a single-element server array behaves like a single server', async t => {
  const { servers, client } = await createCluster(t, 1)

  await client.set('key', 'value')
  deepStrictEqual(await client.get('key'), Buffer.from('value'))
  strictEqual(servers[0].store.size, 1)
})

test('keys are distributed across nodes and read back from the right node', async t => {
  const { servers, client } = await createCluster(t, 3)

  const keys = Array.from({ length: 200 }, (_, i) => `key:${i}`)
  await Promise.all(keys.map(key => client.set(key, `value:${key}`)))

  // Every node owns a share of the keyspace, and every key lives on exactly one node
  for (const server of servers) {
    ok(server.store.size > 0, `node got ${server.store.size} keys`)
  }
  strictEqual(servers.reduce((sum, server) => sum + server.store.size, 0), keys.length)

  // Reads route to the node holding each key
  const values = await Promise.all(keys.map(key => client.get(key)))
  for (let i = 0; i < keys.length; i++) {
    deepStrictEqual(values[i], Buffer.from(`value:${keys[i]}`))
  }

  // Deletes route the same way
  strictEqual(await client.delete(keys[0]), true)
  strictEqual(await client.get(keys[0]), null)
})

test('routing is stable across client instances', async t => {
  const { servers, client } = await createCluster(t, 3)

  const keys = Array.from({ length: 50 }, (_, i) => `stable:${i}`)
  await Promise.all(keys.map(key => client.set(key, key)))

  const other = new Client(servers.map(server => server.address))
  t.after(() => other.close())

  const values = await Promise.all(keys.map(key => other.get(key)))
  for (let i = 0; i < keys.length; i++) {
    deepStrictEqual(values[i], Buffer.from(keys[i]))
  }
})

test('a downed node fails fast without affecting other nodes', async t => {
  const { servers, client } = await createCluster(t, 2)

  const keys = Array.from({ length: 50 }, (_, i) => `down:${i}`)
  await Promise.all(keys.map(key => client.set(key, key)))

  const keyOnA = keys.find(key => servers[0].store.has(key))!
  const keyOnB = keys.find(key => servers[1].store.has(key))!

  await servers[1].close()

  // Keys owned by the downed node reject, the surviving node still serves
  await rejects(client.get(keyOnB), ConnectionError)
  deepStrictEqual(await client.get(keyOnA), Buffer.from(keyOnA))
})

test('version and noop reach every node', async t => {
  const { servers, client } = await createCluster(t, 3)

  strictEqual(await client.version(), '1.6.0-fake')
  await client.noop()

  for (const server of servers) {
    strictEqual(server.versions, 1)
    strictEqual(server.noops, 1)
  }
})

test('close closes every connection', async t => {
  const { client } = await createCluster(t, 3)

  await client.noop()
  await client.close()

  for (const connection of client.connections) {
    strictEqual(connection.closed, true)
  }

  await rejects(client.get('key'), ConnectionError)
})

// HashRing unit tests: properties that need many keys are cheaper without sockets

const NODES = [
  { host: '10.0.0.1', port: 11211 },
  { host: '10.0.0.2', port: 11211 },
  { host: '10.0.0.3', port: 11211 },
  { host: '10.0.0.4', port: 11211 }
]

test('the ring spreads keys over all nodes roughly evenly', () => {
  const ring = new HashRing(NODES)
  const total = 10000
  const counts = [0, 0, 0, 0]

  for (let i = 0; i < total; i++) {
    counts[ring.lookup(`key:${i}`)]++
  }

  for (const count of counts) {
    ok(count > total * 0.1 && count < total * 0.45, `node owns ${count} of ${total} keys`)
  }
})

test('removing a node only remaps the keys it owned', () => {
  const ring4 = new HashRing(NODES)
  const ring3 = new HashRing(NODES.slice(0, 3))
  const total = 10000
  let moved = 0

  for (let i = 0; i < total; i++) {
    const key = `key:${i}`
    const owner = ring4.lookup(key)

    if (owner === 3) {
      moved++
      notStrictEqual(ring3.lookup(key), 3)
    } else {
      // Keys on surviving nodes must not move: their ring points are unchanged
      strictEqual(ring3.lookup(key), owner)
    }
  }

  // The removed node owned roughly a quarter of the keyspace
  ok(moved > total * 0.1 && moved < total * 0.45, `${moved} of ${total} keys moved`)
})

test('lookups are deterministic', () => {
  const ring = new HashRing(NODES)
  const again = new HashRing(NODES)

  for (let i = 0; i < 1000; i++) {
    strictEqual(ring.lookup(`key:${i}`), again.lookup(`key:${i}`))
  }
})
