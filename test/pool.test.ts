import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import { test, type TestContext } from 'node:test'
import { Client, ValidationError } from '../src/index.ts'
import { FakeMemcached } from './fake.ts'
import { createClient, testKey, waitForServer } from './helper.ts'

// Pooling tests use the in-process fake servers (see fake.ts) so pool
// members and per-connection traffic are observable, plus a real memcached
// for end-to-end correctness under pooling.

async function createCluster (
  t: TestContext,
  size: number,
  poolSize?: number
): Promise<{ servers: FakeMemcached[], client: Client }> {
  const servers = Array.from({ length: size }, () => new FakeMemcached())
  await Promise.all(servers.map(server => server.listen()))
  t.after(() => Promise.all(servers.map(server => server.close())))

  const client = new Client(servers.map(server => server.address), { poolSize })
  t.after(() => client.close())

  return { servers, client }
}

test('poolSize must be a positive integer', () => {
  throws(() => new Client('localhost:11211', { poolSize: 0 }), ValidationError)
  throws(() => new Client('localhost:11211', { poolSize: -1 }), ValidationError)
  throws(() => new Client('localhost:11211', { poolSize: 1.5 }), ValidationError)
  throws(() => new Client('localhost:11211', { poolSize: NaN }), ValidationError)
  throws(() => new Client('localhost:11211', { poolSize: Infinity }), ValidationError)
  // @ts-expect-error deliberately invalid type
  throws(() => new Client('localhost:11211', { poolSize: '2' }), ValidationError)
})

test('the default remains a single connection per node', async t => {
  const { servers, client } = await createCluster(t, 2)

  // noop fences every connection, so afterwards all sockets are established
  await client.noop()

  strictEqual(client.connections.length, 2)
  for (const server of servers) {
    strictEqual(server.connections, 1)
  }
})

test('poolSize opens that many connections to every node', async t => {
  const { servers, client } = await createCluster(t, 2, 3)

  await client.noop()

  strictEqual(client.connections.length, 6)
  for (const server of servers) {
    strictEqual(server.connections, 3)
  }
})

test('concurrent commands spread across all pool members', async t => {
  const { client } = await createCluster(t, 1, 4)

  const key = testKey()
  await client.set(key, 'value')
  await client.noop()

  const baselines = client.connections.map(connection => connection.writes)

  const values = await Promise.all(Array.from({ length: 40 }, () => client.get(key)))
  for (const value of values) {
    deepStrictEqual(value, Buffer.from('value'))
  }

  // Least-outstanding dispatch over identical commands degenerates to
  // round-robin: every pool member must have carried a share of the load
  for (let i = 0; i < client.connections.length; i++) {
    ok(client.connections[i].writes > baselines[i], `pool member ${i} got no traffic`)
  }
})

test('new commands prefer idle pool members over a blocked one', async t => {
  const { servers, client } = await createCluster(t, 1, 2)

  const slowKey = testKey()
  const fastKey = testKey()
  await client.set(slowKey, 'slow value')
  await client.set(fastKey, 'fast value')
  await client.noop()

  servers[0].delays.set(slowKey, 200)
  const [first, second] = client.connections
  const firstBaseline = first.writes
  const secondBaseline = second.writes

  // Both members are idle: the slow get lands on the first one and holds it
  const slow = client.get(slowKey)
  strictEqual(first.pending, 1)

  // While it is outstanding, every new command must pick the idle member
  for (let i = 0; i < 5; i++) {
    deepStrictEqual(await client.get(fastKey), Buffer.from('fast value'))
  }

  strictEqual(first.writes - firstBaseline, 1)
  strictEqual(second.writes - secondBaseline, 5)

  deepStrictEqual(await slow, Buffer.from('slow value'))
  strictEqual(first.pending, 0)
})

test('concurrent round-trips against a real server with poolSize 4', async t => {
  await waitForServer()

  const client = createClient(t, undefined, { poolSize: 4 })
  const keys = Array.from({ length: 100 }, () => testKey())

  await Promise.all(keys.map((key, i) => client.set(key, `value:${i}`)))

  const values = await Promise.all(keys.map(key => client.get(key)))
  for (let i = 0; i < keys.length; i++) {
    deepStrictEqual(values[i], Buffer.from(`value:${i}`))
  }

  const deleted = await Promise.all(keys.map(key => client.delete(key)))
  for (const result of deleted) {
    strictEqual(result, true)
  }
})
