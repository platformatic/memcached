import { deepStrictEqual, match, ok, strictEqual, throws } from 'node:assert'
import { before, test, type TestContext } from 'node:test'
import { Client, ConnectionError, ValidationError } from '../src/index.ts'
import { createClient, waitForServer } from './helper.ts'
import { FakeMemcached } from './fake.ts'

before(() => waitForServer())

async function createCluster (t: TestContext, size: number): Promise<{ servers: FakeMemcached[], client: Client }> {
  const servers = Array.from({ length: size }, () => new FakeMemcached())
  await Promise.all(servers.map(server => server.listen()))
  t.after(() => Promise.all(servers.map(server => server.close())))

  const client = new Client(servers.map(server => server.address), { reconnectDelay: 10, maxReconnectDelay: 20 })
  t.after(() => client.close())

  return { servers, client }
}

test('statsAll on a single node returns one entry matching stats', async t => {
  const client = createClient(t)

  const [entries, stats] = await Promise.all([client.statsAll(), client.stats()])

  strictEqual(entries.length, 1)
  const entry = entries[0]

  strictEqual(typeof entry.host, 'string')
  ok(Number.isInteger(entry.port))
  strictEqual(entry.error, null)
  ok(entry.stats !== null)
  ok(Object.keys(entry.stats).length > 0)

  // Counters move between calls, so only compare stable entries
  strictEqual(entry.stats.version, stats.version)
  strictEqual(entry.stats.pid, stats.pid)
  match(entry.stats.evictions, /^\d+$/)
})

test('statsAll queries every node and reports them in constructor order', async t => {
  const { servers, client } = await createCluster(t, 3)

  for (let i = 0; i < servers.length; i++) {
    servers[i].stats.set('node_id', String(i))
    servers[i].stats.set('evictions', String(i * 10))
  }

  const entries = await client.statsAll()

  strictEqual(entries.length, 3)

  for (let i = 0; i < servers.length; i++) {
    strictEqual(entries[i].host, '127.0.0.1')
    strictEqual(entries[i].port, servers[i].port)
    strictEqual(entries[i].error, null)
    // The stats record has a null prototype, so compare a spread copy
    deepStrictEqual({ ...entries[i].stats }, { version: '1.6.0-fake', evictions: String(i * 10), node_id: String(i) })
    deepStrictEqual(servers[i].statsQueries, [null])
  }
})

test('statsAll forwards the subcommand to every node', async t => {
  const { servers, client } = await createCluster(t, 2)

  for (const server of servers) {
    server.subcommandStats.set('items', new Map([['items:1:number', '42']]))
  }

  const entries = await client.statsAll('items')

  strictEqual(entries.length, 2)

  for (let i = 0; i < servers.length; i++) {
    strictEqual(entries[i].error, null)
    deepStrictEqual({ ...entries[i].stats }, { 'items:1:number': '42' })
    deepStrictEqual(servers[i].statsQueries, ['items'])
  }
})

test('a downed node gets an error entry while the others still report stats', async t => {
  const { servers, client } = await createCluster(t, 3)

  // Establish the connections first, then take the middle node down
  await client.statsAll()
  await servers[1].close()

  const entries = await client.statsAll()

  strictEqual(entries.length, 3)

  strictEqual(entries[0].error, null)
  ok(entries[0].stats !== null)

  strictEqual(entries[1].port, servers[1].port)
  strictEqual(entries[1].stats, null)
  ok(entries[1].error instanceof ConnectionError)

  strictEqual(entries[2].error, null)
  ok(entries[2].stats !== null)
})

test('statsAll rejects invalid subcommands synchronously', async t => {
  const { client } = await createCluster(t, 2)

  throws(() => client.statsAll(''), ValidationError)
  throws(() => client.statsAll('with space'), ValidationError)
  throws(() => client.statsAll('with\r\nnewline'), ValidationError)
  throws(() => client.statsAll('with\ttab'), ValidationError)
  throws(() => client.statsAll('a'.repeat(251)), ValidationError)
  throws(() => client.statsAll('non-ascii-é'), ValidationError)
  // @ts-expect-error - invalid on purpose
  throws(() => client.statsAll(42), ValidationError)
})

test('statsAll queries a single connection per node regardless of pool size', async t => {
  const servers = [new FakeMemcached(), new FakeMemcached()]
  await Promise.all(servers.map(server => server.listen()))
  t.after(() => Promise.all(servers.map(server => server.close())))

  const client = new Client(servers.map(server => server.address), { poolSize: 3 })
  t.after(() => client.close())

  const entries = await client.statsAll()

  strictEqual(entries.length, 2)

  for (let i = 0; i < servers.length; i++) {
    strictEqual(entries[i].port, servers[i].port)
    strictEqual(entries[i].error, null)
    deepStrictEqual(servers[i].statsQueries, [null])
  }
})
