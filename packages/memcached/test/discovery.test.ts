import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { test, type TestContext } from 'node:test'
import { Client, ConnectionError, ValidationError } from '../src/index.ts'
import { parseClusterConfig } from '../src/connection.ts'
import { FakeMemcached } from './fake.ts'

// These tests use in-process fake servers (see fake.ts): a set of nodes plus
// a dedicated configuration endpoint serving `config get cluster`, so cluster
// membership can be changed at runtime and per-node state is observable

const INTERVAL = 50

function triple (server: FakeMemcached): string {
  return `node-${server.port}.fake.cache|127.0.0.1|${server.port}`
}

async function waitFor (predicate: () => boolean, message: string, timeout = 3000): Promise<void> {
  const start = Date.now()

  while (!predicate()) {
    if (Date.now() - start > timeout) {
      throw new Error(`Timed out waiting for ${message}`)
    }

    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

interface DiscoveryCluster {
  nodes: FakeMemcached[]
  endpoint: FakeMemcached
  client: Client
}

async function createDiscoveryCluster (t: TestContext, size: number): Promise<DiscoveryCluster> {
  const nodes = Array.from({ length: size }, () => new FakeMemcached())
  const endpoint = new FakeMemcached()
  // Snapshot the list: tests mutate `nodes` to simulate membership changes
  const servers = [endpoint, ...nodes]
  await Promise.all(servers.map(server => server.listen()))
  t.after(() => Promise.all(servers.map(server => server.close())))

  endpoint.configVersion = 1
  endpoint.configNodes = nodes.map(triple)

  const client = new Client(
    { configEndpoint: endpoint.address },
    { autoDiscovery: { interval: INTERVAL }, reconnectDelay: 10, maxReconnectDelay: 20 }
  )
  t.after(() => client.close())

  return { nodes, endpoint, client }
}

// Returns which node serves each key, observed through the per-fake stores
function ownerOf (nodes: FakeMemcached[], key: string, value: string): number {
  for (let i = 0; i < nodes.length; i++) {
    if (nodes[i].store.get(key)?.toString() === value) {
      return i
    }
  }

  return -1
}

test('validates the configEndpoint and autoDiscovery options', () => {
  // configEndpoint is mutually exclusive with host/port and with arrays
  throws(() => new Client({ configEndpoint: 'localhost:11211', host: 'localhost' }), ValidationError)
  throws(() => new Client({ configEndpoint: 'localhost:11211', port: 11211 } as never), ValidationError)
  throws(() => new Client([{ configEndpoint: 'localhost:11211' } as never]), ValidationError)
  throws(() => new Client(['localhost:11211', { configEndpoint: 'localhost:11211' } as never]), ValidationError)

  // autoDiscovery composes with configEndpoint only, and cannot contradict it
  throws(() => new Client('localhost:11211', { autoDiscovery: true }), ValidationError)
  throws(() => new Client(['localhost:11211'], { autoDiscovery: { interval: 1000 } }), ValidationError)
  throws(() => new Client({ configEndpoint: 'localhost:11211' }, { autoDiscovery: false }), ValidationError)

  // Malformed endpoint and interval values
  throws(() => new Client({ configEndpoint: '' }), ValidationError)
  throws(() => new Client({ configEndpoint: 42 } as never), ValidationError)
  for (const interval of [0, -1, 1.5, '60s', NaN]) {
    throws(
      () => new Client({ configEndpoint: 'localhost:11211' }, { autoDiscovery: { interval: interval as never } }),
      ValidationError
    )
  }
})

test('parses the documented ElastiCache config format', () => {
  const config = parseClusterConfig(Buffer.from('12\nhost-a|10.0.0.1|11211 host-b||11212\n'))

  deepStrictEqual(config, {
    version: 12,
    nodes: [
      { host: '10.0.0.1', port: 11211 }, // the ip is preferred when present
      { host: 'host-b', port: 11212 } // and the hostname is the fallback
    ]
  })

  throws(() => parseClusterConfig(Buffer.from('not-a-version\nhost|ip|11211\n')))
  throws(() => parseClusterConfig(Buffer.from('1\nhost|10.0.0.1|not-a-port\n')))
  throws(() => parseClusterConfig(Buffer.from('1\n||11211\n')))
})

test('initial discovery seeds the ring and commands issued before it queue and flow', async t => {
  const { nodes, client } = await createDiscoveryCluster(t, 3)

  // Issued synchronously after the constructor, before any topology exists:
  // they are parked on the discovery gate and routed once it resolves
  const keys = Array.from({ length: 200 }, (_, i) => `discovery:${i}`)
  await Promise.all(keys.map(key => client.set(key, `value:${key}`)))

  strictEqual(client.connections.length, 3)

  // Every node owns a share of the keyspace, every key lives on exactly one node
  for (const node of nodes) {
    ok(node.store.size > 0, `node got ${node.store.size} keys`)
  }
  strictEqual(nodes.reduce((sum, node) => sum + node.store.size, 0), keys.length)

  // Reads route to the node holding each key
  const values = await Promise.all(keys.map(key => client.get(key)))
  for (let i = 0; i < keys.length; i++) {
    deepStrictEqual(values[i], Buffer.from(`value:${keys[i]}`))
  }
})

test('adding a node rebuilds the ring and only a fraction of the keys remap', async t => {
  const { nodes, endpoint, client } = await createDiscoveryCluster(t, 3)

  const keys = Array.from({ length: 200 }, (_, i) => `grow:${i}`)
  await Promise.all(keys.map(key => client.set(key, `v1:${key}`)))
  const before = keys.map(key => ownerOf(nodes, key, `v1:${key}`))

  // A new node joins the cluster: publish a higher version
  const added = new FakeMemcached()
  await added.listen()
  t.after(() => added.close())
  nodes.push(added)
  endpoint.configVersion = 2
  endpoint.configNodes = nodes.map(triple)

  await waitFor(() => client.connections.length === 4, 'the topology to grow')

  await Promise.all(keys.map(key => client.set(key, `v2:${key}`)))

  let moved = 0
  let landed = 0
  for (let i = 0; i < keys.length; i++) {
    const owner = ownerOf(nodes, keys[i], `v2:${keys[i]}`)
    if (owner !== before[i]) {
      moved++
    }
    if (owner === 3) {
      landed++
    }
  }

  // Consistent hashing: the new node took over roughly 1/4 of the keyspace
  // and every remapped key went to it - nothing else was reshuffled
  ok(landed > 0, 'the new node received keys')
  strictEqual(moved, landed)
  ok(moved > 0 && moved < keys.length * 0.6, `${moved} of ${keys.length} keys moved`)
})

test('removing a node closes its connection and remaps its keys', async t => {
  const { nodes, endpoint, client } = await createDiscoveryCluster(t, 3)

  const keys = Array.from({ length: 100 }, (_, i) => `shrink:${i}`)
  await Promise.all(keys.map(key => client.set(key, key)))

  const oldConnections = client.connections

  const removed = nodes.pop()!
  endpoint.configVersion = 2
  endpoint.configNodes = nodes.map(triple)

  await waitFor(() => client.connections.length === 2, 'the topology to shrink')

  // Surviving nodes keep their connection objects; the removed one is closed
  for (const connection of client.connections) {
    ok(oldConnections.includes(connection), 'surviving connections are reused')
  }
  const closed = oldConnections.filter(connection => !client.connections.includes(connection))
  strictEqual(closed.length, 1)
  await waitFor(() => closed[0].closed, 'the removed connection to close')

  // All keys are now served by the remaining nodes, none by the removed one
  removed.store.clear()
  await Promise.all(keys.map(key => client.set(key, `after:${key}`)))
  strictEqual(removed.store.size, 0)
  strictEqual(nodes.reduce((sum, node) => sum + node.store.size, 0), keys.length)
})

test('a configuration with a lower or equal version is ignored', async t => {
  const { nodes, endpoint, client } = await createDiscoveryCluster(t, 2)

  endpoint.configVersion = 5
  await client.set('versioned', 'value')
  const connections = client.connections

  // Publish a stale version with a different node list
  const decoy = new FakeMemcached()
  await decoy.listen()
  t.after(() => decoy.close())
  endpoint.configVersion = 3
  endpoint.configNodes = [triple(decoy)]

  const polls = endpoint.configGets
  await waitFor(() => endpoint.configGets >= polls + 2, 'two more polls')

  // Same connections, same routing: the stale config was not applied
  deepStrictEqual(client.connections, connections)
  deepStrictEqual(await client.get('versioned'), Buffer.from('value'))
  strictEqual(decoy.connections, 0)
  strictEqual(nodes.reduce((sum, node) => sum + node.store.size, 0), 1)
})

test('a malformed configuration is ignored and polling continues', async t => {
  const { endpoint, client } = await createDiscoveryCluster(t, 2)

  await client.set('malformed', 'value')
  const connections = client.connections

  // A non-numeric version makes the data block unparsable
  endpoint.configVersion = 'not-a-number'

  const polls = endpoint.configGets
  await waitFor(() => endpoint.configGets >= polls + 2, 'two more polls')

  deepStrictEqual(client.connections, connections)
  deepStrictEqual(await client.get('malformed'), Buffer.from('value'))

  // A valid, newer configuration published later is still applied
  const added = new FakeMemcached()
  await added.listen()
  t.after(() => added.close())
  endpoint.configVersion = 2
  endpoint.configNodes = [...endpoint.configNodes, triple(added)]

  await waitFor(() => client.connections.length === 3, 'the topology to recover')
})

test('an unreachable config endpoint keeps the last known topology', async t => {
  const { endpoint, client } = await createDiscoveryCluster(t, 2)

  await client.set('failsafe', 'value')

  await endpoint.close()
  await new Promise(resolve => setTimeout(resolve, INTERVAL * 3))

  // The last topology still serves reads and writes
  deepStrictEqual(await client.get('failsafe'), Buffer.from('value'))
  await client.set('failsafe:more', 'value')
  deepStrictEqual(await client.get('failsafe:more'), Buffer.from('value'))
  strictEqual(client.connections.length, 2)
})

test('close stops polling and closes the config and node connections', async t => {
  const { endpoint, client } = await createDiscoveryCluster(t, 2)

  await client.set('closing', 'value')
  await client.close()

  for (const connection of client.connections) {
    strictEqual(connection.closed, true)
  }

  // No poll timer is left behind: the endpoint sees no further requests
  const polls = endpoint.configGets
  await new Promise(resolve => setTimeout(resolve, INTERVAL * 3))
  strictEqual(endpoint.configGets, polls)

  await rejects(client.get('closing'), ConnectionError)
})

test('close before the first discovery rejects parked commands', async t => {
  // An endpoint that is not listening: discovery can never complete
  const endpoint = new FakeMemcached()
  await endpoint.listen()
  const address = endpoint.address
  await endpoint.close()

  const client = new Client(
    { configEndpoint: address },
    { autoDiscovery: { interval: INTERVAL }, reconnectDelay: 10, maxReconnectDelay: 20 }
  )

  const parked = rejects(client.get('never-routed'), ConnectionError)
  await client.close()

  await parked
  // Commands issued after close() reject as well
  await rejects(client.get('after-close'), ConnectionError)
})
