import { deepStrictEqual, rejects, strictEqual } from 'node:assert'
import { once } from 'node:events'
import { createServer } from 'node:net'
import { before, test } from 'node:test'
import { ConnectionError } from '../index.js'
import { createClient, testKey, waitForServer } from './helper.js'

before(() => waitForServer())

test('in-flight commands are rejected when the socket is destroyed', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')

  const inflight = client.get(key)
  client.connection.socket.destroy()

  await rejects(inflight, ConnectionError)
})

test('the client reconnects automatically after a socket failure', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'survivor')

  client.connection.socket.destroy()
  await once(client.connection, 'connect')

  deepStrictEqual(await client.get(key), Buffer.from('survivor'))
})

test('commands issued while reconnecting are queued and executed', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')

  client.connection.socket.destroy()

  // Issued during the backoff window, before the new socket exists
  deepStrictEqual(await client.get(key), Buffer.from('value'))
})

test('close waits for in-flight commands and rejects later ones', async t => {
  const client = createClient(t)
  const key = testKey()

  await client.set(key, 'value')

  const inflight = client.get(key)
  const closed = client.close()

  deepStrictEqual(await inflight, Buffer.from('value'))
  await closed

  await rejects(client.get(key), ConnectionError)

  // close is idempotent
  await client.close()
})

test('connection errors reject commands with ConnectionError', async t => {
  // Bind an ephemeral port, then close it so connecting there is refused
  const server = createServer()
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const port = server.address().port
  await new Promise(resolve => server.close(resolve))

  const client = createClient(t, `127.0.0.1:${port}`, { reconnectDelay: 10, maxReconnectDelay: 20 })

  await rejects(client.get(testKey()), ConnectionError)
})

test('closed is reported by the connection', async t => {
  const client = createClient(t)

  await client.version()
  strictEqual(client.connection.closed, false)

  await client.close()
  strictEqual(client.connection.closed, true)
})
