import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { once } from 'node:events'
import { before, test } from 'node:test'
import { AuthenticationError, Client, ProtocolError, ValidationError } from '../src/index.ts'
import { createClient, testKey, waitForServer } from './helper.ts'

// A dedicated container started with `memcached -Y <authfile>`, see pretest.
// Credentials must match test/fixtures/authfile.
const AUTH_SERVER = process.env.MEMCACHED_AUTH_URL ?? 'localhost:11214'
const AUTH_PORT = Number(AUTH_SERVER.split(':').pop())
const USERNAME = 'testuser'
const PASSWORD = 'testpass'

before(() => waitForServer(AUTH_PORT))

test('correct credentials authenticate and commands work', async t => {
  const client = createClient(t, AUTH_SERVER, { username: USERNAME, password: PASSWORD })
  const key = testKey()

  await client.set(key, 'value')
  deepStrictEqual(await client.get(key), Buffer.from('value'))
})

test('commands queued before the connection are authenticated first', async t => {
  const client = createClient(t, AUTH_SERVER, { username: USERNAME, password: PASSWORD })
  const key = testKey()

  // Issued synchronously before the socket connects: the auth command must
  // still be written ahead of them on connect.
  const [, value] = await Promise.all([client.set(key, 'queued'), client.get(key)])
  deepStrictEqual(value, Buffer.from('queued'))
})

test('wrong credentials reject commands with AuthenticationError', async t => {
  const client = createClient(t, AUTH_SERVER, { username: USERNAME, password: 'wrong' })

  await rejects(client.get(testKey()), (error: unknown) => {
    ok(error instanceof AuthenticationError)
    strictEqual(error.code, 'PLT_MEMCACHED_AUTH_ERROR')
    return true
  })
})

test('all pipelined commands are rejected on authentication failure', async t => {
  const client = createClient(t, AUTH_SERVER, { username: USERNAME, password: 'wrong' })

  const results = await Promise.allSettled([
    client.set(testKey(), 'value'),
    client.get(testKey()),
    client.version()
  ])

  for (const result of results) {
    strictEqual(result.status, 'rejected')
    ok((result as PromiseRejectedResult).reason instanceof AuthenticationError)
  }
})

test('no credentials against an auth-enabled server rejects commands', async t => {
  const client = createClient(t, AUTH_SERVER)

  // The server answers "CLIENT_ERROR unauthenticated" to every command
  await rejects(client.get(testKey()), (error: unknown) => {
    ok(error instanceof ProtocolError)
    ok(error.message.includes('unauthenticated'))
    return true
  })
})

test('credentials can be embedded in the URL', async t => {
  const client = createClient(t, `memcached://${USERNAME}:${PASSWORD}@${AUTH_SERVER}`)
  const key = testKey()

  await client.set(key, 'from-url')
  deepStrictEqual(await client.get(key), Buffer.from('from-url'))
})

test('URL credentials are percent-decoded', async t => {
  // 'test%75ser' decodes to 'testuser', 'testpas%73' to 'testpass'
  const client = createClient(t, `memcached://test%75ser:testpas%73@${AUTH_SERVER}`)
  const key = testKey()

  await client.set(key, 'decoded')
  deepStrictEqual(await client.get(key), Buffer.from('decoded'))
})

test('explicit options take precedence over URL credentials', async t => {
  const client = createClient(t, `memcached://${USERNAME}:wrong@${AUTH_SERVER}`, {
    username: USERNAME,
    password: PASSWORD
  })
  const key = testKey()

  await client.set(key, 'value')
  deepStrictEqual(await client.get(key), Buffer.from('value'))
})

test('reconnections re-authenticate', async t => {
  const client = createClient(t, AUTH_SERVER, { username: USERNAME, password: PASSWORD })
  const key = testKey()

  await client.set(key, 'survivor')

  client.connection.socket!.destroy()
  await once(client.connection, 'connect')

  deepStrictEqual(await client.get(key), Buffer.from('survivor'))
})

test('username and password must be provided together', () => {
  throws(() => new Client(AUTH_SERVER, { username: USERNAME }), ValidationError)
  throws(() => new Client(AUTH_SERVER, { password: PASSWORD }), ValidationError)
  throws(() => new Client(`memcached://${USERNAME}@${AUTH_SERVER}`), ValidationError)
})

test('credentials with whitespace or control characters are rejected', () => {
  throws(() => new Client(AUTH_SERVER, { username: 'user name', password: PASSWORD }), ValidationError)
  throws(() => new Client(AUTH_SERVER, { username: USERNAME, password: 'pass\nword' }), ValidationError)
  throws(() => new Client(AUTH_SERVER, { username: USERNAME, password: 'pass\tword' }), ValidationError)
  throws(() => new Client(AUTH_SERVER, { username: '', password: PASSWORD }), ValidationError)
  throws(() => new Client(AUTH_SERVER, { username: 'usér', password: PASSWORD }), ValidationError)
})
