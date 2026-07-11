import { deepStrictEqual, rejects, throws } from 'node:assert'
import { execSync } from 'node:child_process'
import { once } from 'node:events'
import { mkdirSync, readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { after, before, test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { connect as tlsConnect } from 'node:tls'
import { fileURLToPath } from 'node:url'
import { Client, ConnectionError, ValidationError } from '../src/index.ts'
import { createClient, testKey } from './helper.ts'

const TLS_PORT = 11213
const CONTAINER = 'plt-memcached-tls-test'

// Gitignored fixture directory for the generated self-signed certificate
const certsDir = join(dirname(fileURLToPath(import.meta.url)), 'fixtures', 'tls')

let ca: Buffer

// Waits until the TLS handshake completes, i.e. memcached is actually
// accepting connections behind the published Docker port
async function waitForTLSServer (timeout = 15000): Promise<void> {
  const start = Date.now()

  while (true) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = tlsConnect({ host: 'localhost', port: TLS_PORT, ca })
        socket.once('secureConnect', () => {
          socket.destroy()
          resolve()
        })
        socket.once('error', reject)
      })

      return
    } catch (error) {
      if (Date.now() - start > timeout) {
        throw new Error('TLS memcached did not become reachable, is Docker running?', { cause: error })
      }

      await sleep(100)
    }
  }
}

before(async () => {
  mkdirSync(certsDir, { recursive: true })

  // Fresh self-signed certificate for localhost on every run
  execSync(
    'openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 ' +
      '-keyout key.pem -out cert.pem -days 30 -nodes -subj "/CN=localhost" ' +
      '-addext "subjectAltName=DNS:localhost,IP:127.0.0.1"',
    { cwd: certsDir, stdio: 'ignore' }
  )
  // The key must be readable by the memcached user inside the container
  execSync('chmod 644 key.pem cert.pem', { cwd: certsDir })

  ca = readFileSync(join(certsDir, 'cert.pem'))

  execSync(`docker rm -f ${CONTAINER} > /dev/null 2>&1 || true`)
  execSync(
    `docker run -d --rm --name ${CONTAINER} -p ${TLS_PORT}:11211 -v ${certsDir}:/certs:ro ` +
      'memcached:alpine memcached -Z -o ssl_chain_cert=/certs/cert.pem -o ssl_key=/certs/key.pem',
    { stdio: 'ignore' }
  )

  await waitForTLSServer()
})

after(() => {
  execSync(`docker rm -f ${CONTAINER} > /dev/null 2>&1 || true`)
})

test('set/get round-trip over TLS', async t => {
  const client = createClient(t, `localhost:${TLS_PORT}`, { tls: { ca } })
  const key = testKey()

  await client.set(key, 'secret')

  deepStrictEqual(await client.get(key), Buffer.from('secret'))
})

test('servername can be set explicitly when connecting to an IP address', async t => {
  const client = createClient(t, `127.0.0.1:${TLS_PORT}`, { tls: { ca, servername: 'localhost' } })
  const key = testKey()

  await client.set(key, 'value')

  deepStrictEqual(await client.get(key), Buffer.from('value'))
})

test('the memcacheds:// scheme enables TLS and merges tls options', async t => {
  const client = createClient(t, `memcacheds://localhost:${TLS_PORT}`, { tls: { ca } })
  const key = testKey()

  await client.set(key, 'value')

  deepStrictEqual(await client.get(key), Buffer.from('value'))
})

test('the memcacheds:// scheme verifies certificates by default', async t => {
  // The certificate is self-signed and no ca is provided: the handshake
  // must fail, proving both that the scheme alone switches the transport to
  // TLS and that verification is on by default
  const client = createClient(t, `memcacheds://localhost:${TLS_PORT}`, {
    connectTimeout: 1000,
    reconnectDelay: 10,
    maxReconnectDelay: 20
  })

  await rejects(client.get(testKey()), ConnectionError)
})

test('the memcacheds:// scheme works with rejectUnauthorized disabled', async t => {
  const client = createClient(t, `memcacheds://localhost:${TLS_PORT}`, { tls: { rejectUnauthorized: false } })
  const key = testKey()

  await client.set(key, 'value')

  deepStrictEqual(await client.get(key), Buffer.from('value'))
})

test('plaintext connections to a TLS server fail with ConnectionError', async t => {
  const client = createClient(t, `localhost:${TLS_PORT}`, {
    connectTimeout: 1000,
    reconnectDelay: 10,
    maxReconnectDelay: 20
  })

  await rejects(client.get(testKey()), ConnectionError)
})

test('TLS connections to a plaintext server fail with ConnectionError', async t => {
  const client = createClient(t, 'localhost:11211', {
    tls: { ca },
    connectTimeout: 1000,
    reconnectDelay: 10,
    maxReconnectDelay: 20
  })

  await rejects(client.get(testKey()), ConnectionError)
})

test('the client reconnects automatically over TLS', async t => {
  const client = createClient(t, `localhost:${TLS_PORT}`, { tls: { ca } })
  const key = testKey()

  await client.set(key, 'survivor')

  client.connection.socket!.destroy()
  await once(client.connection, 'connect')

  deepStrictEqual(await client.get(key), Buffer.from('survivor'))
})

test('invalid tls options are rejected', () => {
  throws(() => new Client(`localhost:${TLS_PORT}`, { tls: 'yes' as never }), ValidationError)
})
