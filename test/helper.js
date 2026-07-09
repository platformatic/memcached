import { randomUUID } from 'node:crypto'
import { connect } from 'node:net'
import { setTimeout as sleep } from 'node:timers/promises'
import { Client } from '../index.js'

export const SERVER = process.env.MEMCACHED_URL ?? 'localhost:11211'

// Waits for the Docker container port to accept connections
export async function waitForServer (timeout = 10000) {
  const start = Date.now()

  while (true) {
    try {
      await new Promise((resolve, reject) => {
        const socket = connect(11211, 'localhost')
        socket.once('connect', () => {
          socket.destroy()
          resolve()
        })
        socket.once('error', reject)
      })

      return
    } catch (error) {
      if (Date.now() - start > timeout) {
        throw new Error('memcached did not become reachable, is Docker running?', { cause: error })
      }

      await sleep(100)
    }
  }
}

export function createClient (t, url = SERVER, options = {}) {
  const client = new Client(url, options)
  t.after(() => client.close())
  return client
}

export function testKey () {
  return `test:${randomUUID()}`
}
