import { randomUUID } from 'node:crypto'
import { connect } from 'node:net'
import type { TestContext } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { Client, type ClientOptions, type ServerAddress } from '../src/index.ts'

export const SERVER = process.env.MEMCACHED_URL ?? 'localhost:11211'

// Waits for the Docker container port to accept connections
export async function waitForServer (timeout = 10000): Promise<void> {
  const start = Date.now()

  while (true) {
    try {
      await new Promise<void>((resolve, reject) => {
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

export function createClient (t: TestContext, url: string | ServerAddress = SERVER, options: ClientOptions = {}): Client {
  const client = new Client(url, options)
  t.after(() => client.close())
  return client
}

export function testKey (): string {
  return `test:${randomUUID()}`
}
