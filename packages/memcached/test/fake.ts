import { createServer, type AddressInfo, type Server, type Socket } from 'node:net'

const CRLF = '\r\n'

// Mirrors the O (opaque) flag back like memcached does. Flags start after the
// key (and, for ms, the size), so scanning begins past the fixed tokens.
function mirror (tokens: string[], from: number): string {
  for (let i = from; i < tokens.length; i++) {
    if (tokens[i].charCodeAt(0) === 79) {
      return ` ${tokens[i]}`
    }
  }

  return ''
}

/**
 * A minimal in-process memcached speaking just enough of the meta protocol
 * (mg, ms, md, mn, version, stats) for multi-node routing tests, where several
 * servers are needed and which one received each command must be observable.
 */
export class FakeMemcached {
  store = new Map<string, Buffer>()
  // Keys whose mg responses are delayed by the given milliseconds, to
  // simulate slow value transfers in pooling tests
  delays = new Map<string, number>()
  noops = 0
  versions = 0
  // STAT lines returned for a plain "stats" command; settable per test
  stats = new Map<string, string>([['version', '1.6.0-fake'], ['evictions', '0']])
  // STAT lines returned for "stats <subcommand>"; unknown subcommands get ERROR
  subcommandStats = new Map<string, Map<string, string>>()
  // Subcommand of each stats command received, null for plain "stats"
  statsQueries: Array<string | null> = []
  // Total sockets ever accepted, to observe pool sizes
  connections = 0
  port = 0

  #server: Server
  #sockets = new Set<Socket>()

  constructor () {
    this.#server = createServer(socket => {
      this.connections++
      this.#sockets.add(socket)
      socket.on('close', () => this.#sockets.delete(socket))

      let buffer: Buffer = Buffer.alloc(0)
      let queue: Promise<void> = Promise.resolve()

      // Serialized writer: responses leave in command order even when some
      // are artificially delayed, matching real memcached semantics
      const reply = (delay: number, chunks: Array<string | Buffer>) => {
        queue = queue.then(async () => {
          if (delay > 0) {
            await new Promise(resolve => setTimeout(resolve, delay))
          }

          if (!socket.destroyed) {
            for (const chunk of chunks) {
              socket.write(chunk)
            }
          }
        })
      }

      socket.on('data', chunk => {
        buffer = this.#process(reply, Buffer.concat([buffer, chunk]))
      })
    })
  }

  async listen (): Promise<void> {
    await new Promise<void>(resolve => this.#server.listen(0, '127.0.0.1', resolve))
    this.port = (this.#server.address() as AddressInfo).port
  }

  get address (): string {
    return `127.0.0.1:${this.port}`
  }

  // Stops listening and drops open connections, simulating a node going down
  async close (): Promise<void> {
    for (const socket of this.#sockets) {
      socket.destroy()
    }

    await new Promise<void>(resolve => {
      this.#server.close(() => resolve())
    })
  }

  #process (reply: (delay: number, chunks: Array<string | Buffer>) => void, buffer: Buffer): Buffer {
    while (true) {
      const idx = buffer.indexOf(CRLF)

      if (idx === -1) {
        return buffer
      }

      const line = buffer.toString('latin1', 0, idx)
      const tokens = line.split(' ')
      const command = tokens[0]

      if (command === 'ms') {
        const size = Number(tokens[2])
        const end = idx + 2 + size + 2

        if (buffer.length < end) {
          // Wait for the complete data block
          return buffer
        }

        this.store.set(tokens[1], Buffer.from(buffer.subarray(idx + 2, idx + 2 + size)))
        reply(0, [`HD${mirror(tokens, 3)}${CRLF}`])
        buffer = buffer.subarray(end)
        continue
      }

      buffer = buffer.subarray(idx + 2)

      if (command === 'mg') {
        const value = this.store.get(tokens[1])
        const delay = this.delays.get(tokens[1]) ?? 0

        if (value === undefined) {
          reply(delay, [`EN${mirror(tokens, 2)}${CRLF}`])
        } else if (tokens.includes('v')) {
          reply(delay, [`VA ${value.length}${mirror(tokens, 2)}${CRLF}`, value, CRLF])
        } else {
          reply(delay, [`HD${mirror(tokens, 2)}${CRLF}`])
        }
      } else if (command === 'md') {
        reply(0, [`${this.store.delete(tokens[1]) ? 'HD' : 'NF'}${mirror(tokens, 2)}${CRLF}`])
      } else if (command === 'mn') {
        this.noops++
        reply(0, [`MN${CRLF}`])
      } else if (command === 'version') {
        this.versions++
        reply(0, [`VERSION 1.6.0-fake${CRLF}`])
      } else if (command === 'stats') {
        const subcommand = tokens.length > 1 ? tokens[1] : null
        this.statsQueries.push(subcommand)
        const stats = subcommand === null ? this.stats : this.subcommandStats.get(subcommand)

        if (stats === undefined) {
          reply(0, [`ERROR${CRLF}`])
        } else {
          const chunks: string[] = []

          for (const [name, value] of stats) {
            chunks.push(`STAT ${name} ${value}${CRLF}`)
          }

          chunks.push(`END${CRLF}`)
          reply(0, chunks)
        }
      } else {
        reply(0, [`ERROR${CRLF}`])
      }
    }
  }
}
