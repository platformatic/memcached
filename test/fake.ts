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
 * (mg, ms, md, mn, version) for multi-node routing tests, where several
 * servers are needed and which one received each command must be observable.
 */
export class FakeMemcached {
  store = new Map<string, Buffer>()
  noops = 0
  versions = 0
  port = 0

  #server: Server
  #sockets = new Set<Socket>()

  constructor () {
    this.#server = createServer(socket => {
      this.#sockets.add(socket)
      socket.on('close', () => this.#sockets.delete(socket))

      let buffer: Buffer = Buffer.alloc(0)
      socket.on('data', chunk => {
        buffer = this.#process(socket, Buffer.concat([buffer, chunk]))
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

  #process (socket: Socket, buffer: Buffer): Buffer {
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
        socket.write(`HD${mirror(tokens, 3)}${CRLF}`)
        buffer = buffer.subarray(end)
        continue
      }

      buffer = buffer.subarray(idx + 2)

      if (command === 'mg') {
        const value = this.store.get(tokens[1])

        if (value === undefined) {
          socket.write(`EN${mirror(tokens, 2)}${CRLF}`)
        } else if (tokens.includes('v')) {
          socket.write(`VA ${value.length}${mirror(tokens, 2)}${CRLF}`)
          socket.write(value)
          socket.write(CRLF)
        } else {
          socket.write(`HD${mirror(tokens, 2)}${CRLF}`)
        }
      } else if (command === 'md') {
        socket.write(`${this.store.delete(tokens[1]) ? 'HD' : 'NF'}${mirror(tokens, 2)}${CRLF}`)
      } else if (command === 'mn') {
        this.noops++
        socket.write(`MN${CRLF}`)
      } else if (command === 'version') {
        this.versions++
        socket.write(`VERSION 1.6.0-fake${CRLF}`)
      } else {
        socket.write(`ERROR${CRLF}`)
      }
    }
  }
}
