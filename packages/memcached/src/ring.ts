import { createHash } from 'node:crypto'

// Standard ketama: 40 md5 digests per node, each sliced into 4 unsigned
// 32-bit points, for 160 points per node.
const HASHES_PER_NODE = 40
const POINTS_PER_HASH = 4

export interface RingNode {
  host: string
  port: number
}

/**
 * Ketama-style consistent hash ring. Each node is placed on a 32-bit ring at
 * 160 points; a key belongs to the node owning the first point at or after
 * the key's hash, wrapping around. Adding or removing a node only remaps the
 * keys owned by that node (roughly 1/N of the keyspace) — every other key
 * keeps its owner.
 */
export class HashRing {
  #points: Uint32Array
  #owners: Uint32Array

  constructor (nodes: RingNode[]) {
    const total = nodes.length * HASHES_PER_NODE * POINTS_PER_HASH
    const points = new Uint32Array(total)
    const owners = new Uint32Array(total)
    let index = 0

    for (let node = 0; node < nodes.length; node++) {
      const { host, port } = nodes[node]

      for (let i = 0; i < HASHES_PER_NODE; i++) {
        const digest = createHash('md5').update(`${host}:${port}-${i}`).digest()

        for (let slice = 0; slice < POINTS_PER_HASH; slice++) {
          points[index] = digest.readUInt32LE(slice * 4)
          owners[index] = node
          index++
        }
      }
    }

    // Sort the two parallel arrays together by point; ties break by node
    // index so ring construction is deterministic across processes
    const order = Array.from(points.keys()).sort((a, b) => (points[a] - points[b]) || (owners[a] - owners[b]))
    this.#points = new Uint32Array(total)
    this.#owners = new Uint32Array(total)

    for (let i = 0; i < total; i++) {
      this.#points[i] = points[order[i]]
      this.#owners[i] = owners[order[i]]
    }
  }

  /**
   * Returns the index (into the constructor's node array) of the node that
   * owns the key.
   */
  lookup (key: string): number {
    const hash = createHash('md5').update(key).digest().readUInt32LE(0)
    const points = this.#points

    // Binary search for the first point >= hash, wrapping to the start
    let low = 0
    let high = points.length

    while (low < high) {
      const mid = (low + high) >>> 1

      if (points[mid] < hash) {
        low = mid + 1
      } else {
        high = mid
      }
    }

    return this.#owners[low === points.length ? 0 : low]
  }
}
