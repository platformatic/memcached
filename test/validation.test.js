import { throws } from 'node:assert'
import { test } from 'node:test'
import { Client, ValidationError } from '../index.js'

// Validation is synchronous and happens before any I/O, no server is needed

function createClient (t) {
  const client = new Client('localhost:11211', { reconnectDelay: 10 })
  t.after(() => client.close())
  return client
}

test('rejects invalid server addresses', () => {
  throws(() => new Client(''), ValidationError)
  throws(() => new Client(42), ValidationError)
  throws(() => new Client('memcached://'), ValidationError)
})

test('rejects invalid keys', t => {
  const client = createClient(t)

  throws(() => client.get(''), ValidationError)
  throws(() => client.get('with space'), ValidationError)
  throws(() => client.get('with\r\nnewline'), ValidationError)
  throws(() => client.get('with\ttab'), ValidationError)
  throws(() => client.get('a'.repeat(251)), ValidationError)
  throws(() => client.get('non-ascii-é'), ValidationError)
  throws(() => client.get(42), ValidationError)
  throws(() => client.set('bad key', 'value'), ValidationError)
  throws(() => client.delete('bad key'), ValidationError)
})

test('rejects invalid values', t => {
  const client = createClient(t)

  throws(() => client.set('key', 42), ValidationError)
  throws(() => client.set('key', { object: true }), ValidationError)
  throws(() => client.set('key', null), ValidationError)
})

test('rejects invalid TTLs', t => {
  const client = createClient(t)

  throws(() => client.set('key', 'value', { ttl: -1 }), ValidationError)
  throws(() => client.set('key', 'value', { ttl: 1.5 }), ValidationError)
  throws(() => client.set('key', 'value', { ttl: 'soon' }), ValidationError)
})

test('rejects invalid CAS tokens', t => {
  const client = createClient(t)

  throws(() => client.cas('key', 'value', 'not-a-number'), ValidationError)
  throws(() => client.cas('key', 'value', ''), ValidationError)
  throws(() => client.cas('key', 'value', undefined), ValidationError)
  throws(() => client.delete('key', { cas: 'abc' }), ValidationError)
})

test('rejects invalid deltas', t => {
  const client = createClient(t)

  throws(() => client.incr('key', 0), ValidationError)
  throws(() => client.incr('key', -5), ValidationError)
  throws(() => client.incr('key', 1.5), ValidationError)
  throws(() => client.decr('key', 'two'), ValidationError)
})
