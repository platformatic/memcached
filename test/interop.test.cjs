'use strict'

const { strictEqual } = require('node:assert')
const { test } = require('node:test')

test('the package can be loaded with require()', () => {
  const { Client, MemcachedError, ConnectionError, ProtocolError, ValidationError } = require('../index.js')

  strictEqual(typeof Client, 'function')
  strictEqual(typeof MemcachedError, 'function')
  strictEqual(typeof ConnectionError, 'function')
  strictEqual(typeof ProtocolError, 'function')
  strictEqual(typeof ValidationError, 'function')
})
