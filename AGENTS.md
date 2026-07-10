# Agent notes

Design, API, and testing docs live in README.md — read it first. Only non-obvious workflow
gotchas are listed here.

## Running tests

- Tests hit a real memcached on `localhost:11211`. `npm test` starts/stops a
  `memcached:alpine` Docker container via pretest/posttest, but both hooks end in `|| true`:
  if Docker is unavailable, pretest fails **silently** and every test errors with
  `ConnectionError`/timeout. Connection failures in test output mean "check Docker", not a
  client bug.
- Running a single file (`node --test test/basic.test.js`) skips the pretest hook — start a
  container yourself first: `docker run -d --rm --name memcached -p 11211:11211 memcached:alpine`.
  Don't name it `plt-memcached-test`, or the next `npm test` posttest will remove it.
- `MEMCACHED_URL` overrides the server tests connect to (see `test/helper.js`).
