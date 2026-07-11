# Agent notes

Design, API, and testing docs live in README.md — read it first. Only non-obvious workflow
gotchas are listed here.

## Running tests

- Tests hit a real memcached on `localhost:11211`. `npm test` starts/stops a
  `memcached:alpine` Docker container via pretest/posttest, but the docker commands end in
  `|| true`: if Docker is unavailable, pretest fails **silently** and every test errors with
  `ConnectionError`/timeout. Connection failures in test output mean "check Docker", not a
  client bug.
- Running a single file (`node --test test/basic.test.ts`) skips the pretest hook — start a
  container yourself first: `docker run -d --rm --name memcached -p 11211:11211 memcached:alpine`.
  Don't name it `plt-memcached-test`, or the next `npm test` posttest will remove it.
- `test/auth.test.ts` needs a second, auth-enabled memcached on port 11214 (pretest starts
  it as `plt-memcached-auth-test`, mounting `test/fixtures/authfile`). Standalone:
  `docker run -d --rm --name memcached-auth -p 11214:11211 -v "$PWD/test/fixtures:/auth:ro" memcached:alpine memcached -Y /auth/authfile`.
  The authfile must stay world-readable (644): the container runs as the `memcache` user
  and memcached exits at startup if it cannot read the file, so the container silently
  disappears and auth tests fail with connection errors.
- `test/interop.test.cjs` requires the **built** package (`dist/`), not `src/`; run
  `npm run build` before running it standalone. The other tests import `src/` directly and
  need no build.
- `MEMCACHED_URL` overrides the server tests connect to (see `test/helper.js`).
- `test/tls.test.ts` manages its own TLS memcached container (`plt-memcached-tls-test`, host
  port 11213): it generates a self-signed certificate into the gitignored `test/fixtures/tls`
  directory and starts/removes the container itself, so it also works standalone — but it
  needs Docker and `openssl` on the PATH. The posttest hook removes the container as a
  safety net if the test crashes.
