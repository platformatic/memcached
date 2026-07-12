#!/usr/bin/env -S node

import { execSync } from 'node:child_process'
import { appendFile, readFile, writeFile } from 'node:fs/promises'

// The workspace packages are version-synced: every release bumps them all
const PACKAGES = ['packages/memcached/package.json', 'packages/memcached-otel/package.json']

const VERSION_EXPRESSION = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/

type UserInfo = [string, string]

function getUserInfo (): UserInfo {
  const username = process.argv[3] ?? process.env.GITHUB_ACTOR
  const defaultUser = 'mcollina'

  const users: Record<string, UserInfo> = {
    mcollina: ['Matteo Collina', 'hello@matteocollina.com'],
    ShogunPanda: ['Paolo Insogna', 'paolo@cowtech.it']
  }

  let userInfo = users[username!]

  if (!userInfo) {
    userInfo = users[defaultUser]
  }

  return userInfo
}

async function getVersion (): Promise<string> {
  const requested = process.argv[2]?.replace(/^v/, '')

  if (!requested) {
    throw new Error('Usage: node scripts/bump-version.ts <version|major|minor|patch> [actor]')
  }

  if (['major', 'minor', 'patch'].includes(requested)) {
    const packageJson = JSON.parse(await readFile(PACKAGES[0], 'utf8'))
    const [major, minor, patch] = packageJson.version.split(/[.-]/).slice(0, 3).map(Number)

    switch (requested) {
      case 'major':
        return `${major + 1}.0.0`
      case 'minor':
        return `${major}.${minor + 1}.0`
      default:
        return `${major}.${minor}.${patch + 1}`
    }
  }

  if (!VERSION_EXPRESSION.test(requested)) {
    throw new Error(`Invalid version: ${requested}`)
  }

  return requested
}

async function updatePackageJson (path: string, version: string): Promise<void> {
  const packageJson = JSON.parse(await readFile(path, 'utf8'))
  packageJson.version = version
  await writeFile(path, JSON.stringify(packageJson, null, 2) + '\n')
}

const userInfo = getUserInfo()
const version = await getVersion()

for (const path of PACKAGES) {
  await updatePackageJson(path, version)
}

if (process.env.GITHUB_ACTIONS === 'true') {
  execSync(`git config --global user.name "${userInfo[0]}"`)
  execSync(`git config --global user.email "${userInfo[1]}"`)
}

// Expose the resolved version so the workflow can tag with it even when the
// input was a major/minor/patch increment
if (process.env.GITHUB_OUTPUT) {
  await appendFile(process.env.GITHUB_OUTPUT, `version=${version}\n`)
}

execSync(`git commit -a -m "chore: Bumped v${version}." -m "Signed-off-by: ${userInfo[0]} <${userInfo[1]}>"`)
