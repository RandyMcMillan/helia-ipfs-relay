#!/usr/bin/env node
/* ═══════════════════════════════════════════════════════════════════════
   git-remote-gnostr — git remote helper: clone/fetch git repos announced
   on the "gnostr" gossipsub topic, fetching objects over IPFS/libp2p.

   HOW GIT USES THIS
     Any `git clone/fetch/pull` against a `gnostr://` (or `gnostr:`) remote
     makes git spawn `git-remote-gnostr <remote-name> <url>` on PATH and
     speak the git remote-helper protocol on our stdio. We advertise a
     single capability, "connect": after materializing a local bare repo,
     we relay git-upload-pack's pkt-line stream bidirectionally. Push
     (git-receive-pack) is answered with a clean error — gnostr remotes
     are fetch-only.

   URL FORMS
     gnostr://<repo-name>     listen on the gnostr topic for git ads whose
                              "name" matches; the newest ad wins (bundle
                              ads preferred over http-url ads)
     gnostr://cid/<bundle-cid>  skip discovery entirely; bitswap-fetch this
                              UnixFS/raw CID from the IPFS network

   THE AD FORMAT (published by the web app, the headless backend, and the
   companion gnostr-publish.js, which can announce ANY git repo):
     { type: "git", name, at, bundle?, dir?, url?, addrs? }
       bundle — CID of a `git bundle create` file (fetched over bitswap)
       url    — smart-http git endpoint (materialized via git clone --bare)
       addrs  — optional publisher listen multiaddrs; we dial them so the
                bundle can be pulled straight from the source

   INSTALL
     cp git-remote-gnostr.js ~/.local/bin/git-remote-gnostr
     chmod +x ~/.local/bin/git-remote-gnostr
     (Node >= 22, deps from the repo's package.json: libp2p stack + helia)
     For submodules also allow the transport once — git refuses unknown
     schemes in .gitmodules otherwise:
       git config --global protocol.gnostr.allow always

   SUBMODULES (verified end-to-end)
     git submodule add gnostr://<repo-name> vendor/relay
     git clone + git submodule update --init both resolve the name over the
     gnostr topic and fetch like a plain clone.

   ENV
     GNOSTR_TOPIC              pubsub topic carrying git ads  (default "gnostr")
     GNOSTR_DISCOVERY_TIMEOUT  max seconds to listen in name mode (default 30)
     GNOSTR_BOOTSTRAP          comma-separated extra multiaddrs to dial at boot
                             (e.g. a known peer: /ip4/…/tcp/4001/p2p/<peerId>)

   OPERATIONAL NOTES
     - Fetches are SNAPSHOT-based: every clone/fetch re-resolves the ad and
       re-fetches the whole bundle. Fine for CI/submodules; for incremental
       day-to-day fetching prefer a url ad's http endpoint as the remote.
     - Name ads are UNAUTHENTICATED: anyone on the topic can announce the
       same name, and the newest `at` wins. Use gnostr://<name> inside your
       own swarm; for adversarial settings pin gnostr://cid/<bundle-cid> —
       content-addressed, nothing to shadow.
   ═══════════════════════════════════════════════════════════════════════ */

import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { noise } from '@libp2p/noise'
import { yamux } from '@libp2p/yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { gossipsub } from '@libp2p/gossipsub'
import { bootstrap } from '@libp2p/bootstrap'
import { kadDHT } from '@libp2p/kad-dht'
import { circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { multiaddr } from '@multiformats/multiaddr'
import { createHelia } from 'helia'
import { unixfs } from '@helia/unixfs'
import { spawn } from 'node:child_process'
import { writeFileSync, mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

function fail (msg) {
  process.stderr.write('git-remote-gnostr: ' + msg + '\n')
  process.exit(1)
}

/* ── URL parsing ────────────────────────────────────────────────────── */
const url = process.argv[3]
if (!url || !url.startsWith('gnostr:')) fail('usage: git-remote-gnostr <name> <gnostr-url>')
const rest = url.slice('gnostr:'.length).replace(/\/+$/, '')
let wantName = null
let wantCid = null
if (rest.startsWith('//')) {
  const body = rest.slice(2)
  if (body.startsWith('cid/')) wantCid = body.slice(4) // git only routes scheme:// URLs to helpers, so the direct form lives under the host part
  else wantName = body || null
} else fail('unknown gnostr url form: ' + url + ' (want gnostr://<name> or gnostr://cid/<cid>)')

const TOPIC = process.env.GNOSTR_TOPIC || 'gnostr'
const DISCOVERY_MS = Number(process.env.GNOSTR_DISCOVERY_TIMEOUT || 30) * 1000
const KUBO = [
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPRGAiQDHQE1jY7kNBRLuGg4QgarhVZGxy',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR1ZGCcD4HVm3eoLzUKeoYEeeWXzBdDz4',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt',
  '/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ'
]
const BOOTSTRAP = [
  ...KUBO,
  ...(process.env.GNOSTR_BOOTSTRAP ? process.env.GNOSTR_BOOTSTRAP.split(',').map(s => s.trim()).filter(Boolean) : [])
]

process.stderr.write(`git-remote-gnostr: ${wantCid ? 'cid ' + wantCid : 'name "' + wantName + '"'} via topic "${TOPIC}"\n`)

/* ── libp2p + helia (same pinned stack as the web app / server) ───────
   NOTE: helia must own the libp2p node — pass OPTIONS, not an instance,
   and start helia explicitly, otherwise the bitswap block broker is
   never registered and network fetches silently fall back to gateways. */
const helia = createHelia({
  libp2p: {
    addresses: { listen: [] },
    transports: [tcp(), webSockets(), circuitRelayTransport()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery: [bootstrap({ list: BOOTSTRAP })],
    services: {
      identify: identify(),
      ping: ping(),
      dht: kadDHT({ clientMode: true }),
      pubsub: gossipsub()
    }
  }
})
await helia.start()
const node = helia.libp2p
const ufs = unixfs(helia)
const stop = async () => { try { await helia.stop() } catch {} }

/* bitswap wants must not race ahead of the bootstrap dials — wait until we
   have at least one connection before subscribing or fetching */
async function waitForPeers (min = 1, timeoutMs = 20000) {
  if (node.getPeers().length >= min) return
  process.stderr.write('git-remote-gnostr: dialing bootstrap peers…\n')
  await new Promise((resolve, reject) => {
    const cleanup = () => { clearTimeout(t); node.removeEventListener('peer:connect', onConnect) }
    const t = setTimeout(() => { cleanup(); reject(new Error('timed out waiting for peers')) }, timeoutMs)
    const onConnect = () => { if (node.getPeers().length >= min) { cleanup(); resolve() } }
    node.addEventListener('peer:connect', onConnect)
  })
}

function run (cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['ignore', 'ignore', 'inherit'] })
    p.on('error', reject)
    p.on('exit', code => code === 0 ? resolve() : reject(new Error(cmd + ' exited ' + code)))
  })
}

/* ── discovery: name → newest matching git ad on the gnostr topic ───── */
async function discover (name) {
  const ads = new Map()
  let finish = null
  let grace = null
  const done = new Promise(r => { finish = r })
  node.services.pubsub.subscribe(TOPIC)
  node.services.pubsub.addEventListener('message', (evt) => {
    if (evt.detail.topic !== TOPIC) return
    try {
      const ad = JSON.parse(new TextDecoder().decode(evt.detail.data))
      if (ad && ad.type === 'git' && ad.name === name && (ad.bundle || ad.url)) {
        ads.set(ad.bundle || ad.url, ad)
        if (!grace) grace = setTimeout(finish, 1500) // first match + grace, then go
      }
    } catch {}
  })
  process.stderr.write(`git-remote-gnostr: listening for "${name}" ads (${DISCOVERY_MS / 1000}s max)…\n`)
  const cap = setTimeout(finish, DISCOVERY_MS)
  await done
  clearTimeout(cap); if (grace) clearTimeout(grace)
  const list = [...ads.values()]
  if (!list.length) { await stop(); fail(`no git ads for "${name}" on topic "${TOPIC}"`) }
  list.sort((a, b) => (b.at || 0) - (a.at || 0))
  const best = list.find(a => a.bundle) || list[0]
  process.stderr.write(`git-remote-gnostr: ${list.length} ad(s), using ${best.bundle ? 'bundle ' + best.bundle : 'url ' + best.url}\n`)
  return best
}

/* each cat() re-broadcasts the want to every currently-connected peer, so a
   fresh attempt picks up blocks held by peers that dialed in late */
async function catWithRetry (cidStr, tries = 5) {
  for (let i = 1; ; i++) {
    try {
      const chunks = []
      for await (const c of ufs.cat(cidStr)) chunks.push(c)
      return Buffer.concat(chunks)
    } catch (err) {
      if (i >= tries) throw err
      process.stderr.write(`git-remote-gnostr: fetch attempt ${i} failed (${node.getPeers().length} peers connected), retrying…\n`)
      await new Promise(r => setTimeout(r, 3000))
    }
  }
}

/* ── materialize: ad → local bare repo ─────────────────────────────── */
async function materialize (ad) {
  const dir = mkdtempSync(join(tmpdir(), 'git-remote-gnostr-'))
  if (ad.bundle) {
    process.stderr.write('git-remote-gnostr: fetching bundle over ipfs/bitswap…\n')
    const bundlePath = join(dir, 'repo.bundle')
    writeFileSync(bundlePath, await catWithRetry(ad.bundle))
    await run('git', ['clone', '-q', '--bare', bundlePath, join(dir, 'repo.git')])
  } else if (ad.url) {
    process.stderr.write('git-remote-gnostr: fetching ' + ad.url + ' (smart http)…\n')
    await run('git', ['clone', '-q', '--bare', ad.url, join(dir, 'repo.git')])
  } else {
    await stop(); fail('ad has neither bundle nor url')
  }
  return join(dir, 'repo.git')
}

let repoDir
await waitForPeers().catch(async err => { await stop(); fail(err.message) })
if (wantCid) repoDir = await materialize({ bundle: wantCid })
else {
  const ad = await discover(wantName)
  if (Array.isArray(ad.addrs) && ad.addrs.length) {
    // the ad tells us where the publisher listens — dial it so bitswap
    // can pull the bundle straight from the source (no manual bootstrap)
    process.stderr.write('git-remote-gnostr: dialing ' + ad.addrs.length + ' ad-carried addrs…\n')
    await Promise.allSettled(ad.addrs.map(a => node.dial(multiaddr(a))))
    await new Promise(r => setTimeout(r, 1500))
  }
  repoDir = await materialize(ad)
}
await stop() // git's child needs no libp2p; release the swarm before relaying

/* ── remote-helper protocol: capabilities → connect → raw relay ────── */
process.stderr.write('git-remote-gnostr: serving ' + repoDir + '\n')
let chunks = []
process.stdin.on('data', (d) => {
  chunks.push(d)
  let buf = Buffer.concat(chunks)
  let i
  while ((i = buf.indexOf(0x0a)) !== -1) {
    const line = buf.subarray(0, i).toString('utf8')
    chunks = [buf.subarray(i + 1)]
    buf = Buffer.concat(chunks)
    if (line === 'capabilities') {
      process.stdout.write('connect\n\n')
    } else if (line.startsWith('connect ')) {
      const service = line.slice('connect '.length).trim()
      if (service !== 'git-upload-pack') {
        process.stdout.write('error gnostr remotes are fetch-only (' + service + ' unsupported)\n')
        setTimeout(() => process.exit(1), 500)
        return
      }
      const child = spawn(service, [repoDir])
      child.stderr.pipe(process.stderr)
      process.stdin.removeAllListeners('data')
      process.stdout.write('\n') // connect ack: empty line = proceed (git falls back on 'fallback')
      const rest = Buffer.concat(chunks)
      if (rest.length) child.stdin.write(rest) // leftover pkt-line bytes
      process.stdin.pipe(child.stdin)
      child.stdout.pipe(process.stdout)
      child.on('close', (code) => { process.stdout.write('', () => process.exit(code ?? 0)) }) // drain our stdout buffer into git before exiting, else the pack tail is lost
      return
    } else if (line.startsWith('option ')) {
      process.stdout.write('unsupported\n')
    } else if (line === '') {
      // protocol spacer — ignore
    } else {
      process.stdout.write('error unknown command: ' + line + '\n')
      process.exit(1)
    }
  }
})
