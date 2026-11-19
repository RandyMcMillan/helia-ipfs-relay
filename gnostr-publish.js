#!/usr/bin/env node
/* ═══════════════════════════════════════════════════════════════════════
   gnostr-publish — announce ANY git repo on the "gnostr" gossipsub topic so
   peers running git-remote-gnostr can `git clone gnostr://<name>` it (or
   add it as a submodule).

   USAGE
     gnostr-publish [repo-dir]        (default: current directory)
     KEEP IT RUNNING — besides publishing the ad it is also the bitswap
     seeder: the bundle lives in its in-memory blockstore and is served to
     whoever clones. Stop it and the bundle is only fetchable if someone
     else pinned the CID.

   WHAT IT DOES, EVERY GNOSTR_AD_INTERVAL (default 60 s)
     1. git -C <repo> bundle create repo.bundle --all
     2. adds the bundle bytes to its helia blockstore (raw CID)
     3. publishes  { type:'git', name, at:<epoch ms>, bundle:<CID> }
        on the gnostr topic

   CONSUMERS
     git-remote-gnostr (same zip): `git clone gnostr://<name>` listens for
     the newest matching ad and bitswap-fetches the bundle; the printed
     `gnostr://cid/<CID>` form skips discovery entirely.

   ENV
     GNOSTR_NAME          repo name advertised (default: repo dir basename)
     GNOSTR_TOPIC         pubsub topic carrying git ads  (default "gnostr")
     GNOSTR_AD_INTERVAL   seconds between re-publishes    (default 60)
     GNOSTR_BOOTSTRAP     comma-separated extra multiaddrs to dial at boot
     GNOSTR_LISTEN        comma-separated listen multiaddrs (e.g.
                          /ip4/0.0.0.0/tcp/4001 on a public host). When set,
                          the addrs are embedded in every ad so cloners can
                          dial the publisher straight from the announcement;
                          they are also printed at boot. Without it the
                          publisher is a pure client and relies on both
                          sides sharing relays (or the cid form with
                          GNOSTR_BOOTSTRAP passed to the cloner).

   Requires Node >= 22 and the package.json deps (same libp2p stack as
   git-remote-gnostr.js). Like the helper, helia must own the libp2p node:
   pass options + explicit start, or the bitswap broker never registers.
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
import { createHelia } from 'helia'
import { unixfs } from '@helia/unixfs'
import { spawn } from 'node:child_process'
import { mkdtempSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, join, resolve } from 'node:path'

const repoPath = resolve(process.argv[2] || '.')
const name = process.env.GNOSTR_NAME || basename(repoPath)
const TOPIC = process.env.GNOSTR_TOPIC || 'gnostr'
const INTERVAL = Number(process.env.GNOSTR_AD_INTERVAL || 60) * 1000
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
const LISTEN = process.env.GNOSTR_LISTEN ? process.env.GNOSTR_LISTEN.split(',').map(s => s.trim()).filter(Boolean) : []

function run (cmd, args) {
  return new Promise((resolvePromise, reject) => {
    const p = spawn(cmd, args, { stdio: ['ignore', 'ignore', 'inherit'] })
    p.on('error', reject)
    p.on('exit', code => code === 0 ? resolvePromise() : reject(new Error(cmd + ' exited ' + code)))
  })
}

const helia = createHelia({
  libp2p: {
    addresses: { listen: LISTEN },
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

process.stderr.write(`gnostr-publish: repo ${repoPath} as "${name}" — peer ${node.peerId.toString()}\n`)
for (const a of node.getMultiaddrs()) process.stderr.write(`gnostr-publish: listening ${a.toString()}\n`)

/* don't let the first publish race the bootstrap dials; a timeout is not
   fatal here because re-publishes land once peers connect */
async function waitForPeers (min = 1, timeoutMs = 30000) {
  if (node.getPeers().length >= min) return
  process.stderr.write('gnostr-publish: dialing bootstrap peers…\n')
  await new Promise((resolvePromise) => {
    const t = setTimeout(() => { cleanup(); process.stderr.write('gnostr-publish: no peers yet — publishing anyway\n') }, timeoutMs)
    const cleanup = () => { clearTimeout(t); node.removeEventListener('peer:connect', onConnect) }
    const onConnect = () => { if (node.getPeers().length >= min) { cleanup(); resolvePromise() } }
    node.addEventListener('peer:connect', onConnect)
  })
}

async function publish () {
  const dir = mkdtempSync(join(tmpdir(), 'gnostr-publish-'))
  const bundlePath = join(dir, 'repo.bundle')
  await run('git', ['-C', repoPath, 'bundle', 'create', bundlePath, '--all'])
  const cid = await ufs.addBytes(readFileSync(bundlePath))
  const ad = { type: 'git', name, at: Date.now(), bundle: cid.toString() }
  if (LISTEN.length) ad.addrs = node.getMultiaddrs().map(a => a.toString())
  await node.services.pubsub.publish(TOPIC, new TextEncoder().encode(JSON.stringify(ad))).catch(() => {})
  process.stderr.write(`gnostr-publish: ${name} → ${cid.toString()}  (topic "${TOPIC}", ${node.getPeers().length} peers) — clone with: git clone gnostr://cid/${cid}\n`)
}

/* the interval must never crash the process (Node exits on unhandled
   rejections) and never overlap runs (a slow bundle would stack up) */
let publishing = false
async function tick () {
  if (publishing) return
  publishing = true
  try { await publish() } catch (err) { process.stderr.write('gnostr-publish: publish failed: ' + (err?.message || err) + '\n') }
  finally { publishing = false }
}

await waitForPeers()
await tick()
setInterval(tick, INTERVAL)
