/* Helia IPFS Relay — headless shared backend.
 *
 * One process, two jobs:
 *   1. runs a headless libp2p node (TCP + WebSocket) with a circuit relay v2
 *      server — the non-browser counterpart of the in-page Helia node
 *   2. serves this folder over HTTP and exposes GET /api/relay so the page
 *      can display the headless node in the "Headless relay" card
 *
 * Run:  npm install && node server.js        (or: PORT=9000 node server.js)
 *
 * Notes:
 *   - The blockstore-free headless node relays traffic but does not store
 *     content. Reservations it grants are served to any peer that dials it.
 *   - For the public swarm to reach you, tcp/4001 (+ udp/4001 if you add QUIC)
 *     must be port-forwarded. WebSocket /ws/0.0.0.0/tcp/4002 is what browser
 *     nodes dial; advertise the machine's public IP for that listener if you
 *     want inbound browser connections.
 *   - DHT runs in client mode so a NAT'd node doesn't publish undialable
 *     records; set clientMode: false on a public host to join the DHT
 *     as a server.
 *
 * Git endpoints (both optional, enabled when repo.bundle is present):
 *   - GET /git/repo    smart-http git endpoint (git http-backend) — clone
 *                      with `git clone http://<host>:<port>/git/repo`
 *   - /cors-proxy      @isomorphic-git/cors-proxy for in-browser clones of
 *                      external https remotes. The handler sets a git/
 *                      User-Agent server-side: browsers cannot send one, and
 *                      github.com's smart-http endpoint answers the clone
 *                      handshake only for git-looking requests. Verified:
 *                      in-page clone of github.com/RandyMcMillan/ipfs-gh-pages
 *                      (171 files, HEAD ab9ed44).
 *   The node also advertises { type:'git', name:'helia-ipfs-relay-headless',
 *   at, url } on the gnostr gossipsub topic once a minute, so peers running
 *   the bundled git-remote-gnostr helper can `git clone
 *   gnostr://helia-ipfs-relay-headless`. PUBLIC_URL overrides the url origin.
 *   See the GNOSTR GIT ADS contract in app.js and the helper's own header.
 */
import http from 'node:http'
import { access, readFile } from 'node:fs/promises'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { spawn } from 'node:child_process'
import { extname, join, normalize, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import corsProxy from '@isomorphic-git/cors-proxy'
import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { webSockets } from '@libp2p/websockets'
import { noise } from '@libp2p/noise'
import { yamux } from '@libp2p/yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { bootstrap } from '@libp2p/bootstrap'
import { kadDHT } from '@libp2p/kad-dht'
import { circuitRelayServer, circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { gossipsub } from '@libp2p/gossipsub'
import { autoNAT } from '@libp2p/autonat'
import { dcutr } from '@libp2p/dcutr'

/* ---- config knobs ----
 * PORT            HTTP port for the page + API        (default 8080)
 * listen          libp2p listeners; ws listener (4002) is what browser
 *                 nodes dial. Add '/ip4/0.0.0.0/udp/4001/quic-v1' + the
 *                 quic transport for QUIC.
 * reservations    relay v2 limits; maxReservations caps how many peers may
 *                 hold /p2p-circuit slots on this node at once.
 * dht clientMode  true = DHT client (safe behind NAT). Set false on a
 *                 publicly reachable host to also serve DHT records.
 * Production: serve behind a TLS terminator and announce the /wss address
 * (/dns4/<host>/tcp/443/wss/p2p/<peerId>) or browsers on HTTPS pages
 * cannot dial this node (mixed content).
 */
const ROOT = fileURLToPath(new URL('.', import.meta.url))
const PORT = Number(process.env.PORT || 8080)
const startedAt = Date.now()

const BOOTSTRAP_LIST = [
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPRQ4HEBC6VrPNLPWezKBcjDiJvRft2E6q',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCRHPZsaM3KFN4j5RMteQdaLvMq34dxrb5',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt',
  '/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ'
]

const node = await createLibp2p({
  addresses: {
    listen: [
      '/ip4/0.0.0.0/tcp/4001',
      '/ip4/0.0.0.0/tcp/4002/ws'
    ]
  },
  transports: [tcp(), webSockets(), circuitRelayTransport()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  peerDiscovery: [bootstrap({ list: BOOTSTRAP_LIST })],
  services: {
    identify: identify(),
    ping: ping(),
    dht: kadDHT({ clientMode: true }),
    autonat: autoNAT(),
    dcutr: dcutr(),
    relay: circuitRelayServer({
      reservations: { maxReservations: 128, applyDefaultLimit: false }
    }),
    pubsub: gossipsub({ emitSelf: false })
  }
})

/* gossipsub on the "gnostr" topic — same topic the browser node subscribes
 * to, so browser peers meshed with this node exchange messages with it. */
const PUBSUB_TOPIC = 'gnostr'
node.services.pubsub.subscribe(PUBSUB_TOPIC)
node.services.pubsub.addEventListener('message', (evt) => {
  if (evt.detail.topic !== PUBSUB_TOPIC) return
  console.log('pubsub[' + PUBSUB_TOPIC + '] ' + evt.detail.from.toString() + ': ' +
    new TextDecoder().decode(evt.detail.data))
})

/* ---------------- git smart HTTP ----------------
 * Serves the site's own source repo at /git/repo so both real git and the
 * in-page isomorphic-git can clone it:
 *   git clone http://<host>:PORT/git/repo
 * Implemented by extracting repo.bundle on boot (needs git on PATH) and
 * proxying /git/* to `git http-backend` (CGI). Without git, the /git
 * route is absent and the page's clone button reports it.
 * ---------------- CORS proxy ----------------
 * Mounts @isomorphic-git/cors-proxy at /cors-proxy so the in-page cloner
 * can also fetch from external git hosts that lack CORS headers.
 * ---------------- stats API ----------------
 * GET /api/relay -> JSON consumed by pollHeadless() in app.js:
 *   { peerId, agent, startedAt(ISO), uptimeSec, listenAddrs[],
 *     reservations, pubsubPeers, connections, peers[], git }
 * Served with cache-control: no-store; polled every 3 s by the page. */
const GIT_URL_PATH = '/git/repo'
let gitRoot = null
async function setupGit () {
  const bundlePath = join(ROOT, 'repo.bundle')
  await access(bundlePath)
  gitRoot = mkdtempSync(join(tmpdir(), 'helia-relay-git-'))
  await new Promise((resolve, reject) => {
    const p = spawn('git', ['clone', '-q', '--bare', bundlePath, join(gitRoot, 'repo.git')])
    p.on('error', reject)
    p.on('close', (code) => code === 0 ? resolve() : reject(new Error('git clone --bare exited ' + code)))
  })
  console.log('git endpoint ready: ' + GIT_URL_PATH)
}
function handleGit (req, res, pathname, query) {
  const env = {
    ...process.env,
    GIT_PROJECT_ROOT: gitRoot,
    GIT_HTTP_EXPORT_ALL: '1',
    PATH_INFO: pathname.slice('/git'.length),
    QUERY_STRING: query,
    REQUEST_METHOD: req.method,
    CONTENT_TYPE: req.headers['content-type'] || '',
    REMOTE_USER: ''
  }
  const c = spawn('git', ['http-backend'], { env })
  const chunks = []
  c.stdout.on('data', (d) => chunks.push(d))
  c.stderr.on('data', (d) => console.log('git http-backend: ' + d.toString().trim()))
  c.on('error', () => { if (!res.headersSent) { res.writeHead(500); res.end('git unavailable') } })
  c.on('close', () => {
    const raw = Buffer.concat(chunks)
    const idx = raw.indexOf('\r\n\r\n')
    if (idx === -1) { res.writeHead(500); res.end('bad cgi response'); return }
    let status = 200
    for (const line of raw.slice(0, idx).toString().split('\r\n')) {
      const i = line.indexOf(':')
      const k = line.slice(0, i).toLowerCase()
      const v = line.slice(i + 1).trim()
      if (k === 'status') status = parseInt(v, 10) || 500
      else res.setHeader(k, v)
    }
    res.writeHead(status)
    res.end(raw.slice(idx + 4))
  })
  req.pipe(c.stdin)
}
await setupGit().catch((err) => console.log('git endpoint disabled: ' + err.message))

function snapshot () {
  const conns = node.getConnections()
  return {
    peerId: node.peerId.toString(),
    agent: 'helia-ipfs-relay/headless',
    startedAt: new Date(startedAt).toISOString(),
    uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
    listenAddrs: node.getMultiaddrs().map((ma) => ma.toString()),
    reservations: node.services.relay.reservations?.size ?? 0,
    pubsubPeers: node.services.pubsub.getPeers().length,
    git: Boolean(gitRoot),
    connections: conns.length,
    peers: [...new Set(conns.map((c) => c.remotePeer.toString()))].slice(0, 25)
  }
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.bundle': 'application/octet-stream',
  '.car': 'application/vnd.ipld.car',
  '.json': 'application/json; charset=utf-8'
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost')
    if (url.pathname.startsWith('/cors-proxy')) {
      // strip the mount so the proxy sees /https:/host/path (its URL format)
      req.url = req.url.slice('/cors-proxy'.length)
      // browsers cannot send a User-Agent header, but some git hosts (notably
      // github.com's smart-http endpoint) answer the clone handshake properly
      // only when the request looks like git itself — set it server-side
      req.headers['user-agent'] = 'git/2.43.0'
      corsProxy(req, res, () => { res.writeHead(404); res.end() })
      return
    }
    if (url.pathname.startsWith('/git/')) {
      if (!gitRoot) { res.writeHead(503); res.end('git endpoint unavailable (git not found at boot)'); return }
      handleGit(req, res, url.pathname, url.search.slice(1))
      return
    }
    if (url.pathname === '/api/relay') {
      const body = JSON.stringify(snapshot())
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' })
      res.end(body)
      return
    }
    const path = normalize(url.pathname === '/' ? '/index.html' : url.pathname)
    if (path.includes('..')) { res.writeHead(403); res.end(); return }
    const file = join(ROOT, sep, path)
    if (!file.startsWith(ROOT)) { res.writeHead(403); res.end(); return }
    const data = await readFile(file)
    res.writeHead(200, { 'content-type': MIME[extname(file)] ?? 'application/octet-stream', 'cache-control': 'no-store' })
    res.end(data)
  } catch {
    res.writeHead(404)
    res.end('not found')
  }
})

server.listen(PORT, () => {
  const mas = node.getMultiaddrs().map((ma) => ma.toString())
  console.log('headless relay running')
  console.log('  peer id:  ' + node.peerId.toString())
  for (const ma of mas) console.log('  listen:   ' + ma)
  console.log('  http:     http://localhost:' + PORT + '  (api: /api/relay)')
  console.log('')
  console.log('Browser nodes can dial the /ws/ address above; the page at')
  console.log('http://localhost:' + PORT + ' shows this node in the "Headless relay" card.')
})

for (const evt of ['peer:connect', 'peer:disconnect']) {
  node.addEventListener(evt, (e) => {
    console.log(evt + ' ' + e.detail.toString() + '  (' + node.getConnections().length + ' conns, ' + (node.services.relay.reservations?.size ?? 0) + ' reservations)')
  })
}
node.services.relay.addEventListener('relay:reservation', (e) => {
  console.log('relay:reservation from ' + e.detail.toString())
})

/* advertise the git endpoint on gnostr so page peers can discover it */
const gitAdUrl = (process.env.PUBLIC_URL || 'http://localhost:' + PORT) + GIT_URL_PATH
function publishGitAd () {
  const GIT_AD = { type: 'git', name: 'helia-ipfs-relay-headless', at: Date.now(), url: gitAdUrl }
  node.services.pubsub.publish(PUBSUB_TOPIC, new TextEncoder().encode(JSON.stringify(GIT_AD)))
    .then(() => console.log('pubsub: git ad published (' + node.services.pubsub.getPeers().length + ' gossipsub peers)'))
    .catch((err) => console.log('pubsub: git ad not published: ' + err.message))
}
setInterval(publishGitAd, 60000)
publishGitAd()
