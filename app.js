/* ============================================================================
   HELIA IPFS RELAY — in-browser node + optional headless shared backend

   FILE MAP
     index.html   the page: sticky header with Home/Peer tabs; every card
                  lives in the Peer tab, Home is a blank landing
     app.js       this file — source of the in-browser Helia node + all UI
     relay.js     esbuild bundle of app.js; this is what the page loads.
                  Rebuild after EVERY app.js edit (command below)
     server.js    optional headless backend (Node >= 22): circuit relay v2
                  server on TCP/WebSocket + static file server + GET /api/relay
                  + /git/repo smart-http endpoint + /cors-proxy for external
                  in-page clones (sets a git/ User-Agent; github needs it)
     package.json backend + helper dependencies; `npm start` runs server.js
     git-remote-gnostr.js  git remote helper: `git clone gnostr://<name>`
                  resolves the name over the gnostr pubsub topic and fetches
                  the advertised bundle CID over IPFS/bitswap (see its header)
     gnostr-publish.js generic publisher for ANY git repo: re-bundles every
                  minute, pins the bundle in its blockstore (seeding it over
                  bitswap), and announces it on the gnostr topic — so other
                  projects can `git clone gnostr://<their-name>` too
     repo.bundle  git bundle of this source tree — clone with
                  `git clone repo.bundle`

   RUN MODES
     Static:   serve this folder with any web server. The in-browser node
               works fully (reserves /p2p-circuit slots on public relays);
               the "Headless relay" card stays offline.
     Backend:  `npm install && node server.js` — one process serves the page
               and runs the headless relay; the page attaches automatically
               (pollHeadless() below fetches /api/relay every 3 s).

   REBUILD relay.js AFTER EDITING app.js
     npm i helia@7.1.16 @helia/libp2p@2.1.5 @helia/unixfs@8.0.9 \
         @libp2p/circuit-relay-v2@4.2.17 @libp2p/gossipsub@15.0.23 \
         @ipld/car@5.4.9 isomorphic-git @isomorphic-git/lightning-fs \
         buffer esbuild@0.25.0
     printf 'export { Buffer } from "buffer"\n' > buffer-inject.js
     npx esbuild app.js --bundle --minify --format=esm --target=es2022 \
       '--define:process.env.NODE_ENV="production"' \
       --inject:./buffer-inject.js --outfile=relay.js
     (the inject gives isomorphic-git its browser Buffer global)
     (a local build is required: esm.sh cannot bundle ipfs-unixfs-importer
     17.x because of its circular re-exports)

   REGENERATE repo.bundle AFTER ANY FILE CHANGE
     copy all files into a fresh `git init -b main` repo, commit with fixed
     author/committer dates (keeps the bundle CID stable for unchanged
     trees), then:  git bundle create repo.bundle HEAD main

   DEPLOYMENT NOTES
     - An HTTPS-served page cannot dial ws:// (mixed-content block). For
       production put a TLS terminator in front of server.js and announce
       /dns4/<host>/tcp/443/wss, or run the backend on plain HTTP.
     - The in-browser node refuses to dial private/loopback addresses
       (libp2p private-address gater) — it will not reach a LAN backend.
     - The browser peer key persists in localStorage (LS_KEY below); the
       blockstore is in-memory and resets on reload.

   GNOSTR GIT ADS (the contract shared by all three components)
     Publishers — every minute, on PUBSUB_TOPIC ("gnostr"):
       this page      { type:'git', name:'helia-ipfs-relay',
                        at:<epoch ms>, bundle:<CID>, dir:<CID> }
       server.js      { type:'git', name:'helia-ipfs-relay-headless',
                        at:<epoch ms>, url:'<origin>/git/repo' }
       gnostr-publish.js (any repo) { type:'git', name, at:<epoch ms>,
                        bundle:<CID>, addrs?:[<listen multiaddrs>] }
     Consumers:
       - the "Git repos on gnostr" card below (renders every ad heard)
       - git-remote-gnostr.js: resolves `git clone gnostr://<name>` by
         listening for the newest matching ad (newest `at` wins, bundle
         preferred over url), then fetches it — bundle ads over
         IPFS/bitswap, url ads via smart-http clone. `git clone
         gnostr://cid/<bundle-cid>` skips discovery entirely.
         Also works as a submodule URL: `git submodule add
         gnostr://<name> <path>` (requires
         `git config --global protocol.gnostr.allow always` — git blocks
         unknown transports in .gitmodules by default).
     Push is not supported by the helper; gnostr remotes are fetch-only.
   ========================================================================== */
import { createHelia } from 'helia'
import { libp2pDefaults } from '@helia/libp2p'
import { unixfs } from '@helia/unixfs'
import { circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { gossipsub } from '@libp2p/gossipsub'
import { CarWriter } from '@ipld/car'
import { generateKeyPair, privateKeyFromRaw } from '@libp2p/crypto/keys'
import { multiaddr } from '@multiformats/multiaddr'
import git from 'isomorphic-git'
import gitHttp from 'isomorphic-git/http/web'
import LightningFS from '@isomorphic-git/lightning-fs'

/* ---------------- config ---------------- */
const RELAY_CFG = {
  maxReservations: 32,
  applyDefaultLimit: false,            // no per-connection time/data cap (demo-grade relay)
  reservationTtl: 2 * 60 * 60 * 1000   // 2 h
}
const LS_KEY = 'helia-relay:peer-key:v1'
const PUBSUB_TOPIC = 'gnostr'
const GIT_AD_INTERVAL = 60000   // re-announce own repo on gnostr every 60 s
const GIT_DIR = '/repo'         // in-browser clone target (LightningFS)

/* ---------------- dom ---------------- */
const $ = (id) => document.getElementById(id)
const els = {
  pill: $('statusPill'), statusText: $('statusText'),
  btnStart: $('btnStart'), btnStop: $('btnStop'),
  vPeerId: $('vPeerId'), vAgent: $('vAgent'), vUptime: $('vUptime'), vKey: $('vKey'),
  btnCopyPeer: $('btnCopyPeer'), btnCopyAddr: $('btnCopyAddr'),
  addrList: $('addrList'), addrCount: $('addrCount'),
  stResv: $('stResv'), stRelayed: $('stRelayed'), stDirect: $('stDirect'),
  stPeers: $('stPeers'), stConns: $('stConns'), stAddrs: $('stAddrs'),
  relayLimits: $('relayLimits'), vDht: $('vDht'),
  connTbody: $('connTbody'), connCount: $('connCount'),
  hlState: $('hlState'), hlNote: $('hlNote'), hlBody: $('hlBody'),
  hlPeer: $('hlPeer'), hlUptime: $('hlUptime'), hlAgent: $('hlAgent'),
  hlResv: $('hlResv'), hlConns: $('hlConns'), hlAddrs: $('hlAddrs'),
  hlAddrList: $('hlAddrList'),
  psInput: $('psInput'), btnPublish: $('btnPublish'), psFeed: $('psFeed'), psCount: $('psCount'),
  gitCloneCmd: $('gitCloneCmd'), btnCopyCloneCmd: $('btnCopyCloneCmd'),
  gitUrlInput: $('gitUrlInput'), btnClone: $('btnClone'), gitStatus: $('gitStatus'),
  gitBrowse: $('gitBrowse'), gitInfo: $('gitInfo'), gitFiles: $('gitFiles'), gitFileView: $('gitFileView'),
  gitAdList: $('gitAdList'), gitAdCount: $('gitAdCount'),
  log: $('log'), btnClearLog: $('btnClearLog'),
  selfNote: $('selfNote'), selfBody: $('selfBody'),
  vBundleCid: $('vBundleCid'), vDirCid: $('vDirCid'), cloneHelp: $('cloneHelp'),
  btnCopyBundle: $('btnCopyBundle'), btnCopyDir: $('btnCopyDir'), btnCopyClone: $('btnCopyClone'),
  btnExportCar: $('btnExportCar'), carResult: $('carResult')
}

/* ---------------- state ---------------- */
let node = null        // helia instance with .libp2p
let fs = null
let startedAt = null
let pollTimer = null
let lastRelayAddr = ''
let bundleCid = null
let dirCid = null
let gitAdTimer = null
let headlessOnline = false
const gitAds = new Map()        // bundle CID or url -> ad entry
const gitFs = new LightningFS('helia-git')

/* ---------------- log ---------------- */
function log (msg, level = 'info') {
  const t = new Date().toLocaleTimeString('en-GB', { hour12: false })
  const line = document.createElement('div')
  line.className = 'line'
  const time = document.createElement('time')
  time.textContent = t
  const span = document.createElement('span')
  span.className = 'lv-' + level
  span.textContent = msg
  line.append(time, span)
  els.log.appendChild(line)
  while (els.log.children.length > 500) els.log.firstChild.remove()
  els.log.scrollTop = els.log.scrollHeight
}

/* ---------------- status ---------------- */
function setState (state, text) {
  els.pill.dataset.state = state
  els.statusText.textContent = text
  const running = state === 'running'
  els.btnStart.disabled = running || state === 'starting'
  els.btnStop.disabled = !running
  for (const b of [els.btnCopyPeer, els.btnCopyAddr,
                   els.btnExportCar, els.btnPublish]) {
    b.disabled = !running
  }
}

/* ---------------- helpers ---------------- */
function b64encode (u8) {
  let s = ''
  for (let i = 0; i < u8.length; i += 0x8000) s += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000))
  return btoa(s)
}
function b64decode (s) {
  const bin = atob(s)
  const u8 = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i)
  return u8
}
async function loadOrCreatePeerKey () {
  const stored = localStorage.getItem(LS_KEY)
  if (stored) return { key: privateKeyFromRaw(b64decode(stored)), persisted: true }
  const key = await generateKeyPair('Ed25519')
  localStorage.setItem(LS_KEY, b64encode(key.raw))
  return { key, persisted: false }
}
function fmtDuration (ms) {
  const s = Math.floor(ms / 1000)
  if (s < 60) return s + 's'
  const m = Math.floor(s / 60)
  if (m < 60) return m + 'm ' + (s % 60) + 's'
  return Math.floor(m / 60) + 'h ' + (m % 60) + 'm'
}
function shortPeer (id) { return id.length > 14 ? id.slice(0, 8) + '…' + id.slice(-6) : id }

/* ---------------- node lifecycle ---------------- */
async function startNode () {
  setState('starting', 'starting…')
  log('creating Helia node (in-memory blockstore)…', 'accent')
  try {
    const { key, persisted } = await loadOrCreatePeerKey()
    // config:'merge' in @helia/libp2p@2.1.5 silently drops object-valued keys
    // (merge() recurses into a throwaway object), so compose the config here:
    const libp2pConfig = libp2pDefaults()
    libp2pConfig.services = {
      ...libp2pConfig.services,
      relay: circuitRelayServer({ reservations: RELAY_CFG }),
      pubsub: gossipsub({ emitSelf: false })
    }
    node = await createHelia({
      libp2p: { ...libp2pConfig, privateKey: key }
    }).start()
    fs = unixfs(node)

    const peerId = node.libp2p.peerId.toString()
    els.vPeerId.textContent = peerId
    els.vAgent.textContent = `helia/${node.info?.version ?? '7'} · js-libp2p`
    els.vKey.textContent = persisted
      ? 'restored from localStorage (stable across reloads)'
      : 'generated now, stored in localStorage'
    startedAt = Date.now()
    els.vUptime.textContent = new Date().toLocaleTimeString()

    log('node started — peer id ' + peerId, 'good')
    if (!persisted) log('new peer identity generated and persisted', 'warn')

    wireEvents()
    subscribePubsub()
    setState('running', 'running')
    pollTimer = setInterval(refreshStats, 2000)
    refreshStats()
    publishSelf()
    gitAdTimer = setInterval(publishGitAd, GIT_AD_INTERVAL)
  } catch (err) {
    console.error(err)
    log('failed to start: ' + (err?.message ?? err), 'error')
    setState('error', 'error')
  }
}

async function stopNode () {
  clearInterval(pollTimer)
  clearInterval(gitAdTimer)
  pollTimer = null
  gitAdTimer = null
  setState('stopped', 'stopping…')
  try {
    try { node?.libp2p.services.pubsub.unsubscribe(PUBSUB_TOPIC) } catch {}
    if (node) await node.stop()
    log('node stopped', 'warn')
  } catch (err) {
    log('error while stopping: ' + (err?.message ?? err), 'error')
  }
  node = null
  fs = null
  bundleCid = null
  dirCid = null
  els.connTbody.innerHTML = '<tr><td colspan="3" class="empty">No connections yet.</td></tr>'
  els.addrList.innerHTML = '<span class="empty">Addresses appear here once the node is started and has reserved slots on public relays.</span>'
  els.addrCount.textContent = ''
  els.vUptime.textContent = '—'
  setState('stopped', 'stopped')
  refreshStats()
}

function wireEvents () {
  const lp2p = node.libp2p
  lp2p.addEventListener('peer:connect', (evt) => {
    log('peer connected: ' + shortPeer(evt.detail.toString()), 'good')
    refreshStats()
  })
  lp2p.addEventListener('peer:disconnect', (evt) => {
    log('peer disconnected: ' + shortPeer(evt.detail.toString()), 'warn')
    refreshStats()
  })
  lp2p.addEventListener('self:peer:update', () => refreshAddrs())
  lp2p.services.relay.addEventListener('relay:reservation', (evt) => {
    const from = evt.detail?.addr != null ? evt.detail.addr.getPeerId() : 'peer'
    log('relay reservation granted to ' + shortPeer(String(from ?? 'peer')), 'accent')
    refreshStats()
  })
}

/* ---------------- rendering ---------------- */
function refreshAddrs () {
  if (!node) return
  const addrs = node.libp2p.getMultiaddrs().map((ma) => ma.toString())
  lastRelayAddr = addrs.find((a) => a.includes('/p2p-circuit')) ?? lastRelayAddr
  els.addrCount.textContent = String(addrs.length)
  els.addrList.innerHTML = ''
  if (addrs.length === 0) {
    els.addrList.innerHTML = '<span class="empty">No addresses yet — waiting for relay reservations…</span>'
    return
  }
  for (const a of addrs) {
    const row = document.createElement('div')
    row.className = 'addr-row'
    const tag = document.createElement('span')
    tag.className = 'tag'
    tag.textContent = a.includes('/p2p-circuit') ? 'relay' : a.includes('/webrtc') ? 'webrtc' : 'direct'
    const ma = document.createElement('span')
    ma.className = 'ma'
    ma.textContent = a
    const copy = document.createElement('button')
    copy.className = 'icon-btn'
    copy.title = 'Copy address'
    copy.innerHTML = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="3.5" y="3.5" width="6" height="6" rx="1" stroke="currentColor" stroke-width="1.1"/><path d="M8.5 3.5v-1a1 1 0 0 0-1-1h-5a1 1 0 0 0-1 1v5a1 1 0 0 0 1 1h1" stroke="currentColor" stroke-width="1.1"/></svg>'
    copy.addEventListener('click', () => { navigator.clipboard.writeText(a); log('address copied', 'info') })
    row.append(tag, ma, copy)
    els.addrList.appendChild(row)
  }
}

function refreshStats () {
  if (!node) {
    for (const el of [els.stPeers, els.stConns, els.stAddrs, els.stResv, els.stRelayed, els.stDirect]) el.textContent = '0'
    els.relayLimits.textContent = 'Reservation limits shown while running.'
    els.connCount.textContent = '0'
    return
  }
  const lp2p = node.libp2p
  const conns = lp2p.getConnections()
  const peers = new Set(conns.map((c) => c.remotePeer.toString()))
  let relayed = 0; let direct = 0
  const rows = []
  for (const c of conns) {
    const addr = c.remoteAddr.toString()
    const isRelay = addr.includes('/p2p-circuit')
    if (isRelay) relayed++; else direct++
    rows.push({
      peer: c.remotePeer.toString(),
      dir: c.direction,
      via: isRelay ? 'relay' : (addr.split('/').filter(Boolean).pop()?.toUpperCase() ?? '—')
    })
  }
  els.stPeers.textContent = String(peers.size)
  els.stConns.textContent = String(conns.length)
  els.stResv.textContent = String(lp2p.services.relay.reservations?.size ?? 0)
  els.stRelayed.textContent = String(relayed)
  els.stDirect.textContent = String(direct)
  els.relayLimits.textContent =
    `limits: ${RELAY_CFG.maxReservations} reservations · ${fmtDuration(RELAY_CFG.reservationTtl)} reservation TTL · no per-connection cap`
  refreshAddrs()
  els.stAddrs.textContent = els.addrCount.textContent
  els.vUptime.textContent = fmtDuration(Date.now() - startedAt)
  els.psCount.textContent = node.libp2p.services.pubsub.getPeers().length

  els.connCount.textContent = String(rows.length)
  els.connTbody.innerHTML = ''
  if (rows.length === 0) {
    els.connTbody.innerHTML = '<tr><td colspan="3" class="empty">No connections yet.</td></tr>'
    return
  }
  rows.sort((a, b) => a.peer.localeCompare(b.peer))
  for (const r of rows.slice(0, 50)) {
    const tr = document.createElement('tr')
    const tdPeer = document.createElement('td'); tdPeer.textContent = shortPeer(r.peer); tdPeer.title = r.peer
    const tdDir = document.createElement('td')
    tdDir.className = r.dir === 'inbound' ? 'dir-in' : 'dir-out'
    tdDir.textContent = r.dir
    const tdVia = document.createElement('td'); tdVia.textContent = r.via
    tr.append(tdPeer, tdDir, tdVia)
    els.connTbody.appendChild(tr)
  }
}

/* ---------------- actions ---------------- */
function showResult (el, text, isErr = false) {
  el.className = 'result show' + (isErr ? ' err' : '')
  el.textContent = text
}

/* ---------------- self-hosting ---------------- */
async function publishSelf () {
  try {
    const [bundleBytes, indexBytes, appBytes, relayBytes] = await Promise.all(
      ['repo.bundle', 'index.html', 'app.js', 'relay.js'].map(async (f) => {
        const res = await fetch(f)
        if (!res.ok) throw new Error(f + ' HTTP ' + res.status)
        return new Uint8Array(await res.arrayBuffer())
      }))

    bundleCid = await fs.addBytes(bundleBytes)

    const entries = []
    for await (const entry of fs.addAll([
      { path: 'helia-ipfs-relay/index.html', content: indexBytes },
      { path: 'helia-ipfs-relay/app.js', content: appBytes },
      { path: 'helia-ipfs-relay/relay.js', content: relayBytes },
      { path: 'helia-ipfs-relay/repo.bundle', content: bundleBytes }
    ])) {
      entries.push(entry)
    }
    dirCid = (entries.find((e) => e.path === '') ?? entries[entries.length - 1]).cid

    els.selfNote.style.display = 'none'
    els.selfBody.classList.add('show')
    els.vBundleCid.textContent = bundleCid.toString()
    els.vDirCid.textContent = dirCid.toString()
    const b = bundleCid.toString()
    els.cloneHelp.textContent =
      '# fetch the bundle and clone it with plain git:\n' +
      'curl -L https://ipfs.io/ipfs/' + b + ' -o repo.bundle\n' +
      'git clone repo.bundle helia-ipfs-relay\n\n' +
      '# or with a local Kubo node:\n' +
      'ipfs get ' + b + ' && git clone ' + b + ' helia-ipfs-relay\n\n' +
      '# browse the raw sources: https://ipfs.io/ipfs/' + dirCid.toString() + '/'
    for (const btn of [els.btnCopyBundle, els.btnCopyDir, els.btnCopyClone]) btn.disabled = false
    log('self-hosted: git bundle ' + b, 'good')
    log('self-hosted: source dir ' + dirCid.toString(), 'good')
    publishGitAd()
  } catch (err) {
    els.selfNote.textContent = 'Self-hosting unavailable: ' + (err?.message ?? err) +
      ' — serve this folder over HTTP (repo.bundle must sit next to index.html).'
    log('self-host publish failed: ' + (err?.message ?? err), 'error')
  }
}

async function doExportCar () {
  if (!node) return
  try {
    const roots = [bundleCid, dirCid].filter(Boolean)
    const { writer, out } = await CarWriter.create(roots)
    const chunks = []
    const collect = (async () => { for await (const c of out) chunks.push(c) })()
    let count = 0
    for await (const pair of node.blockstore.getAll()) {
      // getAll() yields { cid, bytes } where bytes may be a chunk generator
      let bytes = pair.bytes
      if (!(bytes instanceof Uint8Array)) {
        const chunks = []
        let len = 0
        for await (const chunk of bytes) { chunks.push(chunk); len += chunk.length }
        bytes = new Uint8Array(len)
        let off = 0
        for (const chunk of chunks) { bytes.set(chunk, off); off += chunk.length }
      }
      await writer.put({ cid: pair.cid, bytes })
      count++
    }
    await writer.close()
    await collect
    const blob = new Blob(chunks, { type: 'application/vnd.ipld.car' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'helia-relay.car'
    a.click()
    setTimeout(() => URL.revokeObjectURL(url), 10000)
    showResult(els.carResult, 'exported ' + count + ' blocks, ' + blob.size + ' bytes, roots: ' +
      roots.map((r) => r.toString()).join(', '))
    log('CAR exported: ' + count + ' blocks (' + blob.size + ' bytes)', 'good')
  } catch (err) {
    showResult(els.carResult, 'CAR export failed: ' + (err?.message ?? err), true)
    log('CAR export failed: ' + (err?.message ?? err), 'error')
  }
}

/* ---------------- pubsub (gossipsub, topic "gnostr") ---------------- */
function psFeed (from, text, mine) {
  const t = new Date().toLocaleTimeString('en-GB', { hour12: false })
  const line = document.createElement('div')
  line.className = 'line'
  const time = document.createElement('time')
  time.textContent = t
  const span = document.createElement('span')
  span.className = mine ? 'lv-accent' : 'lv-info'
  span.textContent = (mine ? 'you' : from) + ': ' + text
  line.append(time, span)
  els.psFeed.appendChild(line)
  while (els.psFeed.children.length > 200) els.psFeed.firstChild.remove()
  els.psFeed.scrollTop = els.psFeed.scrollHeight
}
function subscribePubsub () {
  const pubsub = node.libp2p.services.pubsub
  pubsub.subscribe(PUBSUB_TOPIC)
  pubsub.addEventListener('message', (evt) => {
    if (evt.detail.topic !== PUBSUB_TOPIC) return
    const text = new TextDecoder().decode(evt.detail.data)
    try {
      const ad = JSON.parse(text)
      if (ad && ad.type === 'git') { onGitAd(evt.detail.from.toString(), ad, false); return }
    } catch {}
    psFeed(evt.detail.from.toString().slice(-8), text, false)
  })
  log('pubsub: subscribed to "' + PUBSUB_TOPIC + '"', 'accent')
}
async function doPublish () {
  const text = els.psInput.value
  if (!text || !node) return
  try {
    await node.libp2p.services.pubsub.publish(PUBSUB_TOPIC, new TextEncoder().encode(text))
    psFeed('', text, true)
    els.psInput.value = ''
    log('pubsub: published to "' + PUBSUB_TOPIC + '"', 'good')
  } catch (err) {
    if (String(err?.message ?? err).includes('NoPeersSubscribed')) {
      psFeed('you · not relayed (no peers yet)', text, false)
      log('pubsub: no peers subscribed yet — message kept local', 'warn')
    } else {
      log('publish failed: ' + (err?.message ?? err), 'error')
    }
  }
}

/* ---------------- git repo ads on gnostr ----------------
 * Ads are JSON on the pubsub topic: { type:'git', name, bundle?, dir?, url? }
 * bundle/dir -> clone via IPFS; url -> clone directly with git. */
function renderGitAds () {
  els.gitAdList.innerHTML = ''
  els.gitAdCount.textContent = String(gitAds.size)
  for (const e of gitAds.values()) {
    const entry = document.createElement('div')
    entry.style.marginBottom = '10px'
    const row = document.createElement('div')
    row.className = 'addr-row'
    const info = document.createElement('span')
    info.className = 'ma'
    info.textContent = e.name + (e.mine ? ' · you' : ' · ' + e.peer.slice(-8)) + ' — ' +
      (e.url ? e.url : String(e.bundle).slice(0, 20) + '…')
    const tag = document.createElement('span')
    tag.className = 'tag'
    tag.textContent = e.url ? 'git url' : 'ipfs'
    row.append(info, tag)
    const btn = document.createElement('button')
    btn.className = 'btn'
    btn.style.minHeight = '28px'
    btn.style.marginTop = '6px'
    btn.textContent = 'Copy clone recipe'
    btn.addEventListener('click', () => {
      const recipe = e.url
        ? 'git clone ' + e.url
        : 'ipfs get ' + e.bundle + ' -o repo.bundle && git clone repo.bundle'
      navigator.clipboard.writeText(recipe)
      log('clone recipe copied', 'info')
    })
    entry.append(row, btn)
    els.gitAdList.appendChild(entry)
  }
}
function onGitAd (peer, ad, mine) {
  if (!ad || (ad.type !== 'git') || (!ad.bundle && !ad.url)) return
  const key = ad.url || ad.bundle
  if (gitAds.has(key)) { gitAds.get(key).seen = Date.now(); return }
  gitAds.set(key, { name: String(ad.name || 'unnamed repo'), bundle: ad.bundle, dir: ad.dir, url: ad.url, peer, mine, seen: Date.now() })
  renderGitAds()
  log('git repo discovered on gnostr: ' + (ad.name || key), 'accent')
}
function publishGitAd () {
  if (!node || !bundleCid) return
  const ad = { type: 'git', name: 'helia-ipfs-relay', at: Date.now(), bundle: bundleCid.toString(), dir: dirCid ? dirCid.toString() : null }
  if (headlessOnline) ad.url = location.origin + '/git/repo'
  node.libp2p.services.pubsub.publish(PUBSUB_TOPIC, new TextEncoder().encode(JSON.stringify(ad))).catch(() => {})
  onGitAd(node.libp2p.peerId.toString(), ad, true)
}

/* ---------------- git clone + browse (isomorphic-git) ----------------
   THREE CLONE PATHS, all into LightningFS (browser memory), branch+history
   depth 1, force:true so a re-clone overwrites '/repo':
     a) blank input        → this origin's /git/repo smart-http endpoint
                             (needs the headless backend online)
     b) external URL       → fetched through this origin's /cors-proxy
                             (same-origin, so no CORS failure in-page; the
                             backend forwards to the real host). server.js
                             sets a git/ User-Agent on proxied requests —
                             browsers can't send one and github.com's
                             smart-http endpoint otherwise refuses the clone
                             handshake. Verified: in-page clone of
                             github.com/RandyMcMillan/ipfs-gh-pages
                             (171 files, HEAD ab9ed44)
     c) browse             → git.listFiles + readBlob per clicked file,
                             rendered into #gitFileView (text, 8000-char cap)
   --------------------------------------------------------------------- */
async function doGitClone () {
  const custom = els.gitUrlInput.value.trim()
  const url = custom || (location.origin + '/git/repo')
  if (!custom && !headlessOnline) {
    showResult(els.gitStatus, 'Cloning this site needs the shared backend (node server.js). You can still clone an external URL if this origin has /cors-proxy.', true)
    return
  }
  showResult(els.gitStatus, 'cloning ' + url + ' …')
  els.btnClone.disabled = true
  try {
    await git.clone({
      fs: gitFs, http: gitHttp, dir: GIT_DIR, url,
      corsProxy: custom ? location.origin + '/cors-proxy' : undefined,
      singleBranch: true, depth: 1, force: true
    })
    const head = await git.resolveRef({ fs: gitFs, dir: GIT_DIR, ref: 'HEAD' })
    const [latest] = await git.log({ fs: gitFs, dir: GIT_DIR, depth: 1 })
    els.gitInfo.textContent = latest.commit.message.trim().split('\n')[0] + ' · ' + head.slice(0, 12)
    els.gitBrowse.hidden = false
    await renderGitFiles()
    showResult(els.gitStatus, 'cloned — browse below')
    log('git: cloned ' + url, 'good')
  } catch (err) {
    showResult(els.gitStatus, 'clone failed: ' + (err?.message ?? err), true)
    log('git clone failed: ' + (err?.message ?? err), 'error')
  } finally {
    els.btnClone.disabled = !headlessOnline
  }
}
async function renderGitFiles () {
  const files = await git.listFiles({ fs: gitFs, dir: GIT_DIR, ref: 'HEAD' })
  els.gitFiles.innerHTML = ''
  for (const f of files) {
    const row = document.createElement('div')
    row.className = 'addr-row'
    row.style.cursor = 'pointer'
    const span = document.createElement('span')
    span.className = 'ma'
    span.textContent = f
    row.appendChild(span)
    row.addEventListener('click', () => showGitFile(f))
    els.gitFiles.appendChild(row)
  }
  els.gitFileView.className = 'result show'
  els.gitFileView.textContent = files.length + ' files at HEAD — click one to view'
}
async function showGitFile (path) {
  try {
    const oid = await git.resolveRef({ fs: gitFs, dir: GIT_DIR, ref: 'HEAD' })
    const { blob } = await git.readBlob({ fs: gitFs, dir: GIT_DIR, oid, filepath: path })
    const text = new TextDecoder().decode(blob)
    els.gitFileView.className = 'result show'
    els.gitFileView.textContent = '— ' + path + ' —\n' + (text.length > 8000 ? text.slice(0, 8000) + '\n…(truncated)' : text)
  } catch (err) {
    showResult(els.gitFileView, 'read failed: ' + (err?.message ?? err), true)
  }
}

/* ---------------- headless relay (shared backend) ---------------- */
async function pollHeadless () {
  try {
    const res = await fetch('/api/relay', { cache: 'no-store' })
    if (!res.ok) throw new Error('HTTP ' + res.status)
    const s = await res.json()
    headlessOnline = true
    els.btnClone.disabled = false
    els.gitCloneCmd.textContent = 'git clone ' + location.origin + '/git/repo'
    els.btnCopyCloneCmd.disabled = false
    els.hlState.textContent = 'online'
    els.hlNote.hidden = true
    els.hlBody.hidden = false
    els.hlPeer.textContent = s.peerId
    els.hlUptime.textContent = fmtDuration(s.uptimeSec * 1000)
    els.hlAgent.textContent = s.agent
    els.hlResv.textContent = s.reservations
    els.hlConns.textContent = s.connections
    els.hlAddrs.textContent = s.listenAddrs.length
    els.hlAddrList.innerHTML = ''
    for (const ma of s.listenAddrs) {
      const row = document.createElement('div')
      row.className = 'addr-row'
      const span = document.createElement('span')
      span.className = 'ma'
      span.textContent = ma
      const tag = document.createElement('span')
      tag.className = 'tag'
      tag.textContent = ma.includes('/ws') ? 'ws · browser-dialable' : 'tcp'
      row.append(span, tag)
      els.hlAddrList.appendChild(row)
    }
  } catch {
    headlessOnline = false
    els.btnClone.disabled = true
    els.hlState.textContent = 'offline'
    els.hlNote.hidden = false
    els.hlBody.hidden = true
  }
}
pollHeadless()
setInterval(pollHeadless, 3000)

/* ---------------- wiring ---------------- */
const tabEls = { home: $('tabHome'), peer: $('tabPeer') }
document.querySelectorAll('.tabs .tab').forEach((btn) => {
  btn.addEventListener('click', () => {
    for (const [name, el] of Object.entries(tabEls)) el.hidden = name !== btn.dataset.tab
    document.querySelectorAll('.tabs .tab').forEach((b) => b.classList.toggle('active', b === btn))
  })
})
els.btnStart.addEventListener('click', startNode)
els.btnStop.addEventListener('click', stopNode)
els.btnClearLog.addEventListener('click', () => { els.log.innerHTML = '' })
els.btnPublish.addEventListener('click', doPublish)
els.psInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doPublish() })
els.btnClone.addEventListener('click', doGitClone)
els.btnCopyCloneCmd.addEventListener('click', () => {
  if (els.gitCloneCmd.textContent) { navigator.clipboard.writeText(els.gitCloneCmd.textContent); log('clone command copied', 'info') }
})
els.btnExportCar.addEventListener('click', doExportCar)
els.btnCopyBundle.addEventListener('click', () => {
  if (bundleCid) { navigator.clipboard.writeText(bundleCid.toString()); log('bundle CID copied', 'info') }
})
els.btnCopyDir.addEventListener('click', () => {
  if (dirCid) { navigator.clipboard.writeText(dirCid.toString()); log('dir CID copied', 'info') }
})
els.btnCopyClone.addEventListener('click', () => {
  if (els.cloneHelp.textContent) { navigator.clipboard.writeText(els.cloneHelp.textContent); log('clone commands copied', 'info') }
})
els.btnCopyPeer.addEventListener('click', () => {
  if (node) { navigator.clipboard.writeText(node.libp2p.peerId.toString()); log('peer id copied', 'info') }
})
els.btnCopyAddr.addEventListener('click', () => {
  const target = lastRelayAddr || (node ? node.libp2p.getMultiaddrs()[0]?.toString() : '')
  if (target) { navigator.clipboard.writeText(target); log('address copied', 'info') }
})
window.addEventListener('beforeunload', () => { if (node) node.stop().catch(() => {}) })
window.__relay = { get node () { return node }, get fs () { return fs }, get lastRelayAddr () { return lastRelayAddr }, get bundleCid () { return bundleCid }, get dirCid () { return dirCid }, multiaddr }

log('page loaded — booting the Helia node…', 'info')
startNode()

/* ============================================================================
   REMOVED FEATURES — HOW TO RE-ENABLE
   Three features were removed from the UI. Each needs markup in index.html
   (commented blocks marked "RE-ENABLE (1)" and "RE-ENABLE (2)") plus the
   code below. After any change, rebuild the JS bundle from the app.js entry:

     npx esbuild app.js --bundle --minify --format=esm --target=es2022 \
       '--define:process.env.NODE_ENV="production"' --outfile=relay.js

   ---------------------------------------------------------------------------
   FEATURE 1 — CONTENT CARD (add text / add file / fetch CID)

   Step 1: in index.html, uncomment the section marked "RE-ENABLE (1)"
           (it sits between the Connections and Dial cards).
   Step 2: add this import next to the other imports at the top of the file:

       import { CID } from 'multiformats/cid'

   Step 3: add these refs to the `els` object (in the dom section):

       addText: $('addText'), btnAddText: $('btnAddText'),
       addFile: $('addFile'), btnAddFile: $('btnAddFile'),
       catInput: $('catInput'), btnCat: $('btnCat'),
       addResult: $('addResult'), catResult: $('catResult'),

   Step 4: add els.btnAddText, els.btnAddFile and els.btnCat to the
           disabled-button list inside setState().
   Step 5: paste in the functions and listeners:

       async function doAddBytes (bytes, label) {
         if (!node) return
         try {
           const cid = await fs.addBytes(bytes)
           showResult(els.addResult, label + ' added:\n' + cid.toString())
           log(label + ' added: ' + cid, 'good')
         } catch (err) {
           showResult(els.addResult, 'add failed: ' + (err?.message ?? err), true)
           log('add failed: ' + (err?.message ?? err), 'error')
         }
       }
       async function doAddText () {
         if (!els.addText.value || !node) return
         await doAddBytes(new TextEncoder().encode(els.addText.value), 'text')
       }
       async function doAddFile () {
         const file = els.addFile.files?.[0]
         if (!file || !node) return
         try {
           const content = new Uint8Array(await file.arrayBuffer())
           let root = null
           for await (const entry of fs.addAll([{ path: file.name, content }])) root = entry
           showResult(els.addResult, file.name + ' added:\n' + root.cid.toString())
           log(file.name + ' added: ' + root.cid, 'good')
         } catch (err) {
           showResult(els.addResult, 'add failed: ' + (err?.message ?? err), true)
           log('add failed: ' + (err?.message ?? err), 'error')
         }
       }
       async function doCat () {
         const input = els.catInput.value.trim()
         if (!input || !node) return
         try {
           const chunks = []
           let len = 0
           for await (const chunk of fs.cat(CID.parse(input))) { chunks.push(chunk); len += chunk.length }
           const bytes = new Uint8Array(len)
           let off = 0
           for (const chunk of chunks) { bytes.set(chunk, off); off += chunk.length }
           const text = new TextDecoder().decode(bytes)
           showResult(els.catResult, text.length > 4000 ? text.slice(0, 4000) + '…' : text)
           log('fetched ' + input + ' (' + bytes.length + ' bytes)', 'good')
         } catch (err) {
           showResult(els.catResult, 'fetch failed: ' + (err?.message ?? err), true)
           log('fetch failed: ' + (err?.message ?? err), 'error')
         }
       }
       els.btnAddText.addEventListener('click', doAddText)
       els.addText.addEventListener('keydown', (e) => { if (e.key === 'Enter') doAddText() })
       els.btnAddFile.addEventListener('click', doAddFile)
       els.btnCat.addEventListener('click', doCat)
       els.catInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doCat() })

   ---------------------------------------------------------------------------
   FEATURE 2 — CAR IMPORT (import a .car file into the blockstore)

   Step 1: in index.html, uncomment the field and button marked
           "RE-ENABLE (2)" (inside the Self-hosting card, next to Export CAR).
   Step 2: change the @ipld/car import at the top of the file to:

       import { CarWriter, CarReader } from '@ipld/car'

   Step 3: add these refs to the `els` object:

       carFile: $('carFile'), btnImportCar: $('btnImportCar'),

   Step 4: add els.btnImportCar to the disabled-button list inside setState().
   Step 5: paste in the function and listener:

       async function doImportCar () {
         const file = els.carFile.files?.[0]
         if (!file || !node) return
         try {
           const reader = await CarReader.fromBytes(new Uint8Array(await file.arrayBuffer()))
           const roots = (await reader.getRoots()).map((r) => r.toString())
           let count = 0
           for await (const { cid, bytes } of reader.blocks()) {
             await node.blockstore.put(cid, bytes)
             count++
           }
           showResult(els.carResult, 'imported ' + count + ' blocks from "' + file.name + '"' +
             (roots.length ? '\nroots: ' + roots.join(', ') : ''))
           log('CAR imported: ' + count + ' blocks from ' + file.name, 'good')
         } catch (err) {
           showResult(els.carResult, 'CAR import failed: ' + (err?.message ?? err), true)
           log('CAR import failed: ' + (err?.message ?? err), 'error')
         }
       }
       els.btnImportCar.addEventListener('click', doImportCar)

   ---------------------------------------------------------------------------
   FEATURE 3 — DIAL A PEER (dial a multiaddr or peer ID manually)

   Step 1: in index.html, uncomment the section marked "RE-ENABLE (3)"
           (it sits between the content-card comment and the Self-hosting card).
   Step 2: add this import next to the other imports at the top of the file:

       import { multiaddr } from '@multiformats/multiaddr'

   Step 3: add these refs to the `els` object:

       dialInput: $('dialInput'), btnDial: $('btnDial'), dialResult: $('dialResult'),

   Step 4: add els.btnDial to the disabled-button list inside setState().
   Step 5: paste in the function and listeners:

       async function doDial () {
         const input = els.dialInput.value.trim()
         if (!input || !node) return
         els.dialResult.className = 'result show'
         els.dialResult.textContent = 'Dialing ' + input + ' …'
         log('dialing ' + input, 'accent')
         try {
           const conn = input.startsWith('/')
             ? await node.libp2p.dial(multiaddr(input), { signal: AbortSignal.timeout(30000) })
             : await node.libp2p.dial(input, { signal: AbortSignal.timeout(30000) })
           const msg = 'connected to ' + conn.remotePeer.toString() + '\nvia ' + conn.remoteAddr.toString()
           els.dialResult.textContent = msg
           log('dial succeeded: ' + conn.remotePeer.toString(), 'good')
           refreshStats()
         } catch (err) {
           els.dialResult.textContent = 'Dial failed: ' + (err?.message ?? err)
           els.dialResult.classList.add('err')
           log('dial failed: ' + (err?.message ?? err), 'error')
         }
       }
       els.btnDial.addEventListener('click', doDial)
       els.dialInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doDial() })
   ========================================================================== */
