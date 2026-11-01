/**
 * GGE Tracker API（Cloudflare Workers + D1）v2
 *
 * 認証   : Authorization: Bearer <TOKEN>（Secret）／ X-User-Id: <ユーザーID>
 * ルート : POST /api/aqua/sync | settings | test
 *          POST /api/fort/sync | settings | test
 * データ : 要塞・ドラゴン = ユーザーIDごと／アクア = 全員で共通（user_id='shared'）
 * 通知   : Cronトリガー（5分おき）。Webhook・通知設定はアプリから登録（D1に保存）
 *
 * Secrets: TOKEN
 * Vars   : ALLOWED_ORIGIN（カンマ区切りで複数可）
 */

const FORT_TYPES = ['dragon', 'fortress', 'wildFortress']
const AQUA_TYPES = ['aqua', 'smallAqua']
const CHANNEL_TYPES = { aqua: AQUA_TYPES, fort: FORT_TYPES }
const SHARED_USER = 'shared' // アクアの共通データの保存先

const TYPE_INFO = {
  dragon: { name: 'ドラゴン', icon: '🐉' },
  fortress: { name: '砂漠の要塞', icon: '🏰' },
  wildFortress: { name: '野生の要塞', icon: '🌲' },
  aqua: { name: '大アクア', icon: '🌊' },
  smallAqua: { name: '小アクア', icon: '💧' }
}

const TOMB_KEEP_MS = 60 * 24 * 3600 * 1000       // 削除の記録は60日で破棄
const HIST_TOMB_KEEP_MS = 180 * 24 * 3600 * 1000 // ルビー記録の削除の記録は180日で破棄
const MAX_ITEMS = 1000
const MAX_HIST = 3000
const GRACE_MIN = 10          // 時刻から何分以内なら「通常の通知」にするか
const DIGEST_MAX_AGE_H = 12   // まとめ通知に含める最大の遅れ（時間）

/* ================= スキーマ（初回アクセス時に自動作成） ================= */
const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS items (
    user_id TEXT NOT NULL, channel TEXT NOT NULL, item_key TEXT NOT NULL, type TEXT NOT NULL,
    x REAL NOT NULL, y REAL NOT NULL, label TEXT NOT NULL DEFAULT '',
    event_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0, notify INTEGER NOT NULL DEFAULT 1, notified INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, channel, item_key))`,
  `CREATE INDEX IF NOT EXISTS idx_items_notify ON items (deleted, notify, event_at)`,
  `CREATE TABLE IF NOT EXISTS history (
    user_id TEXT NOT NULL, id TEXT NOT NULL, type TEXT NOT NULL DEFAULT '', entry_id TEXT NOT NULL DEFAULT '',
    label TEXT NOT NULL DEFAULT '', x REAL NOT NULL DEFAULT 0, y REAL NOT NULL DEFAULT 0,
    result TEXT NOT NULL DEFAULT 'ruby', ruby INTEGER NOT NULL DEFAULT 0, at_ms INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0, touched_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, id))`,
  `CREATE INDEX IF NOT EXISTS idx_history_touched ON history (user_id, touched_at)`,
  `CREATE TABLE IF NOT EXISTS settings (
    user_id TEXT NOT NULL, channel TEXT NOT NULL, json TEXT NOT NULL,
    PRIMARY KEY (user_id, channel))`
]

let schemaReady = null
function ensureSchema(env) {
  if (!schemaReady) {
    schemaReady = env.DB.batch(SCHEMA.map((s) => env.DB.prepare(s))).catch((e) => {
      schemaReady = null
      throw e
    })
  }
  return schemaReady
}

/* D1のバッチは100件ずつに分けて実行 */
async function runBatch(env, stmts) {
  for (let i = 0; i < stmts.length; i += 100) {
    await env.DB.batch(stmts.slice(i, i + 100))
  }
}

/* ================= 共通 ================= */
function json(obj, status, cors) {
  return new Response(JSON.stringify(obj), {
    status: status || 200,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...(cors || {}) }
  })
}

function corsHeaders(request, env) {
  const origin = request.headers.get('Origin') || ''
  const allowed = String(env.ALLOWED_ORIGIN || '*').split(',').map((s) => s.trim()).filter(Boolean)
  const h = {
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Authorization, X-User-Id, Content-Type',
    'Access-Control-Max-Age': '86400',
    Vary: 'Origin'
  }
  if (allowed.includes('*')) h['Access-Control-Allow-Origin'] = '*'
  else if (allowed.includes(origin)) h['Access-Control-Allow-Origin'] = origin
  return h
}

async function safeEqual(a, b) {
  const enc = new TextEncoder()
  const [ha, hb] = await Promise.all([
    crypto.subtle.digest('SHA-256', enc.encode(String(a))),
    crypto.subtle.digest('SHA-256', enc.encode(String(b)))
  ])
  const x = new Uint8Array(ha)
  const y = new Uint8Array(hb)
  let d = 0
  for (let i = 0; i < x.length; i++) d |= x[i] ^ y[i]
  return d === 0
}

const asArr = (v) => (Array.isArray(v) ? v : [])
const pad = (n) => String(n).padStart(2, '0')
const jstHour = (ms) => new Date(ms + 9 * 3600000).getUTCHours()
function jstHM(ms) {
  const d = new Date(ms + 9 * 3600000)
  return pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes())
}

/* ================= リクエスト処理 ================= */
export default {
  async fetch(request, env) {
    const cors = corsHeaders(request, env)
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors })
    try {
      return await route(request, env, cors)
    } catch (err) {
      console.error(err)
      return json({ ok: false, error: 'サーバーエラー: ' + ((err && err.message) || err) }, 500, cors)
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(notifyCheck(env).catch((e) => console.error('notifyCheck error', e)))
  }
}

async function route(request, env, cors) {
  const url = new URL(request.url)
  const path = url.pathname.replace(/\/+$/, '') || '/'

  if (request.method === 'GET' && (path === '/' || path === '/api')) {
    return json({ ok: true, message: 'GGE Tracker API is running' }, 200, cors)
  }

  const m = path.match(/^\/api\/(aqua|fort)\/(sync|settings|test)$/)
  if (!m || request.method !== 'POST') return json({ ok: false, error: '不明な操作です' }, 404, cors)
  const channel = m[1]
  const action = m[2]

  if (!env.TOKEN) return json({ ok: false, error: 'サーバーにTOKENが設定されていません' }, 500, cors)
  const auth = request.headers.get('Authorization') || ''
  const token = auth.startsWith('Bearer ') ? auth.slice(7).trim() : ''
  if (!(await safeEqual(token, env.TOKEN))) return json({ ok: false, error: '合言葉が違います' }, 401, cors)

  const userId = (request.headers.get('X-User-Id') || '').trim()
  if (!/^[\x21-\x7E]{1,64}$/.test(userId)) {
    return json({ ok: false, error: 'ユーザーIDが正しくありません（半角英数字・記号、64文字以内）' }, 400, cors)
  }
  // アクアは全員で共通のデータ。要塞・ドラゴンはユーザーごと
  const dataUser = channel === 'aqua' ? SHARED_USER : userId

  await ensureSchema(env)
  let body = {}
  try { body = await request.json() } catch (e) { body = {} }
  if (!body || typeof body !== 'object') body = {}

  if (action === 'settings') {
    const r = await handleSettings(env, dataUser, channel, body)
    return json(r, r.ok ? 200 : 400, cors)
  }
  if (action === 'test') {
    const r = await handleTest(env, dataUser, channel, userId, body)
    return json(r, r.ok ? 200 : 400, cors)
  }
  return json(await handleSync(env, dataUser, channel, body), 200, cors)
}

/* ================= 端末同期（sync） ================= */
function normalizeItem(it, types) {
  if (!it || types.indexOf(it.type) === -1) return null
  const x = Number(it.x)
  const y = Number(it.y)
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null
  const deleted = it.deleted === true
  const eventAt = Number(it.eventAt)
  if (!deleted && !Number.isFinite(eventAt)) return null
  return {
    key: it.type + '|' + x + '|' + y,
    type: it.type,
    x, y,
    label: String(it.label || '').slice(0, 50),
    eventAt: Number.isFinite(eventAt) ? eventAt : 0,
    updatedAt: Number(it.updatedAt) || 0,
    deleted,
    notify: it.notify !== false, // 省略時は通知する
    notified: 0
  }
}

/* incoming(a) が current(b) に勝つか（「最後に編集した方が勝ち」） */
function wins(a, b) {
  if (a.deleted && b.deleted) return a.updatedAt > b.updatedAt
  if (a.deleted) return a.updatedAt >= b.updatedAt
  if (b.deleted) return a.updatedAt > b.updatedAt
  if (a.updatedAt !== b.updatedAt) return a.updatedAt > b.updatedAt
  return a.eventAt > b.eventAt
}

function rowToRec(r) {
  return {
    key: r.item_key, type: r.type, x: r.x, y: r.y, label: r.label || '',
    eventAt: r.event_at, updatedAt: r.updated_at, deleted: !!r.deleted,
    notify: !!r.notify, notified: r.notified || 0
  }
}
function publicRec(r) {
  return {
    key: r.key, type: r.type, x: r.x, y: r.y, label: r.label,
    eventAt: r.eventAt, updatedAt: r.updatedAt, deleted: r.deleted, notify: r.notify
  }
}

function upsertStmt(env, dataUser, channel, r) {
  return env.DB.prepare(
    `INSERT INTO items (user_id, channel, item_key, type, x, y, label, event_at, updated_at, deleted, notify, notified)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
     ON CONFLICT(user_id, channel, item_key) DO UPDATE SET
       type=excluded.type, x=excluded.x, y=excluded.y, label=excluded.label,
       event_at=excluded.event_at, updated_at=excluded.updated_at,
       deleted=excluded.deleted, notify=excluded.notify, notified=excluded.notified`
  ).bind(dataUser, channel, r.key, r.type, r.x, r.y, r.label, r.eventAt, r.updatedAt, r.deleted ? 1 : 0, r.notify ? 1 : 0, r.notified || 0)
}

async function handleSync(env, dataUser, channel, req) {
  const types = CHANNEL_TYPES[channel]
  const now = Date.now()

  const { results } = await env.DB.prepare(
    'SELECT item_key, type, x, y, label, event_at, updated_at, deleted, notify, notified FROM items WHERE user_id=? AND channel=?'
  ).bind(dataUser, channel).all()
  const map = new Map()
  results.forEach((r) => map.set(r.item_key, rowToRec(r)))
  const dirty = new Set()

  asArr(req.items).slice(0, MAX_ITEMS).forEach((it) => {
    const rec = normalizeItem(it, types)
    if (!rec) return
    if (channel === 'aqua') rec.notify = true // アクアの通知対象は、通知設定の「種類」で決める
    const cur = map.get(rec.key)
    if (!cur) { map.set(rec.key, rec); dirty.add(rec.key); return }
    if (wins(rec, cur)) {
      // 同じ時刻なら「通知済み」を引き継ぐ（時刻が変わったら、また通知する）
      rec.notified = !rec.deleted && cur.eventAt === rec.eventAt ? cur.notified : 0
      map.set(rec.key, rec)
      dirty.add(rec.key)
    } else if (!rec.deleted && !cur.deleted && rec.updatedAt === cur.updatedAt && rec.eventAt === cur.eventAt && cur.notify !== rec.notify) {
      cur.notify = rec.notify // 内容が同じなら、通知する／しないの設定だけ最新の送信を反映
      dirty.add(rec.key)
    }
  })

  const stmts = []
  const out = []
  for (const [k, r] of Array.from(map.entries())) {
    if (r.deleted && now - r.updatedAt > TOMB_KEEP_MS) {
      stmts.push(env.DB.prepare('DELETE FROM items WHERE user_id=? AND channel=? AND item_key=?').bind(dataUser, channel, k))
      continue
    }
    out.push(r)
    if (dirty.has(k)) stmts.push(upsertStmt(env, dataUser, channel, r))
  }
  if (stmts.length) await runBatch(env, stmts)

  const res = { ok: true, items: out.map(publicRec) }
  if (channel === 'fort' && Array.isArray(req.history)) await addHistory(env, dataUser, req, res, now)
  return res
}

/* ================= ルビー獲得記録の同期（追加と削除だけ・差分のみ） ================= */
function normalizeHist(h) {
  if (!h) return null
  const id = String(h.id || '').slice(0, 64)
  if (!id) return null
  if (h.deleted === true) return { id, deleted: true }
  const x = Number(h.x)
  const y = Number(h.y)
  const at = Number(h.at)
  if (FORT_TYPES.indexOf(h.type) === -1 || !Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(at)) return null
  return {
    id, type: h.type,
    entryId: String(h.entryId || '').slice(0, 64),
    label: String(h.label || '').slice(0, 50),
    x, y,
    result: h.result === 'taken' ? 'taken' : 'ruby',
    ruby: Number(h.ruby) || 0,
    at, deleted: false
  }
}

async function addHistory(env, userId, req, out, now) {
  const since = Number(req.historySince) || 0
  const { results } = await env.DB.prepare('SELECT id, deleted FROM history WHERE user_id=?').bind(userId).all()
  const cur = new Map(results.map((r) => [r.id, !!r.deleted]))
  const stmts = []
  const rejected = []

  asArr(req.history).slice(0, MAX_HIST).forEach((h) => {
    const rec = normalizeHist(h)
    if (!rec) return
    const exists = cur.has(rec.id)
    if (rec.deleted) {
      if (!exists) {
        stmts.push(env.DB.prepare('INSERT OR IGNORE INTO history (user_id, id, deleted, touched_at) VALUES (?,?,1,?)').bind(userId, rec.id, now))
        cur.set(rec.id, true)
      } else if (!cur.get(rec.id)) {
        stmts.push(env.DB.prepare('UPDATE history SET deleted=1, touched_at=? WHERE user_id=? AND id=?').bind(now, userId, rec.id))
        cur.set(rec.id, true)
      }
    } else if (!exists) {
      stmts.push(env.DB.prepare(
        'INSERT OR IGNORE INTO history (user_id, id, type, entry_id, label, x, y, result, ruby, at_ms, deleted, touched_at) VALUES (?,?,?,?,?,?,?,?,?,?,0,?)'
      ).bind(userId, rec.id, rec.type, rec.entryId, rec.label, rec.x, rec.y, rec.result, rec.ruby, rec.at, now))
      cur.set(rec.id, false)
    } else if (cur.get(rec.id)) {
      rejected.push(rec.id) // すでに他の端末で削除済み → 送った側にも削除させる
    }
  })
  stmts.push(env.DB.prepare('DELETE FROM history WHERE user_id=? AND deleted=1 AND touched_at<?').bind(userId, now - HIST_TOMB_KEEP_MS))
  await runBatch(env, stmts)

  const rows = await env.DB.prepare(
    'SELECT id, type, entry_id, label, x, y, result, ruby, at_ms, deleted FROM history WHERE user_id=? AND touched_at>=?'
  ).bind(userId, since).all()
  out.history = rows.results.map((r) => ({
    id: r.id, type: r.type, entryId: r.entry_id, label: r.label, x: r.x, y: r.y,
    result: r.result, ruby: r.ruby, at: r.at_ms, deleted: !!r.deleted
  }))
  out.historyDeleted = rejected
  out.historyNow = now
}

/* ================= 通知設定（要塞＝ユーザーごと／アクア＝共通） ================= */
const WEBHOOK_RE = /^https:\/\/(?:(?:canary|ptb)\.)?(?:discord|discordapp)\.com\/api\/(?:v\d+\/)?webhooks\/\d+\/[\w-]+$/
const isWebhook = (u) => typeof u === 'string' && WEBHOOK_RE.test(u)

function sanitizeSettings(channel, s) {
  s = s || {}
  const lead = Number(s.leadMin)
  const qs = Number(s.quietStart)
  const qe = Number(s.quietEnd)
  const types = {}
  CHANNEL_TYPES[channel].forEach((t) => { types[t] = !(s.types && s.types[t] === false) })
  const roleId = String(s.roleId || '').trim()
  return {
    leadMin: Number.isFinite(lead) ? Math.min(60, Math.max(0, Math.round(lead))) : 10,
    quietOn: s.quietOn === true,
    quietStart: Number.isFinite(qs) ? Math.min(23, Math.max(0, Math.round(qs))) : 0,
    quietEnd: Number.isFinite(qe) ? Math.min(23, Math.max(0, Math.round(qe))) : 7,
    digest: s.digest !== false,
    roleId: /^\d{5,25}$/.test(roleId) ? roleId : '',
    types
  }
}

async function loadSettings(env, dataUser, channel) {
  const row = await env.DB.prepare('SELECT json FROM settings WHERE user_id=? AND channel=?').bind(dataUser, channel).first()
  let s = {}
  if (row) { try { s = JSON.parse(row.json) } catch (e) { s = {} } }
  const st = sanitizeSettings(channel, s)
  st.webhookUrl = isWebhook(s.webhookUrl) ? s.webhookUrl : ''
  return st
}

async function saveSettings(env, dataUser, channel, st) {
  await env.DB.prepare(
    'INSERT INTO settings (user_id, channel, json) VALUES (?,?,?) ON CONFLICT(user_id, channel) DO UPDATE SET json=excluded.json'
  ).bind(dataUser, channel, JSON.stringify(st)).run()
}

/* Webhook URLは外に返さない（登録済みかどうかと、末尾6文字だけ返す） */
function settingsResponse(st) {
  const pub = { ...st }
  delete pub.webhookUrl
  return { ok: true, settings: pub, webhook: { set: !!st.webhookUrl, hint: st.webhookUrl ? '…' + st.webhookUrl.slice(-6) : '' } }
}

/* body.settings：通知設定（送られたときだけ更新）／ body.webhookUrl：''で解除・URLで登録（送られたときだけ更新） */
async function handleSettings(env, dataUser, channel, body) {
  const cur = await loadSettings(env, dataUser, channel)
  let next = cur
  let changed = false
  if (body.settings && typeof body.settings === 'object') {
    next = sanitizeSettings(channel, body.settings)
    next.webhookUrl = cur.webhookUrl
    changed = true
  }
  if (typeof body.webhookUrl === 'string') {
    const u = body.webhookUrl.trim()
    if (u === '') {
      next.webhookUrl = ''
    } else if (isWebhook(u)) {
      next.webhookUrl = u
    } else {
      return { ok: false, error: 'DiscordのWebhook URLの形式が正しくありません（https://discord.com/api/webhooks/… で始まるURLを入力してください）' }
    }
    changed = true
  }
  if (changed) await saveSettings(env, dataUser, channel, next)
  return settingsResponse(next)
}

/* ================= Discord ================= */
async function sendDiscord(url, text, roleId) {
  const chunks = []
  let cur = ''
  String(text).split('\n').forEach((line) => {
    if ((cur + line).length > 1800 && cur) { chunks.push(cur); cur = '' }
    cur += line + '\n'
  })
  if (cur) chunks.push(cur)
  let ok = true
  for (const c of chunks) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: c, allowed_mentions: { parse: [], roles: roleId ? [String(roleId)] : [] } })
      })
      if (!res.ok) {
        console.error('Discord HTTP ' + res.status + ': ' + (await res.text()))
        ok = false
      }
    } catch (e) {
      console.error('Discord通信エラー', e)
      ok = false
    }
  }
  return ok
}

/* テスト送信：body.webhookUrl があればそのURLへ（未保存のURLも試せる）、なければ登録済みのURLへ */
async function handleTest(env, dataUser, channel, userId, body) {
  const st = await loadSettings(env, dataUser, channel)
  let url = st.webhookUrl
  if (typeof body.webhookUrl === 'string' && body.webhookUrl.trim()) {
    if (!isWebhook(body.webhookUrl.trim())) {
      return { ok: false, error: 'DiscordのWebhook URLの形式が正しくありません（https://discord.com/api/webhooks/… で始まるURL）' }
    }
    url = body.webhookUrl.trim()
  }
  if (!url) return { ok: false, error: 'Webhook URLが未登録です。URLを入力してからテストしてください' }
  const label = channel === 'aqua' ? 'アクア用（共有）' : '要塞・ドラゴン用（ユーザー: ' + userId + '）'
  const ok = await sendDiscord(url, '✅ GGE Tracker の通知テストです — ' + label, '')
  return ok ? { ok: true } : { ok: false, error: 'Discordへの送信に失敗しました。Webhook URLが有効か確認してください' }
}

/* 通知しない時間帯か（開始＞終了なら日付をまたぐ。開始＝終了は「停止なし」） */
function isQuiet(st, hour) {
  if (!st.quietOn || st.quietStart === st.quietEnd) return false
  if (st.quietStart < st.quietEnd) return hour >= st.quietStart && hour < st.quietEnd
  return hour >= st.quietStart || hour < st.quietEnd
}

/* ================= 5分おきの通知チェック（Cron） ================= */
async function notifyCheck(env) {
  await ensureSchema(env)
  const now = Date.now()
  const horizon = now + 60 * 60000 // 「何分前」は最大60分

  const [itemsR, setR] = await Promise.all([
    env.DB.prepare(
      'SELECT user_id, channel, item_key, type, x, y, label, event_at FROM items WHERE deleted=0 AND notify=1 AND notified<>event_at AND event_at<=?'
    ).bind(horizon).all(),
    env.DB.prepare('SELECT user_id, channel, json FROM settings').all()
  ])
  if (!itemsR.results.length) return

  const settingsMap = new Map()
  setR.results.forEach((r) => {
    let s = {}
    try { s = JSON.parse(r.json) } catch (e) { s = {} }
    const st = sanitizeSettings(r.channel, s)
    st.webhookUrl = isWebhook(s.webhookUrl) ? s.webhookUrl : ''
    settingsMap.set(r.user_id + '|' + r.channel, st)
  })

  // ユーザー × チャンネルごとにまとめる（アクアは共通データなので全員で1グループ）
  const groups = new Map()
  itemsR.results.forEach((r) => {
    const k = r.user_id + '|' + r.channel
    if (!groups.has(k)) groups.set(k, { userId: r.user_id, channel: r.channel, rows: [] })
    groups.get(k).rows.push(r)
  })

  const hour = jstHour(now)

  for (const g of groups.values()) {
    const st = settingsMap.get(g.userId + '|' + g.channel)
    if (!st || !st.webhookUrl) continue // Webhook未登録なら通知しない
    if (isQuiet(st, hour)) continue // 通知停止中は保留（停止が明けてから判断する）
    const lead = st.leadMin * 60000
    const isAqua = g.channel === 'aqua'
    const verbSoon = isAqua ? '浮上します' : '攻撃可能になります'
    const verbNow = isAqua ? '浮上しました' : '攻撃可能になりました'

    g.rows.sort((a, b) => a.event_at - b.event_at)
    const dueLines = []
    const lateLines = []
    const touched = []

    g.rows.forEach((r) => {
      const left = r.event_at - now
      if (left > lead) return // まだ早い
      if (st.types[r.type] === false) { touched.push(r); return } // 通知オフの種類は、通知せずに処理済みにする

      const info = TYPE_INFO[r.type] || { name: r.type, icon: '📍' }
      const name = info.icon + ' ' + info.name + ' (' + r.x + ',' + r.y + ')' + (r.label ? ' ' + r.label : '')
      const when = jstHM(r.event_at)
      const late = -left // 時刻から何ms過ぎたか（未到達なら負）

      if (late <= GRACE_MIN * 60000) {
        dueLines.push(left > 0
          ? '⏰ ' + name + ' が約' + Math.ceil(left / 60000) + '分後に' + verbSoon + '（' + when + '）'
          : '🔔 ' + name + ' が' + verbNow + '（' + when + '）')
      } else if (st.digest && late <= DIGEST_MAX_AGE_H * 3600000) {
        lateLines.push('・' + name + '（' + when + '〜）')
      }
      touched.push(r)
    })

    if (!touched.length) continue

    const parts = []
    if (dueLines.length) parts.push(dueLines.join('\n'))
    if (lateLines.length) parts.push('🌙 通知停止中などに' + (isAqua ? '浮上' : '攻撃可能') + 'になっていた対象\n' + lateLines.join('\n'))
    if (parts.length) {
      const text = (st.roleId ? '<@&' + st.roleId + '>\n' : '') + parts.join('\n\n')
      if (!(await sendDiscord(st.webhookUrl, text, st.roleId))) {
        console.error('Discord送信に失敗しました。次回の実行で再試行します。')
        continue // 失敗時は「通知済み」にしない
      }
    }

    const stmts = touched.map((r) =>
      env.DB.prepare('UPDATE items SET notified=event_at WHERE user_id=? AND channel=? AND item_key=? AND event_at=?')
        .bind(g.userId, g.channel, r.item_key, r.event_at)
    )
    await runBatch(env, stmts)
  }
}
