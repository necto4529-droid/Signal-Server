const WebSocket = require('ws');
const http = require('http');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');
const { spawn } = require('child_process');
const os = require('os');
const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════════
// ── STEALTH-TRANSPORT ─────────────────────────────────────────────────────────
// Маскировка трафика под случайный бинарный мусор для обхода DPI/ТСПУ.
//
// Протокол фрейма (бинарный, каждое сообщение):
//   [4 байта] magic XOR'd  — всегда разные, не детектируемые
//   [2 байта] pad_len      — длина шума (little-endian, XOR'd)
//   [pad_len байт] padding — случайный мусор
//   [остаток] payload      — XOR-поток с rolling-ключом
//
// Ключ сессии: 32 случайных байта, согласуются при handshake через HTTP /stealth-init
// Rolling XOR: каждый байт payload XOR-ится с key[i % 32], где i — глобальный счётчик
// ═══════════════════════════════════════════════════════════════════════════════

const STEALTH_MAGIC = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF]); // маскируется XOR
const STEALTH_VERSION = 0x02;
const PAD_MIN = 8;
const PAD_MAX = 128;

// Генерация сессионного ключа
function generateSessionKey() {
  return crypto.randomBytes(32);
}

// XOR-поток: шифрует/дешифрует буфер с rolling-счётчиком
function xorStream(buf, key, counter) {
  const out = Buffer.alloc(buf.length);
  for (let i = 0; i < buf.length; i++) {
    // Добавляем counter для rolling-эффекта — каждый пакет уникален
    out[i] = buf[i] ^ key[(counter + i) % 32] ^ ((counter >> 8) & 0xFF) ^ (i & 0xFF);
  }
  return out;
}

// Упаковать JSON-объект в Stealth-фрейм
function stealthEncode(obj, key, counter) {
  const jsonBuf = Buffer.from(JSON.stringify(obj), 'utf8');

  // Случайный padding
  const padLen = PAD_MIN + Math.floor(Math.random() * (PAD_MAX - PAD_MIN));
  const padding = crypto.randomBytes(padLen);

  // Маскируем magic XOR с первыми байтами ключа
  const magic = Buffer.alloc(4);
  for (let i = 0; i < 4; i++) magic[i] = STEALTH_MAGIC[i] ^ key[i] ^ STEALTH_VERSION;

  // Длина padding (2 байта LE), тоже XOR'd
  const padLenBuf = Buffer.alloc(2);
  padLenBuf.writeUInt16LE(padLen, 0);
  padLenBuf[0] ^= key[4];
  padLenBuf[1] ^= key[5];

  // Шифруем payload XOR-потоком
  const encPayload = xorStream(jsonBuf, key, counter);

  return Buffer.concat([magic, padLenBuf, padding, encPayload]);
}

// Распаковать Stealth-фрейм обратно в объект
function stealthDecode(buf, key, counter) {
  if (buf.length < 6) return null;

  // Проверяем magic
  for (let i = 0; i < 4; i++) {
    if (buf[i] !== (STEALTH_MAGIC[i] ^ key[i] ^ STEALTH_VERSION)) return null;
  }

  // Читаем длину padding
  const padLenBuf = Buffer.from([buf[4] ^ key[4], buf[5] ^ key[5]]);
  const padLen = padLenBuf.readUInt16LE(0);

  if (buf.length < 6 + padLen) return null;

  // Расшифровываем payload
  const encPayload = buf.slice(6 + padLen);
  const jsonBuf = xorStream(encPayload, key, counter);

  try {
    return JSON.parse(jsonBuf.toString('utf8'));
  } catch {
    return null;
  }
}

// Хранилище сессионных ключей: sessionId → { key, rxCounter, txCounter, createdAt }
const stealthSessions = new Map();

// Очистка устаревших сессий (старше 24 часов)
setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const [sid, sess] of stealthSessions) {
    if (sess.createdAt < cutoff) stealthSessions.delete(sid);
  }
}, 60 * 60 * 1000);

// ─── SQLite WAL ───────────────────────────────────────────────────────────────
const dbPath = path.join(__dirname, 'offline_queue.db');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('cache_size = -16000');
db.pragma('temp_store = MEMORY');
db.pragma('mmap_size = 268435456'); // 256 МБ memory-mapped I/O

db.exec(`
  CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id TEXT NOT NULL,
    type         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    created_at   INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_recipient ON events (recipient_id);

  CREATE TABLE IF NOT EXISTS groups (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    creator_id  TEXT NOT NULL,
    invite_code TEXT UNIQUE NOT NULL,
    avatar      TEXT DEFAULT '👥',
    description TEXT DEFAULT '',
    members     TEXT DEFAULT '[]',
    created_at  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS file_headers (
    file_id      TEXT PRIMARY KEY,
    sender_id    TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    name         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    mime_type    TEXT NOT NULL,
    total_chunks INTEGER NOT NULL,
    caption      TEXT DEFAULT '',
    thumb        TEXT DEFAULT '',
    ts           INTEGER NOT NULL,
    created_at   INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_file_headers_recipient ON file_headers (recipient_id);

  CREATE TABLE IF NOT EXISTS file_chunks (
    file_id     TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    data        TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (file_id, chunk_index)
  );
  CREATE INDEX IF NOT EXISTS idx_file_chunks_file ON file_chunks (file_id);
`);

try {
  db.exec(`ALTER TABLE file_headers ADD COLUMN thumb TEXT DEFAULT ''`);
} catch(e) {}

// ─── Подготовленные запросы ───────────────────────────────────────────────────
const stmtInsertEvent     = db.prepare(`INSERT INTO events (recipient_id,type,payload,created_at) VALUES (?,?,?,?)`);
const stmtGetEvents       = db.prepare(`SELECT * FROM events WHERE recipient_id=? ORDER BY created_at`);
const stmtDeleteEvent     = db.prepare(`DELETE FROM events WHERE id=?`);
const stmtAckMsg          = db.prepare(`
  SELECT id, json_extract(payload,'$.from') AS sender
  FROM events
  WHERE recipient_id=? AND type='incoming-msg' AND json_extract(payload,'$.msgId')=?
  LIMIT 1
`);
const stmtDeleteById      = db.prepare(`DELETE FROM events WHERE id=?`);

const stmtInsertHeader    = db.prepare(`
  INSERT OR REPLACE INTO file_headers
    (file_id, sender_id, recipient_id, name, size, mime_type, total_chunks, caption, thumb, ts, created_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
`);
const stmtInsertChunk     = db.prepare(`
  INSERT OR REPLACE INTO file_chunks (file_id, chunk_index, data, created_at)
  VALUES (?,?,?,?)
`);
const stmtGetHeader       = db.prepare(`SELECT * FROM file_headers WHERE file_id=?`);
const stmtCountChunks     = db.prepare(`SELECT COUNT(*) as cnt FROM file_chunks WHERE file_id=?`);
const stmtDeleteChunks    = db.prepare(`DELETE FROM file_chunks WHERE file_id=?`);
const stmtDeleteHeader    = db.prepare(`DELETE FROM file_headers WHERE file_id=?`);
const stmtGetPendingFiles = db.prepare(`SELECT * FROM file_headers WHERE recipient_id=?`);

const stmtDeleteFileAvail = db.prepare(`
  DELETE FROM events
  WHERE recipient_id=? AND type='file-available'
    AND json_extract(payload,'$.fileId')=?
`);

// ─── Push ─────────────────────────────────────────────────────────────────────
const pushFile = path.join(__dirname, 'push_subscriptions.json');
let pushSubs = {};
try { if(fs.existsSync(pushFile)) pushSubs = JSON.parse(fs.readFileSync(pushFile, 'utf8')); } catch(e) {}

const ONESIGNAL_APP_ID = 'c5b0ecd0-3e67-47a0-823d-771a7c4de3be';
const ONESIGNAL_KEY    = 'os_v2_app_ywyozub6m5d2bar5o4nhytpdxzr72sz2khuemruxqbapncfalaxcwfqlqoxvcenyxr6sa5uvelsbqwpwrwihgdpwn4ectomaup5byuq';

async function sendPush(userId, message) {
  const playerId = pushSubs[userId];
  if(!playerId) return;
  try {
    await fetch('https://api.onesignal.com/notifications', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Basic ${ONESIGNAL_KEY}` },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_player_ids: [playerId],
        contents: { en: message },
        headings: { en: 'K-Chat' }
      })
    });
  } catch(e) {}
}

// ─── АУДИО ОБРАБОТКА (FFmpeg) ─────────────────────────────────────────────────
async function optimizeAudioWithFFmpeg(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
      const ffmpegArgs = [
        '-i', inputPath,
        '-c:a', 'libopus',
        '-b:a', '192k',
        '-vbr', 'on',
        '-compression_level', '10',
        '-ar', '48000',
        '-ac', '1',
        '-application', 'audio',
        '-filter_complex', 'afftdn=nr=5:nf=-20:rn=0.005:rf=0.005,adeclick,acompressor=ratio=2:attack=3:release=30:threshold=-18:detection=peak,loudnorm=I=-16:TP=-1.5:LRA=11',
        '-y',
        outputPath
      ];
    const ffmpeg = spawn('ffmpeg', ffmpegArgs);
    let stderr = '';
    ffmpeg.stderr.on('data', (data) => { stderr += data.toString(); });
    ffmpeg.on('close', (code) => {
      if (code === 0) resolve(outputPath);
      else reject(new Error(`FFmpeg error: ${stderr}`));
    });
    ffmpeg.on('error', reject);
  });
}

async function optimizeVideoWithFFmpeg(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const ffmpegArgs = [
      '-i', inputPath,
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-profile:v', 'main',
      '-crf', '23',
      '-g', '30',
      '-keyint_min', '30',
      '-sc_threshold', '0',
      '-r', '30',
      '-b:v', '400k',
      '-maxrate', '600k',
      '-bufsize', '800k',
      '-pix_fmt', 'yuv420p',
      '-movflags', '+faststart',
      '-c:a', 'libopus',
      '-b:a', '128k',
      '-ar', '48000',
      '-ac', '1',
      '-filter:v', 'fps=fps=30:round=near,scale=640:640:force_original_aspect_ratio=increase,crop=640:640',
      '-y',
      outputPath
    ];
    const ffmpeg = spawn('ffmpeg', ffmpegArgs);
    let stderr = '';
    ffmpeg.stderr.on('data', (data) => { stderr += data.toString(); });
    ffmpeg.on('close', (code) => {
      if (code === 0) resolve(outputPath);
      else reject(new Error(`FFmpeg error: ${stderr}`));
    });
    ffmpeg.on('error', reject);
  });
}

async function saveTemporaryFile(data, ext = '.webm') {
  const tempDir = path.join(os.tmpdir(), 'kchat-audio');
  if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
  const filename = `audio-${Date.now()}-${Math.random().toString(36).substr(2, 9)}${ext}`;
  const filepath = path.join(tempDir, filename);
  
  if (typeof data === 'string' && data.startsWith('data:')) {
    const base64 = data.replace(/^data:[^,]+,/, '');
    fs.writeFileSync(filepath, Buffer.from(base64, 'base64'));
  } else {
    fs.writeFileSync(filepath, data);
  }
  return filepath;
}

function cleanupTemporaryFile(filepath) {
  try { if (fs.existsSync(filepath)) fs.unlinkSync(filepath); } catch (e) {}
}

async function processVoiceMessage(voiceData) {
  let inputPath, outputPath;
  try {
    inputPath = await saveTemporaryFile(voiceData, '.webm');
    outputPath = inputPath.replace('.webm', '-optimized.webm');
    await optimizeAudioWithFFmpeg(inputPath, outputPath);
    const optimizedData = fs.readFileSync(outputPath);
    const originalSize = fs.statSync(inputPath).size;
    const optimizedSize = fs.statSync(outputPath).size;
    const compression = ((1 - optimizedSize / originalSize) * 100).toFixed(1);
    
    cleanupTemporaryFile(inputPath);
    cleanupTemporaryFile(outputPath);
    
    return {
      data: `data:audio/webm;base64,${optimizedData.toString('base64')}`,
      size: optimizedSize,
      compression: compression
    };
  } catch (error) {
    if(inputPath) cleanupTemporaryFile(inputPath);
    if(outputPath) cleanupTemporaryFile(outputPath);
    return { data: voiceData, size: 0, compression: 0 };
  }
}

// ─── Очистка старых файлов ────────────────────────────────────────────────────
function cleanupOldFiles() {
  const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
  const old = db.prepare(`SELECT file_id FROM file_headers WHERE created_at < ?`).all(cutoff);
  for(const { file_id } of old) {
    stmtDeleteChunks.run(file_id);
    stmtDeleteHeader.run(file_id);
    db.prepare(`DELETE FROM events WHERE type='file-available' AND json_extract(payload,'$.fileId')=?`).run(file_id);
  }
  if(old.length > 0) console.log('[Cleanup] Удалены старые файлы');
}
setInterval(cleanupOldFiles, 60 * 60 * 1000);
cleanupOldFiles();

// ─── WebSocket ────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const HEALTH_PORT = process.env.HEALTH_PORT || 8080;
const MAX_CONNS_PER_IP = 8;

// ═══════════════════════════════════════════════════════════════════════════════
// ── HTTP-СЕРВЕР (совмещает Health-check + Stealth HTTP Fallback) ──────────────
// Render требует один HTTP-порт. Мы поднимаем HTTP на HEALTH_PORT,
// а WebSocket — на PORT. HTTP-сервер обрабатывает:
//   GET  /health            — health-check для Render
//   GET  /stats             — статистика (с токеном)
//   POST /stealth-init      — выдача сессионного ключа клиенту
//   POST /stealth-poll      — HTTP fallback: клиент шлёт пакет, получает ответы
// ═══════════════════════════════════════════════════════════════════════════════

const STATS_TOKEN = process.env.STATS_TOKEN || '';

// Буфер HTTP-fallback сообщений: peerId → [{encoded, ts}]
// Сервер складывает сюда исходящие сообщения для клиентов без WS
const httpFallbackQueues = new Map();

function httpFallbackEnqueue(peerId, obj) {
  if (!httpFallbackQueues.has(peerId)) httpFallbackQueues.set(peerId, []);
  const q = httpFallbackQueues.get(peerId);
  q.push({ obj, ts: Date.now() });
  // Не копим больше 200 сообщений
  if (q.length > 200) q.splice(0, q.length - 200);
}

// Очистка старых HTTP-fallback буферов (старше 5 минут)
setInterval(() => {
  const cutoff = Date.now() - 5 * 60 * 1000;
  for (const [pid, q] of httpFallbackQueues) {
    const filtered = q.filter(x => x.ts > cutoff);
    if (filtered.length === 0) httpFallbackQueues.delete(pid);
    else httpFallbackQueues.set(pid, filtered);
  }
}, 60 * 1000);

// Читаем тело HTTP-запроса как Buffer
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

const healthServer = http.createServer(async (req, res) => {
  // ── CORS для браузера ──
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Session-Id, X-Seq');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // ── Health-check ──
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    // Имитируем обычный веб-сервер
    res.end('OK');
    return;
  }

  // ── Stats ──
  if (req.url === '/stats' && STATS_TOKEN) {
    const auth = req.headers['authorization'] || '';
    if (auth !== `Bearer ${STATS_TOKEN}`) {
      res.writeHead(403);
      return res.end('Forbidden');
    }
    const eventCount = db.prepare('SELECT COUNT(*) as cnt FROM events').get().cnt;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ activeConnections: peers.size, queuedEvents: eventCount }));
    return;
  }

  // ── Stealth Init: клиент запрашивает сессионный ключ ──
  // POST /stealth-init
  // Body: { "v": 2 }  (plain JSON — первый и единственный открытый запрос)
  // Response: { "sid": "...", "key": "<base64 32 bytes>", "ts": ... }
  // После этого весь трафик идёт через бинарный Stealth-фрейм
  if (req.url === '/stealth-init' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const parsed = JSON.parse(body.toString('utf8'));
      if (parsed.v !== 2) { res.writeHead(400); return res.end(); }

      const sid = crypto.randomBytes(16).toString('hex');
      const key = generateSessionKey();
      stealthSessions.set(sid, { key, rxCounter: 0, txCounter: 0, createdAt: Date.now() });

      // Имитируем ответ обычного API
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff'
      });
      res.end(JSON.stringify({ sid, key: key.toString('base64'), ts: Date.now() }));
    } catch(e) {
      res.writeHead(400);
      res.end();
    }
    return;
  }

  // ── Stealth Poll: HTTP fallback для заблокированных WS ──
  // POST /stealth-poll
  // Headers: X-Session-Id: <sid>, X-Seq: <counter>
  // Body: бинарный Stealth-фрейм с JSON-сообщением
  // Response: бинарный Stealth-фрейм (массив ответных сообщений)
  if (req.url === '/stealth-poll' && req.method === 'POST') {
    const sid = req.headers['x-session-id'];
    const seq = parseInt(req.headers['x-seq'] || '0', 10);
    const sess = stealthSessions.get(sid);

    if (!sess) {
      // Имитируем 404 обычного сайта
      res.writeHead(404, { 'Content-Type': 'text/html' });
      return res.end('<html><body>Not found</body></html>');
    }

    try {
      const body = await readBody(req);
      const decoded = stealthDecode(body, sess.key, seq);
      if (!decoded) {
        res.writeHead(400);
        return res.end();
      }

      // Обновляем счётчик приёма
      sess.rxCounter = seq + body.length;

      // Создаём виртуальный WS-объект для HTTP-клиентов
      // Он накапливает ответы в буфер вместо отправки по WS
      const responseBuffer = [];
      const virtualWs = {
        readyState: WebSocket.OPEN,
        _isHttpFallback: true,
        _peerId: null,
        _responseBuffer: responseBuffer,
        send(jsonStr) {
          try {
            const obj = JSON.parse(jsonStr);
            responseBuffer.push(obj);
          } catch(e) {}
        }
      };

      // Если у нас есть peerId из сессии — добавляем накопленные сообщения
      if (sess.peerId) {
        const q = httpFallbackQueues.get(sess.peerId) || [];
        for (const item of q) responseBuffer.push(item.obj);
        httpFallbackQueues.delete(sess.peerId);
      }

      // Обрабатываем входящее сообщение
      await handleMessage(virtualWs, decoded, sid);

      // Если зарегистрировался — запоминаем peerId в сессии
      if (virtualWs._peerId) sess.peerId = virtualWs._peerId;

      // Кодируем все ответы в один бинарный фрейм
      const responseJson = JSON.stringify(responseBuffer);
      const encoded = stealthEncode({ batch: responseBuffer }, sess.key, sess.txCounter);
      sess.txCounter += responseJson.length;

      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Content-Length': encoded.length,
        // Имитируем заголовки обычного бинарного API
        'X-Content-Type-Options': 'nosniff',
        'Cache-Control': 'no-store'
      });
      res.end(encoded);
    } catch(e) {
      console.error('[Stealth-Poll] Error:', e.message);
      res.writeHead(500);
      res.end();
    }
    return;
  }

  // Всё остальное — имитируем обычный веб-сервер (404)
  res.writeHead(404, { 'Content-Type': 'text/html' });
  res.end('<html><head><title>404 Not Found</title></head><body><h1>Not Found</h1></body></html>');
});

healthServer.listen(HEALTH_PORT, () => {
  console.log(`[Health+Stealth] listening on port ${HEALTH_PORT}`);
});

// ─── WebSocket Server ─────────────────────────────────────────────────────────
const wss = new WebSocket.Server({
  port: PORT,
  maxPayload: 1024 * 1024 * 1024,  // 1 ГБ
  verifyClient: (info) => {
    const ip = info.req.socket.remoteAddress || 'unknown';
    let count = 0;
    for (const [, ws] of peers) {
      if (ws._socket && (ws._socket.remoteAddress || '') === ip) count++;
    }
    if (count >= MAX_CONNS_PER_IP) {
      console.log(`[WARN] Too many connections from ${ip}`);
      return false;
    }
    return true;
  }
});

const peers = new Map();
const activeFetchers = new Map();
const heartbeats = new Map();
const HEARTBEAT_TIMEOUT = 600_000;   // 10 минут

// Rate limiting
const rateLimits = new Map();
function checkRateLimit(ws) {
  const now = Date.now();
  const window = 1000;
  let entry = rateLimits.get(ws);
  if (!entry) {
    entry = { count: 1, start: now };
    rateLimits.set(ws, entry);
    return true;
  }
  if (now - entry.start > window) {
    entry.count = 1;
    entry.start = now;
    return true;
  }
  if (entry.count >= 30) return false;
  entry.count++;
  return true;
}

setInterval(() => {
  const now = Date.now();
  for (const [ws, entry] of rateLimits) {
    if (now - entry.start > 5000) rateLimits.delete(ws);
  }
}, 10000);

const priorityQueues = new Map();

// ═══════════════════════════════════════════════════════════════════════════════
// ── SEND: отправка с Stealth-кодированием ────────────────────────────────────
// Если у WS-клиента есть сессионный ключ — отправляем бинарный фрейм.
// Иначе — обычный JSON (обратная совместимость).
// ═══════════════════════════════════════════════════════════════════════════════
function send(ws, obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  // HTTP fallback — просто добавляем в буфер
  if (ws._isHttpFallback) {
    ws.send(JSON.stringify(obj));
    return;
  }

  // Stealth-режим: бинарный фрейм
  if (ws._stealthKey) {
    try {
      const frame = stealthEncode(obj, ws._stealthKey, ws._stealthTxCounter);
      ws._stealthTxCounter += JSON.stringify(obj).length;
      ws.send(frame);
    } catch(e) {
      // Fallback на JSON если что-то пошло не так
      ws.send(JSON.stringify(obj));
    }
    return;
  }

  // Обычный JSON (клиент без Stealth)
  ws.send(JSON.stringify(obj));
}

function flushPriorityQueue(userId) {
  const ws = peers.get(userId);
  if(!ws || ws.readyState !== WebSocket.OPEN) return;
  const queue = priorityQueues.get(userId) || [];
  while(queue.length > 0) {
    const msg = queue.shift();
    try { send(ws, msg); } catch(e) { break; }
  }
  if(queue.length === 0) priorityQueues.delete(userId);
  else priorityQueues.set(userId, queue);
}

function broadcastPresence(peerId, isOnline) {
  const msg = { type: 'presence', peerId, online: isOnline };
  for(const [, ws] of peers) send(ws, msg);
}

function resetHeartbeat(peerId) {
  if(heartbeats.has(peerId)) clearTimeout(heartbeats.get(peerId));
  heartbeats.set(peerId, setTimeout(() => { peers.get(peerId)?.close(); }, HEARTBEAT_TIMEOUT));
}

function enqueueEvent(recipientId, type, payload) {
  return stmtInsertEvent.run(recipientId, type, JSON.stringify(payload), Date.now()).lastInsertRowid;
}

// ─── Группы ──────────────────────────────────────────────────────────────────
const stmtGetGroup        = db.prepare('SELECT * FROM groups WHERE id=?');
const stmtGetGroupInvite  = db.prepare('SELECT * FROM groups WHERE invite_code=?');
const stmtCreateGroup     = db.prepare('INSERT INTO groups (id,name,creator_id,invite_code,avatar,description,members,created_at) VALUES (?,?,?,?,?,?,?,?)');
const stmtUpdateGroupMbrs = db.prepare('UPDATE groups SET members=? WHERE id=?');
const stmtUpdateGroup     = db.prepare('UPDATE groups SET name=?,avatar=?,description=?,members=? WHERE id=?');
const getGroup            = id   => stmtGetGroup.get(id);
const getGroupInvite      = code => stmtGetGroupInvite.get(code);

function addMember(groupId, peerId) {
  const g = getGroup(groupId); if(!g) return false;
  const members = JSON.parse(g.members);
  if(members.includes(peerId) || members.length >= 20) return false;
  members.push(peerId);
  stmtUpdateGroupMbrs.run(JSON.stringify(members), groupId);
  return true;
}
function removeMember(groupId, peerId) {
  const g = getGroup(groupId); if(!g) return;
  stmtUpdateGroupMbrs.run(JSON.stringify(JSON.parse(g.members).filter(id => id !== peerId)), groupId);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── ОБРАБОТЧИК СООБЩЕНИЙ (общий для WS и HTTP fallback) ──────────────────────
// ═══════════════════════════════════════════════════════════════════════════════
async function handleMessage(ws, data, sessionId) {
  // Для WS-клиентов rate-limit проверяется в onmessage
  // Для HTTP-fallback — проверяем отдельно (лимит мягче: 60/сек)

  // Получаем myId из контекста ws
  let myId = ws._myId || null;

  if(myId) resetHeartbeat(myId);

  // ── Stealth-handshake: клиент сообщает свой sessionId ──
  // { type: 'stealth-hello', sid: '...' }
  // После этого все исходящие сообщения этому WS шифруются
  if(data.type === 'stealth-hello') {
    const sid = data.sid;
    const sess = stealthSessions.get(sid);
    if(sess) {
      ws._stealthKey = sess.key;
      ws._stealthTxCounter = 0;
      ws._stealthRxCounter = 0;
      ws._stealthSid = sid;
      console.log(`[Stealth] WS client activated stealth mode, sid=${sid}`);
    }
    send(ws, { type: 'stealth-ack', ok: true });
    return;
  }

  // ── Регистрация ──────────────────────────────────────────────────────────
  if(data.type === 'register') {
    const newId = (data.peerId || '').toLowerCase().trim();
    if(!newId) return;
    const old = peers.get(newId);
    if(old && old !== ws && old.readyState === WebSocket.OPEN) old.close();
    myId = newId;
    ws._myId = myId;
    peers.set(myId, ws);
    send(ws, { type: 'registered' });
    broadcastPresence(myId, true);
    resetHeartbeat(myId);

    flushPriorityQueue(myId);

    const rows = stmtGetEvents.all(myId);
    const events = rows.map(r => ({
      id: r.id,
      type: r.type,
      payload: JSON.parse(r.payload)
    }));
    for(const ev of events) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });

    const pendingFiles = stmtGetPendingFiles.all(myId);
    for(const h of pendingFiles) {
      const cnt = stmtCountChunks.get(h.file_id);
      if(cnt && cnt.cnt >= 1) {
        send(ws, {
          type: 'file-available',
          fileId: h.file_id,
          senderId: h.sender_id,
          name: h.name,
          size: h.size,
          mimeType: h.mime_type,
          totalChunks: h.total_chunks,
          caption: h.caption,
          thumb: h.thumb || '',
          ts: h.ts,
          chunksReady: cnt.cnt
        });
      }
    }
    return;
  }

  if(data.type === 'ping') { return; }

  if(data.type === 'register-push') {
    if(!myId || !data.playerId) return;
    pushSubs[myId] = data.playerId;
    try { fs.writeFileSync(pushFile, JSON.stringify(pushSubs)); } catch(e) {}
    return;
  }

  // ── ОБРАБОТКА ГОЛОСОВЫХ СООБЩЕНИЙ ─────────────────────────────────────────
  if(data.type === 'send-msg' && data.payload && data.payload.startsWith('data:audio')) {
    try {
      const processed = await processVoiceMessage(data.payload);
      data.payload = processed.data;
      console.log(`[Voice] Оптимизировано голосовое сообщение (сжато на ${processed.compression}%)`);
    } catch (error) {
      console.error('Ошибка при обработке голосового:', error);
    }
  }

  // ── file-available-ack ────────────────────────────────────────────────
  if(data.type === 'file-available-ack') {
    if(!myId) return;
    const header = stmtGetHeader.get(data.fileId);
    if(header && header.recipient_id === myId) {
      const cnt = stmtCountChunks.get(data.fileId);
      if(cnt && cnt.cnt >= header.total_chunks) {
        const deliveryPayload = { fileId: data.fileId, by: myId };
        const eventId = enqueueEvent(header.sender_id, 'file-delivered', deliveryPayload);
        const senderWs = peers.get(header.sender_id);
        if(senderWs) send(senderWs, { type: 'file-delivered', ...deliveryPayload, eventId });
      }
    }
    return;
  }

  // ── ФАЙЛОВЫЙ ПРОТОКОЛ ─────────────────────────────────────────────────
  if(data.type === 'store-file-header') {
    if(!myId) return;
    const recipient = (data.recipientId || '').toLowerCase();
    if(!recipient) return;
    stmtInsertHeader.run(
      data.fileId, myId, recipient, data.name, data.size,
      data.mimeType, data.totalChunks, data.caption || '',
      data.thumb || '',
      data.ts || Date.now(), Date.now()
    );
    send(ws, { type: 'store-file-header-ack', fileId: data.fileId });
    return;
  }

  if(data.type === 'store-chunks') {
    if(!myId) return;
    const chunks = data.chunks;
    if(!Array.isArray(chunks)) return;

    const header = stmtGetHeader.get(data.fileId);
    if(!header) return;

    for(const chunk of chunks) {
      stmtInsertChunk.run(data.fileId, chunk.index, chunk.data, Date.now());
    }

    const cnt = stmtCountChunks.get(data.fileId);
    const receivedCount = cnt ? cnt.cnt : 0;

    if(receivedCount === chunks.length && receivedCount >= 1) {
      const payload = {
        fileId: data.fileId,
        senderId: myId,
        name: header.name,
        size: header.size,
        mimeType: header.mime_type,
        totalChunks: header.total_chunks,
        caption: header.caption,
        thumb: header.thumb || '',
        ts: header.ts,
        chunksReady: receivedCount
      };

      const recipientWs = peers.get(header.recipient_id);

      const alreadyNotified = db.prepare(
        `SELECT id FROM events WHERE recipient_id=? AND type='file-available' AND json_extract(payload,'$.fileId')=? LIMIT 1`
      ).get(header.recipient_id, data.fileId);

      if(!alreadyNotified) {
        const eventId = enqueueEvent(header.recipient_id, 'file-available', payload);
        if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          flushPriorityQueue(header.recipient_id);
          send(recipientWs, { type: 'file-available', ...payload, eventId });
        } else {
          // HTTP fallback: кладём в буфер
          httpFallbackEnqueue(header.recipient_id, { type: 'file-available', ...payload, eventId });
          const queue = priorityQueues.get(header.recipient_id) || [];
          queue.push({ type: 'file-available', ...payload, eventId });
          priorityQueues.set(header.recipient_id, queue);
          sendPush(header.recipient_id, `📎 ${header.name}`);
        }
      } else {
        if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          send(recipientWs, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
        } else {
          httpFallbackEnqueue(header.recipient_id, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
        }
      }
    } else if(receivedCount > chunks.length) {
      const recipientWs = peers.get(header.recipient_id);
      if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
        send(recipientWs, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
      } else {
        httpFallbackEnqueue(header.recipient_id, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
      }
    }

    if(receivedCount >= header.total_chunks) {
      if (header.name.includes('__vnote__')) {
        (async () => {
          let inputPath, outputPath;
          try {
            const chunks = db.prepare(`SELECT * FROM file_chunks WHERE file_id=? ORDER BY chunk_index`).all(data.fileId);
            const buffer = Buffer.concat(chunks.map(c => Buffer.from(c.data, 'base64')));
            inputPath = await saveTemporaryFile(buffer, '.webm');
            outputPath = inputPath.replace('.webm', '-optimized.webm');
            
            await optimizeVideoWithFFmpeg(inputPath, outputPath);
            
            const optimizedData = fs.readFileSync(outputPath);
            const newTotalChunks = Math.ceil(optimizedData.length / (256 * 1024));
            
            db.transaction(() => {
              stmtDeleteChunks.run(data.fileId);
              db.prepare(`UPDATE file_headers SET size=?, total_chunks=? WHERE file_id=?`).run(optimizedData.length, newTotalChunks, data.fileId);
              for (let i = 0; i < newTotalChunks; i++) {
                const start = i * (256 * 1024);
                const chunk = optimizedData.slice(start, start + (256 * 1024));
                stmtInsertChunk.run(data.fileId, i, chunk.toString('base64'), Date.now());
              }
            })();
            
            const updatedHeader = stmtGetHeader.get(data.fileId);
            const payload = {
              fileId: data.fileId,
              senderId: header.sender_id,
              name: updatedHeader.name,
              size: updatedHeader.size,
              mimeType: updatedHeader.mime_type,
              totalChunks: updatedHeader.total_chunks,
              caption: updatedHeader.caption,
              thumb: updatedHeader.thumb || '',
              ts: updatedHeader.ts,
              chunksReady: updatedHeader.total_chunks
            };
            
            stmtDeleteFileAvail.run(updatedHeader.recipient_id, data.fileId);
            const eventId = enqueueEvent(updatedHeader.recipient_id, 'file-available', payload);
            const recipientWs = peers.get(updatedHeader.recipient_id);
            if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
              send(recipientWs, { type: 'file-available', ...payload, eventId });
            } else {
              httpFallbackEnqueue(updatedHeader.recipient_id, { type: 'file-available', ...payload, eventId });
            }
            send(ws, { type: 'file-upload-complete', fileId: data.fileId });
          } catch (e) {
            console.error('[VN-Optimize] Error:', e);
            const updHeader = stmtGetHeader.get(data.fileId);
            const payload = { fileId: data.fileId, senderId: header.sender_id, name: header.name, size: header.size, mimeType: header.mime_type, totalChunks: header.total_chunks, caption: header.caption, thumb: header.thumb||'', ts: header.ts, chunksReady: header.total_chunks };
            stmtDeleteFileAvail.run(header.recipient_id, data.fileId);
            const eventId = enqueueEvent(header.recipient_id, 'file-available', payload);
            const recipientWs = peers.get(header.recipient_id);
            if(recipientWs && recipientWs.readyState === WebSocket.OPEN) send(recipientWs, { type: 'file-available', ...payload, eventId });
            else httpFallbackEnqueue(header.recipient_id, { type: 'file-available', ...payload, eventId });
            send(ws, { type: 'file-upload-complete', fileId: data.fileId });
          } finally {
            cleanupTemporaryFile(inputPath);
            cleanupTemporaryFile(outputPath);
          }
        })();
        return;
      }

      const fetchers = activeFetchers.get(data.fileId);
      if (fetchers) {
        data.chunks.forEach(c => {
          const chunkMsg = {
            type: 'file-data-chunk',
            fileId: data.fileId,
            index: c.index,
            total: header.total_chunks,
            data: c.data
          };
          fetchers.forEach(fws => {
            if (fws.readyState === WebSocket.OPEN) send(fws, chunkMsg);
          });
        });
      }

      const recipientWs = peers.get(header.recipient_id);
      if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
        send(recipientWs, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
      } else {
        httpFallbackEnqueue(header.recipient_id, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
      }

      send(ws, { type: 'file-upload-complete', fileId: data.fileId });
      
      setTimeout(() => {
        const recipientWs = peers.get(header.recipient_id);
        if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          send(recipientWs, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: header.total_chunks, totalChunks: header.total_chunks });
        }
      }, 100);
    } else {
      send(ws, { type: 'store-chunks-ack', fileId: data.fileId });
    }
    return;
  }

  if(data.type === 'fetch-file') {
    if(!myId) return;
    const fileId = data.fileId;
    const fromIndex = data.fromIndex || 0;
    const header = stmtGetHeader.get(fileId);
    if(!header) { send(ws, { type: 'file-fetch-error', fileId, msg: 'File not found' }); return; }
    if(header.recipient_id !== myId) { send(ws, { type: 'file-fetch-error', fileId, msg: 'Access denied' }); return; }

    if (!activeFetchers.has(fileId)) activeFetchers.set(fileId, new Set());
    activeFetchers.get(fileId).add(ws);

    const chunks = db.prepare(`SELECT * FROM file_chunks WHERE file_id=? ORDER BY chunk_index`).all(fileId);
    
    send(ws, {
      type: 'file-data-header',
      fileId: fileId,
      senderId: header.sender_id,
      name: header.name,
      size: header.size,
      mimeType: header.mime_type,
      totalChunks: header.total_chunks,
      caption: header.caption,
      thumb: header.thumb || '',
      ts: header.ts,
      fromIndex: fromIndex,
      chunksReady: chunks.length
    });

    for(const chunk of chunks) {
      if(chunk.chunk_index >= fromIndex) {
        send(ws, { type: 'file-data-chunk', fileId, index: chunk.chunk_index, data: chunk.data });
      }
    }
    return;
  }

  if(data.type === 'ack-file') {
    if(!myId) return;
    const header = stmtGetHeader.get(data.fileId);
    if(header) {
      if(header.recipient_id === myId) {
        const cnt = stmtCountChunks.get(data.fileId);
        if(cnt && cnt.cnt >= header.total_chunks) {
          stmtDeleteChunks.run(data.fileId);
          stmtDeleteHeader.run(data.fileId);
          stmtDeleteFileAvail.run(myId, data.fileId);
          console.log(`[File] Deleted ${data.fileId} after recipient ack`);
        } else {
          console.log(`[File] Recipient sent ack for ${data.fileId}, but file is still uploading (${cnt?.cnt}/${header.total_chunks}). Keeping for now.`);
        }
      } else if(header.sender_id === myId) {
        console.log(`[File] Received ack from sender for ${data.fileId}, keeping file`);
      }
    }
    return;
  }

  if(data.type === 'send-msg') {
    if(!myId) return;
    const target = (data.target || '').toLowerCase();
    if(!target) return;
    const msgId = data.msgId;
    const payload = data.payload;
    const eventId = enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
    const targetWs = peers.get(target);
    if(targetWs && targetWs.readyState === WebSocket.OPEN) {
      send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload, eventId });
    } else {
      // HTTP fallback: кладём в буфер
      httpFallbackEnqueue(target, { type: 'incoming-msg', from: myId, msgId, payload, eventId });
      const queue = priorityQueues.get(target) || [];
      queue.push({ type: 'incoming-msg', from: myId, msgId, payload, eventId });
      priorityQueues.set(target, queue);
      sendPush(target, `💬 ${myId}`);
    }
    return;
  }

  if(data.type === 'ack-msg') {
    if(!myId || !data.msgId) return;
    const row = stmtAckMsg.get(myId, data.msgId);
    if(!row) return;
    stmtDeleteById.run(row.id);
    if(!row.sender) return;
    const deliveryPayload = { msgId: data.msgId, by: myId };
    const deliveredEventId = enqueueEvent(row.sender, 'msg-delivered', deliveryPayload);
    const senderWs = peers.get(row.sender);
    if(senderWs && senderWs.readyState === WebSocket.OPEN) {
      send(senderWs, { type: 'msg-delivered', ...deliveryPayload, eventId: deliveredEventId });
    } else {
      httpFallbackEnqueue(row.sender, { type: 'msg-delivered', ...deliveryPayload, eventId: deliveredEventId });
    }
    return;
  }

  if(data.type === 'ack-event') {
    if(!myId) return;
    stmtDeleteEvent.run(data.eventId);
    return;
  }

  if(data.type === 'query-presence') {
    if(!myId) return;
    const target = (data.target || '').toLowerCase();
    send(ws, { type: 'presence-reply', target, online: peers.has(target) });
    return;
  }

  if(data.type === 'voice-listened') {
    if(!myId) return;
    const target = (data.target || '').toLowerCase();
    const eventId = enqueueEvent(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
    const tw = peers.get(target);
    if(tw) send(tw, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
    else httpFallbackEnqueue(target, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
    return;
  }

  if(data.type === 'vn-watched') {
    if(!myId) return;
    const target = (data.target || '').toLowerCase();
    if(!target || !data.vnMsgId) return;
    const eventPayload = { from: myId, vnMsgId: data.vnMsgId };
    const eventId = enqueueEvent(target, 'vn-watched', eventPayload);
    const tw = peers.get(target);
    if(tw) send(tw, { type: 'vn-watched', from: myId, vnMsgId: data.vnMsgId, eventId });
    else httpFallbackEnqueue(target, { type: 'vn-watched', from: myId, vnMsgId: data.vnMsgId, eventId });
    return;
  }

  // ── Группы ────────────────────────────────────────────────────────────
  if(data.type === 'create-group') {
    if(!myId) return;
    if(getGroupInvite(data.inviteCode) || getGroup(data.groupId)) {
      send(ws, { type: 'error', msg: 'Группа уже существует' }); return;
    }
    stmtCreateGroup.run(data.groupId, data.name, data.creator, data.inviteCode,
      data.avatar||'👥', data.description||'', JSON.stringify([data.creator]), Date.now());
    send(ws, { type: 'group-created', groupId: data.groupId });
    return;
  }
  if(data.type === 'group-info') {
    const g = getGroupInvite(data.inviteCode);
    if(!g) { send(ws, { type: 'error', msg: 'Группа не найдена' }); return; }
    send(ws, { type: 'group-info-reply', group: { groupId: g.id, name: g.name, avatar: g.avatar, description: g.description } });
    return;
  }
  if(data.type === 'join-group') {
    if(!myId) return;
    const g = getGroupInvite(data.inviteCode);
    if(!g) { send(ws, { type: 'error', msg: 'Группа не найдена' }); return; }
    if(JSON.parse(g.members).length >= 20) { send(ws, { type: 'error', msg: 'Группа переполнена' }); return; }
    if(!addMember(g.id, data.peerId)) { send(ws, { type: 'error', msg: 'Вы уже в группе' }); return; }
    const updated = getGroup(g.id);
    const members = JSON.parse(updated.members);
    send(ws, { type: 'group-joined', groupId: g.id, name: g.name, avatar: g.avatar,
      description: g.description, inviteCode: g.invite_code, members });
    members.forEach(mid => {
      if(mid === data.peerId) return;
      const ev = { type: 'group-updated', groupId: g.id, members };
      const tw = peers.get(mid);
      if(tw) send(tw, ev);
      else { enqueueEvent(mid, 'group-updated', ev); httpFallbackEnqueue(mid, ev); }
    });
    return;
  }
  if(data.type === 'group-update') {
    if(!myId) return;
    const g = getGroup(data.groupId);
    if(!g || g.creator_id !== myId) return;
    stmtUpdateGroup.run(data.changes.name||g.name, data.changes.avatar||g.avatar,
      data.changes.description||g.description,
      data.changes.members ? JSON.stringify(data.changes.members) : g.members, data.groupId);
    const updated = getGroup(data.groupId);
    JSON.parse(updated.members).forEach(mid => {
      const ev = { type: 'group-updated', groupId: data.groupId, changes: data.changes };
      const tw = peers.get(mid);
      if(tw) send(tw, ev);
      else { enqueueEvent(mid, 'group-updated', ev); httpFallbackEnqueue(mid, ev); }
    });
    return;
  }
  if(data.type === 'group-remove-member') {
    if(!myId) return;
    const g = getGroup(data.groupId);
    if(!g || g.creator_id !== myId || data.targetPeerId === myId) return;
    removeMember(data.groupId, data.targetPeerId);
    const updated = getGroup(data.groupId);
    const tw = peers.get(data.targetPeerId);
    const removedEv = { type: 'group-member-removed', groupId: data.groupId, targetPeerId: data.targetPeerId };
    if(tw) send(tw, removedEv); else { enqueueEvent(data.targetPeerId, 'group-member-removed', removedEv); httpFallbackEnqueue(data.targetPeerId, removedEv); }
    JSON.parse(updated.members).forEach(mid => {
      const ev = { type: 'group-updated', groupId: data.groupId, members: JSON.parse(updated.members) };
      const mw = peers.get(mid);
      if(mw) send(mw, ev); else { enqueueEvent(mid, 'group-updated', ev); httpFallbackEnqueue(mid, ev); }
    });
    return;
  }
  if(data.type === 'group-leave') {
    if(!myId) return;
    const g = getGroup(data.groupId); if(!g) return;
    removeMember(data.groupId, data.peerId);
    const updated = getGroup(data.groupId);
    JSON.parse(updated.members).forEach(mid => {
      const ev = { type: 'group-updated', groupId: data.groupId, members: JSON.parse(updated.members) };
      const mw = peers.get(mid);
      if(mw) send(mw, ev); else { enqueueEvent(mid, 'group-updated', ev); httpFallbackEnqueue(mid, ev); }
    });
    return;
  }
  if(data.type === 'group-members') {
    if(!myId) return;
    const g = getGroup(data.groupId); if(!g) return;
    send(ws, { type: 'group-members-reply', groupId: data.groupId, members: JSON.parse(g.members) });
    return;
  }
  if(data.type === 'group-read') {
    if(!myId) return;
    const g = getGroup(data.groupId); if(!g) return;
    JSON.parse(g.members).forEach(mid => {
      if(mid === data.readerPeerId) return;
      const ev = { type: 'group-msg-read', groupId: data.groupId, msgId: data.msgId };
      const mw = peers.get(mid);
      if(mw) send(mw, ev); else { enqueueEvent(mid, 'group-msg-read', ev); httpFallbackEnqueue(mid, ev); }
    });
    return;
  }

  if(data.type === 'signal' && data.target) {
    const tw = peers.get(data.target.toLowerCase());
    if(tw) send(tw, { type: 'signal', from: myId, payload: data.payload });
  }
}

// ─── WebSocket соединения ─────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  ws._socket.setTimeout(0);
  ws._socket.setNoDelay(true);
  ws._socket.setKeepAlive(true, 60000);

  // Stealth-состояние для этого соединения
  ws._stealthKey = null;
  ws._stealthTxCounter = 0;
  ws._stealthRxCounter = 0;
  ws._stealthSid = null;
  ws._myId = null;

  ws.on('message', async (raw) => {
    if (!checkRateLimit(ws)) {
      ws.close(4001, 'Rate limit exceeded');
      return;
    }

    let data;

    // ── Определяем тип фрейма: бинарный (Stealth) или текстовый (JSON) ──
    if (Buffer.isBuffer(raw)) {
      // Бинарный фрейм — пробуем декодировать как Stealth
      if (ws._stealthKey) {
        // Клиент уже в Stealth-режиме
        data = stealthDecode(raw, ws._stealthKey, ws._stealthRxCounter);
        if (data) ws._stealthRxCounter += raw.length;
        else {
          // Попытка plain JSON в бинарном буфере (fallback)
          try { data = JSON.parse(raw.toString('utf8')); } catch { return; }
        }
      } else {
        // Ключа нет — пробуем как plain text
        try { data = JSON.parse(raw.toString('utf8')); } catch { return; }
      }
    } else {
      // Текстовый фрейм — обычный JSON
      try { data = JSON.parse(raw); } catch { return; }
    }

    if (!data) return;
    await handleMessage(ws, data, ws._stealthSid);
  });

  ws.on('close', () => {
    const myId = ws._myId;
    if(myId) {
      clearTimeout(heartbeats.get(myId));
      heartbeats.delete(myId);
      if(peers.get(myId) === ws) { 
        peers.delete(myId); 
        broadcastPresence(myId, false); 
      }
      
      activeFetchers.forEach((subs, fileId) => {
        if (subs.has(ws)) {
          subs.delete(ws);
          if (subs.size === 0) activeFetchers.delete(fileId);
        }
      });
    }
  });
  ws.on('error', (err) => console.error('WS error:', err.message));
});

// ─── Мониторинг (каждые 5 минут) ────────────────────────────────────────────
setInterval(() => {
  console.log(`[Stats] Active connections: ${peers.size}, Stealth sessions: ${stealthSessions.size}`);
}, 5 * 60 * 1000);

// ─── Graceful shutdown ─────────────────────────────────────────────────────────
function gracefulShutdown() {
  console.log('\n[Shutdown] closing all connections…');
  for (const [, ws] of peers) {
    try { ws.close(1001, 'Server restarting'); } catch(e) {}
  }
  try { fs.writeFileSync(pushFile, JSON.stringify(pushSubs)); } catch(e) {}
  try { db.close(); } catch(e) {}
  console.log('[Shutdown] done');
  process.exit(0);
}
process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// ─── Самопинг ────────────────────────────────────────────────────────────────
const APP_URL = 'https://signal-server-aipd.onrender.com';
setInterval(() => {
  https.get(APP_URL, res => console.log(`[Self-Ping] ${res.statusCode}`))
       .on('error', err => console.error(`[Self-Ping] Error: ${err.message}`));
}, 4 * 60 * 1000);

console.log(`[K-Chat server] ready on port ${PORT} | Stealth-Transport v2 active`);
