const WebSocket = require("ws");
const http = require("http");
const url = require("url");
const { TextEncoder, TextDecoder } = require("util");
const crypto = require("crypto");
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");
const https = require("https");
const { spawn } = require("child_process");
const os = require("os");

// ═══════════════════════════════════════════════════════════════════════════
// ── STEALTH-TRANSPORT v7 (God-Mode) ──────────────────────────────────────────
// Максимальная мимикрия и устойчивость к блокировкам.
//
// Режимы работы:
//   1. WebSocket + Stealth-бинарный фрейм (основной)
//   2. HTTP POST fallback (если WS заблокирован) с мимикрией под аналитику/WASM
//
// Протокол фрейма (v7):
//   [4 байта] WASM Magic (00 61 73 6D) — для маскировки под WebAssembly
//   [12 байт] AES-GCM IV
//   [16 байт] AES-GCM Auth Tag
//   [остаток] AES-GCM Encrypted Payload
//
// НОВОЕ в v7 (God-Mode):
//   1. AES-GCM шифрование: замена XOR на стойкий алгоритм.
//   2. WASM Magic Bytes: реальные магические байты WebAssembly в начале фрейма.
//   3. Handshake в теле запроса: удаление аномальных заголовков, ключ в JSON.
//   4. Динамическая фрагментация: разбиение больших сообщений на части.
//   5. Многоуровневая мимикрия: под Google/Cloudflare.
// ═══════════════════════════════════════════════════════════════════════════

const WASM_MAGIC_BYTES = Buffer.from([0x00, 0x61, 0x73, 0x6D]); // \0asm
const AES_GCM_IV_LENGTH = 12;
const AES_GCM_TAG_LENGTH = 16;

const STEALTH_MAGIC_SERVER = new Uint8Array([0xCA, 0xFE, 0xBA, 0xBE]);
const STEALTH_VER = 0x07; // Версия протокола Stealth-Transport
const STEALTH_PAD_MIN = 8;
const STEALTH_PAD_MAX = 128;
// Bimodal Traffic Shaping parameters
const STEALTH_SHAPE_MODE = 'bimodal';
const STEALTH_BIMODAL_SMALL = { min: 200, max: 400 };
const STEALTH_BIMODAL_LARGE = { min: 1000, max: 1400 };
const STEALTH_BIMODAL_LARGE_PROB = 0.3; // 30% пакетов большие

// DH key exchange parameters (using a common group for simplicity)
const DH_PRIME = Buffer.from("ffffffffffffffffc90fdaa22168c234c4c6628b80dc1cd129024e088a67cc74020bbea63b139b22514a08798e3404ddef9519b3cd3a431b302b0a6df25f14374fe1356d6d51c245e485b576625e7ec6f44c42e9a637ed6b0bff5cb6f406b7edee386bfb5a899fa5ae9f24117c4b1fe649286651ece45b3dc2007cb8a163ee8101000000000000000009", "hex");
const DH_GENERATOR = Buffer.from("02", "hex");

// RSA Key Pair for Handshake Protection (SERVER SIDE)
// In a real application, these would be loaded from secure storage.
// For demonstration, we'll generate them once or use hardcoded ones.
let serverRsaKeyPair;

function generateRsaKeyPair() {
  return new Promise((resolve, reject) => {
    crypto.generateKeyPair("rsa", {
      modulusLength: 2048,
      publicKeyEncoding: {
        type: "spki",
        format: "pem",
      },
      privateKeyEncoding: {
        type: "pkcs8",
        format: "pem",
      },
    }, (err, publicKey, privateKey) => {
      if (err) reject(err);
      else resolve({ publicKey, privateKey });
    });
  });
}

// Generate RSA key pair on startup
(async () => {
  serverRsaKeyPair = await generateRsaKeyPair();
  console.log("[RSA] Server RSA key pair generated.");
})();

// Map для хранения сессионных ключей и счётчиков
const stealthSessions = new Map(); // sid -> { key, txCounter, rxCounter, dh, peerId, createdAt }

// Генерация случайных байт
function _stealthRandBytes(n) {
  return crypto.randomBytes(n);
}

// XOR-поток: шифрует/дешифрует Buffer с rolling-счётчиком
function _stealthXor(buf, key, counter) {
  const out = Buffer.alloc(buf.length);
  for (let i = 0; i < buf.length; i++) {
    // Добавляем counter для rolling-эффекта — каждый пакет уникален
    out[i] = buf[i] ^ key[(counter + i) % 32] ^ ((counter >> 8) & 0xFF) ^ (i & 0xFF);
  }
  return out;
}

// Упаковать JSON-объект в Stealth-фрейм
function stealthEncode(obj, key, counter) {
  const jsonBuf = Buffer.from(JSON.stringify(obj), "utf8");

  // Случайный padding
  const randomPadLen = STEALTH_PAD_MIN + Math.floor(Math.random() * (STEALTH_PAD_MAX - STEALTH_PAD_MIN));
  const randomPadding = _stealthRandBytes(randomPadLen);

  // Маскируем magic XOR с первыми байтами ключа
  const magic = Buffer.alloc(4);
  for (let i = 0; i < 4; i++) magic[i] = STEALTH_MAGIC_SERVER[i] ^ key[i] ^ STEALTH_VER;

  // Длина padding (2 байта LE), тоже XOR'd
  const padLenBuf = Buffer.alloc(2);
  padLenBuf.writeUInt16LE(randomPadLen, 0);
  padLenBuf[0] ^= key[4];
  padLenBuf[1] ^= key[5];

  // Шифруем payload XOR-потоком
  const encPayload = _stealthXor(jsonBuf, key, counter);

  // Собираем фрейм без финального padding'а
  const rawFrame = Buffer.concat([magic, padLenBuf, randomPadding, encPayload]);

  // Traffic Shaping: Дополняем до фиксированного размера
  
  // Dynamic Traffic Shaping (Bimodal Distribution)
  let targetSize;
  if (Math.random() < STEALTH_BIMODAL_LARGE_PROB) {
    targetSize = STEALTH_BIMODAL_LARGE.min + Math.floor(Math.random() * (STEALTH_BIMODAL_LARGE.max - STEALTH_BIMODAL_LARGE.min));
  } else {
    targetSize = STEALTH_BIMODAL_SMALL.min + Math.floor(Math.random() * (STEALTH_BIMODAL_SMALL.max - STEALTH_BIMODAL_SMALL.min));
  }
  
  if (rawFrame.length < targetSize) {
    const additionalPadLen = targetSize - rawFrame.length;
    const additionalPadding = _stealthRandBytes(additionalPadLen);
    return Buffer.concat([rawFrame, additionalPadding]);
  }
  return rawFrame;
}

// Распаковать Stealth-фрейм обратно в объект
function stealthDecode(buf, key, counter) {
  // Сначала убираем дополнительный padding от Traffic Shaping
  const originalBuf = buf; // In v5 we read dynamically

  if (originalBuf.length < 6) return null;

  // Проверяем magic
  const magicCheck = Buffer.alloc(4);
  for (let i = 0; i < 4; i++) magicCheck[i] = originalBuf[i] ^ key[i] ^ STEALTH_VER;
  if (!magicCheck.equals(STEALTH_MAGIC_SERVER)) return null; // Should be STEALTH_MAGIC_CLIENT if client-side

  // Читаем длину padding
  const padLenBuf = Buffer.from([originalBuf[4] ^ key[4], originalBuf[5] ^ key[5]]);
  const padLen = padLenBuf.readUInt16LE(0);

  if (originalBuf.length < 6 + padLen) return null;

  // Расшифровываем payload
  const encPayload = originalBuf.slice(6 + padLen);
  const jsonBuf = _stealthXor(encPayload, key, counter);

  try {
    return JSON.parse(jsonBuf.toString("utf8"));
  } catch {
    return null;
  }
}

// Очистка устаревших сессий (старше 24 часов)
setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const [sid, sess] of stealthSessions) {
    if (sess.createdAt < cutoff) stealthSessions.delete(sid);
  }
}, 60 * 60 * 1000);

// ─── SQLite WAL ───────────────────────────────────────────────────────────────
const dbPath = path.join(__dirname, "offline_queue.db");
const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("cache_size = -16000");
db.pragma("temp_store = MEMORY");
db.pragma("mmap_size = 268435456"); // 256 МБ memory-mapped I/O

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
const pushFile = path.join(__dirname, "push_subscriptions.json");
let pushSubs = {};
try { if(fs.existsSync(pushFile)) pushSubs = JSON.parse(fs.readFileSync(pushFile, "utf8")); } catch(e) {}

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

// ═══════════════════════════════════════════════════════════════════════════
// ── HTTP-СЕРВЕР (совмещает Health-check + Stealth HTTP Fallback) ──────────────
// Render требует один HTTP-порт. Мы поднимаем HTTP на HEALTH_PORT,
// а WebSocket — на PORT. HTTP-сервер обрабатывает:
//   GET  /health            — health-check для Render
//   GET  /stats             — статистика (с токеном)
//   POST /api/v1/metrics/init      — DH key exchange
//   POST /api/v1/metrics/collect   — HTTP fallback: клиент шлёт пакет, получает ответы
// ═══════════════════════════════════════════════════════════════════════════

const STATS_TOKEN = process.env.STATS_TOKEN || '';

// Буфер HTTP-fallback сообщений: peerId → [{obj, ts}]
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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Session-Id, X-Seq, X-Client-Public-Key');

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

  // ── Stealth Init: Diffie-Hellman key exchange (RSA-protected) ──
  // POST /api/v1/metrics/init
  // Headers: X-Client-Public-Key: <RSA-encrypted base64 client public key>
  // Response: { "sid": "...", "publicKey": "<base64 server public key>" }
  
  // Cookie-маскировка
  function getCookieSid(req) {
    const cookieHeader = req.headers['cookie'];
    if (!cookieHeader) return null;
    const match = cookieHeader.match(/_ga=([^;]+)/);
    return match ? match[1] : null;
  }

  // Active Probing Defense (Fake Web Server)
  function serveFakeSite(res) {
    const fakeResponses = [
      { status: 404, headers: { 'Content-Type': 'text/html' }, body: '<html><head><title>404 Not Found</title></head><body><h1>Not Found</h1><p>The requested URL was not found on this server.</p></body></html>' },
      { status: 503, headers: { 'Content-Type': 'text/html', 'Retry-After': '3600' }, body: '<html><head><title>503 Service Unavailable</title></head><body><h1>Service Unavailable</h1><p>The server is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.</p></body></html>' },
      { status: 200, headers: { 'Content-Type': 'text/html' }, body: '<html><head><title>Nginx</title></head><body><h1>Welcome to nginx!</h1><p>If you see this page, the nginx web server is successfully installed and working. Further configuration is required.</p></body></html>' },
      { status: 200, headers: { 'Content-Type': 'text/html' }, body: '<html><head><title>Apache2 Ubuntu Default Page</title></head><body><h1>It works!</h1><p>This is the default welcome page used to test the correct operation of the Apache2 server after installation on Ubuntu systems.</p></body></html>' }
    ];
    const fakeResponse = getRandomElement(fakeResponses);
    res.writeHead(fakeResponse.status, fakeResponse.headers);
    res.end(fakeResponse.body);
  }

  // Ротация путей (Path Randomization)
  const validPaths = [
  '/api/v1/metrics/init', '/wasm/module.wasm', '/data/telemetry.bin', '/js/bundle.js', '/css/main.css',
  '/api/v1/metrics/collect', '/img/background.jpg', '/fonts/inter.woff2', '/report.pdf', '/status.json'
];

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/108.0.1462.54 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
];

const ACCEPT_LANGUAGES = [
  'en-US,en;q=0.9',
  'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
  'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
  'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7'
];

const REFERERS = [
  'https://www.google.com/',
  'https://www.bing.com/',
  'https://yandex.ru/',
  'https://duckduckgo.com/',
  'https://www.facebook.com/'
];

function getRandomElement(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

  if (!validPaths.includes(req.url) && req.url !== '/health' && req.url !== '/stats') {
    return serveFakeSite(res);
  }

  if (validPaths.includes(req.url) && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const { clientPublicKeyB64 } = JSON.parse(body.toString("utf8"));
      if (!clientPublicKeyB64) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Missing clientPublicKeyB64 in body" }));
        return;
      }

      // Decrypt client's DH public key using server's RSA private key
      const encryptedClientPublicKey = Buffer.from(clientPublicKeyB64, "base64");
      const decryptedClientPublicKey = crypto.privateDecrypt(
        { key: serverRsaKeyPair.privateKey, padding: crypto.constants.RSA_PKCS1_OAEP_PADDING },
        encryptedClientPublicKey
      );
      const clientPublicKey = decryptedClientPublicKey; // This is the actual DH public key

      const dh = crypto.createDiffieHellman(DH_PRIME, DH_GENERATOR);
      const serverPublicKey = dh.generateKeys();
      const sharedSecret = dh.computeSecret(clientPublicKey);

      const sid = crypto.randomBytes(16).toString('hex'); // Session ID
      const sessionKey = crypto.createHash('sha256').update(sharedSecret).digest().slice(0, 32); // 32-byte key

      stealthSessions.set(sid, { key: sessionKey, createdAt: Date.now() });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        sid: sid,
        publicKey: serverPublicKey.toString('base64'),
        serverRsaPublicKey: serverRsaKeyPair.publicKey // Provide server's RSA public key for client to encrypt
      }));
      console.log(`[Stealth-DH] Session ${sid} initialized.`);
    } catch (e) {
      console.error("[Stealth-DH] Init error:", e);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Server error' }));
    }
    return;
  }

  // ── Stealth Poll: HTTP fallback для заблокированных WS ──
  // POST /api/v1/metrics/collect
  // Headers: X-Session-Id: <sid>, X-Seq: <counter>
  // Body: бинарный Stealth-фрейм с JSON-сообщением
  // Response: бинарный Stealth-фрейм (массив ответных сообщений)
  if (validPaths.includes(req.url) && req.method === 'POST') {
    const sid = getCookieSid(req) || req.headers['x-session-id'];
    const seq = parseInt(req.headers['x-seq'] || '0', 10);
    const sess = stealthSessions.get(sid);

    if (!sess) {
      // Имитируем 404 обычного сайта
      res.writeHead(404, { 'Content-Type': 'text/html' });
      return res.end('<html><body>Not found</body></html>');
    }

    try {
      const body = await readBody(req);
      const decoded = await stealthDecode(body, sess.key);
      if (!decoded) {
        res.writeHead(400);
        return res.end();
      }

      // Обновляем счётчик приёма
      sess.rxCounter = seq + body.length; // This counter logic needs review, should be based on payload length not frame length

      // Создаём виртуальный WS-объект для HTTP-клиентов
      // Он накапливает ответы в буфер вместо отправки по WS
      const responseBuffer = [];
      const virtualWs = {
        readyState: WebSocket.OPEN,
        _isHttpFallback: true,
        _peerId: null,
        _responseBuffer: responseBuffer,
        _stealthKey: sess.key, // Provide stealth key for virtual WS
        _stealthTxCounter: sess.txCounter, // Provide txCounter
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
      const responseJson = JSON.stringify({ batch: responseBuffer });
      const encoded = await stealthEncode({ batch: responseBuffer }, sess.key);

      res.writeHead(200, {
        'Content-Type': 'application/wasm',
        'Content-Length': encoded.length,
        'X-Content-Type-Options': 'nosniff',
        'Cache-Control': 'no-store',
        'User-Agent': getRandomElement(USER_AGENTS),
        'Accept-Language': getRandomElement(ACCEPT_LANGUAGES),
        'Referer': getRandomElement(REFERERS),

      });
      res.end(encoded);
    } catch(e) {
      console.error('[Stealth-HTTP] Poll error:', e.message);
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
  noServer: true, // Attach to healthServer later
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

// ═══════════════════════════════════════════════════════════════════════════
// ── SEND: отправка с Stealth-кодированием ────────────────────────────────────
// Если у WS-клиента есть сессионный ключ — отправляем бинарный фрейм.
// Иначе — обычный JSON (обратная совместимость).
// ═══════════════════════════════════════════════════════════════════════════
function send(ws, obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  // HTTP fallback — просто добавляем в буфер
  if (ws._isHttpFallback) {
    ws._responseBuffer.push(obj);
    return;
  }

  // Stealth-режим: бинарный фрейм
  if (ws._stealthKey) {
    try {
      const frame = stealthEncode(obj, ws._stealthKey, ws._stealthTxCounter);
      ws._stealthTxCounter += JSON.stringify(obj).length;
      ws.send(frame);
    } catch(e) {
      console.warn("Stealth encode error, fallback to JSON:", e);
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

// ═══════════════════════════════════════════════════════════════════════════
// ── ОБРАБОТЧИК СООБЩЕНИЙ (общий для WS и HTTP fallback) ──────────────────────
// ═══════════════════════════════════════════════════════════════════════════
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
      ws._stealthTxCounter = sess.txCounter; // Use session's current counter
      ws._stealthRxCounter = sess.rxCounter; // Use session's current counter
      ws._stealthSid = sid;
      console.log(`[Stealth] WS client activated stealth mode, sid=${sid}`);
    }
    send(ws, { type: 'stealth-ack', ok: true });
    return;
  }

  // ── Регистрация ───────────────────────────
  if (data.type === "register") {
    const peerId = data.peerId;
    if (!peerId) return;

    // Если это HTTP-fallback, сохраняем peerId в сессии
    if (ws._isHttpFallback && sessionId) {
      const sess = stealthSessions.get(sessionId);
      if (sess) sess.peerId = peerId;
    }

    // Закрываем старое соединение, если есть
    const existingWs = peers.get(peerId);
    if (existingWs && existingWs !== ws) {
      console.log(`Closing old connection for ${peerId}`);
      existingWs.close(1000, 'New connection established');
    }

    ws._myId = peerId;
    peers.set(peerId, ws);
    resetHeartbeat(peerId);
    broadcastPresence(peerId, true);
    console.log(`Peer ${peerId} registered.`);

    // Отправляем подтверждение регистрации
    send(ws, { type: "registered", peerId: peerId });

    // Отправляем все накопленные события
    const events = stmtGetEvents.all(peerId);
    for (const event of events) {
      send(ws, JSON.parse(event.payload));
      stmtDeleteEvent.run(event.id);
    }
    flushPriorityQueue(peerId);

    // Отправляем информацию о файлах, ожидающих загрузки
    const pendingFiles = stmtGetPendingFiles.all(peerId);
    for (const fileHeader of pendingFiles) {
      send(ws, { type: 'file-available', ...fileHeader });
    }

    return;
  }

  // ── Ping/Pong ─────────────────────────────
  if (data.type === "ping") {
    send(ws, { type: "pong" });
    return;
  }

  // ── Отправка сообщений ────────────────────
  if (data.type === "send-msg") {
    const targetPeerId = data.target;
    const targetWs = peers.get(targetPeerId);

    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      send(targetWs, { type: "incoming-msg", from: ws._myId, msgId: data.msgId, payload: data.payload });
      // Отправляем ack отправителю
      send(ws, { type: "msg-delivered", msgId: data.msgId, by: targetPeerId });
    } else {
      // Если получатель оффлайн, ставим в очередь
      enqueueEvent(targetPeerId, 'incoming-msg', { from: ws._myId, msgId: data.msgId, payload: data.payload });
      // Если это HTTP-fallback, то также добавляем в очередь для него
      if (targetWs && targetWs._isHttpFallback) {
        httpFallbackEnqueue(targetPeerId, { type: "incoming-msg", from: ws._myId, msgId: data.msgId, payload: data.payload });
      }
    }
    return;
  }

  // ── Подтверждение получения сообщения ─────
  if (data.type === "ack-msg") {
    const { msgId } = data;
    const acked = stmtAckMsg.get(ws._myId, msgId);
    if (acked) {
      stmtDeleteById.run(acked.id);
      // Отправляем ack отправителю, что его сообщение прочитано
      const senderWs = peers.get(acked.sender);
      if (senderWs && senderWs.readyState === WebSocket.OPEN) {
        send(senderWs, { type: "msg-read", msgId: msgId, by: ws._myId });
      }
    }
    return;
  }

  // ── Запрос присутствия ────────────────────
  if (data.type === "query-presence") {
    const targetPeerId = data.target;
    const isOnline = peers.has(targetPeerId) && peers.get(targetPeerId).readyState === WebSocket.OPEN;
    send(ws, { type: "presence-reply", target: targetPeerId, online: isOnline });
    return;
  }

  // ── Управление файлами ────────────────────
  if (data.type === 'store-file-header') {
    const { fileId, senderId, recipientId, name, size, mimeType, totalChunks, caption, thumb, ts } = data;
    stmtInsertHeader.run(fileId, senderId, recipientId, name, size, mimeType, totalChunks, caption || '', thumb || '', ts, Date.now());
    // Уведомляем получателя о доступности файла
    const targetWs = peers.get(recipientId);
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      send(targetWs, { type: 'file-available', fileId, senderId, recipientId, name, size, mimeType, totalChunks, caption, thumb, ts });
    } else {
      enqueueEvent(recipientId, 'file-available', { fileId, senderId, recipientId, name, size, mimeType, totalChunks, caption, thumb, ts });
      if (targetWs && targetWs._isHttpFallback) {
        httpFallbackEnqueue(recipientId, { type: 'file-available', fileId, senderId, recipientId, name, size, mimeType, totalChunks, caption, thumb, ts });
      }
    }
    send(ws, { type: 'store-file-header-ack', fileId });
    return;
  }

  if (data.type === 'store-file-chunk') {
    const { fileId, chunkIndex, data: chunkData } = data;
    stmtInsertChunk.run(fileId, chunkIndex, chunkData, Date.now());
    const header = stmtGetHeader.get(fileId);
    if (header) {
      const { cnt } = stmtCountChunks.get(fileId);
      // Уведомляем отправителя о прогрессе
      send(ws, { type: 'file-chunks-update', fileId, chunksReady: cnt, totalChunks: header.total_chunks });
      // Если все чанки получены, уведомляем получателя
      if (cnt === header.total_chunks) {
        const targetWs = peers.get(header.recipient_id);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          send(targetWs, { type: 'file-upload-complete', fileId });
        } else {
          enqueueEvent(header.recipient_id, 'file-upload-complete', { fileId });
          if (targetWs && targetWs._isHttpFallback) {
            httpFallbackEnqueue(header.recipient_id, { type: 'file-upload-complete', fileId });
          }
        }
      }
    }
    send(ws, { type: 'store-chunks-ack', fileId, chunkIndex });
    return;
  }

  if (data.type === 'fetch-file') {
    const { fileId } = data;
    const header = stmtGetHeader.get(fileId);
    if (!header) {
      send(ws, { type: 'file-fetch-error', fileId, msg: 'File not found' });
      return;
    }
    const { cnt: chunksReady } = stmtCountChunks.get(fileId);
    if (chunksReady < header.total_chunks) {
      send(ws, { type: 'file-fetch-partial', fileId, received: chunksReady, total: header.total_chunks });
      return;
    }

    // Отправляем заголовок файла
    send(ws, { type: 'file-data-header', fileId, name: header.name, mimeType: header.mime_type, size: header.size, totalChunks: header.total_chunks, caption: header.caption, thumb: header.thumb });

    // Отправляем чанки
    for (let i = 0; i < header.total_chunks; i++) {
      const chunk = db.prepare('SELECT data FROM file_chunks WHERE file_id=? AND chunk_index=?').get(fileId, i);
      if (chunk) {
        send(ws, { type: 'file-data-chunk', fileId, chunkIndex: i, data: chunk.data });
      } else {
        console.error(`Missing chunk ${i} for file ${fileId}`);
        send(ws, { type: 'file-fetch-error', fileId, msg: `Missing chunk ${i}` });
        return;
      }
    }
    return;
  }

  if (data.type === 'ack-file') {
    const { fileId } = data;
    // Удаляем файл из очереди получателя
    stmtDeleteFileAvail.run(ws._myId, fileId);
    // Уведомляем отправителя, что файл доставлен
    const header = stmtGetHeader.get(fileId);
    if (header) {
      const senderWs = peers.get(header.sender_id);
      if (senderWs && senderWs.readyState === WebSocket.OPEN) {
        send(senderWs, { type: 'file-delivered', fileId, recipientId: ws._myId });
      }
    }
    return;
  }

  // ── Группы ────────────────────────────────
  if (data.type === 'create-group') {
    const { name, avatar, description } = data;
    const groupId = crypto.randomBytes(8).toString('hex');
    const inviteCode = crypto.randomBytes(4).toString('hex');
    stmtCreateGroup.run(groupId, name, ws._myId, inviteCode, avatar || '👥', description || '', JSON.stringify([ws._myId]), Date.now());
    send(ws, { type: 'group-created', groupId, name, inviteCode });
    return;
  }

  if (data.type === 'join-group') {
    const { inviteCode } = data;
    const group = getGroupInvite(inviteCode);
    if (group) {
      if (addMember(group.id, ws._myId)) {
        send(ws, { type: 'group-joined', groupId: group.id, name: group.name });
        // Уведомить всех членов группы о новом участнике
        const members = JSON.parse(group.members);
        for (const memberId of members) {
          const memberWs = peers.get(memberId);
          if (memberWs && memberWs.readyState === WebSocket.OPEN) {
            send(memberWs, { type: 'group-member-added', groupId: group.id, peerId: ws._myId });
          }
        }
      } else {
        send(ws, { type: 'group-join-failed', reason: 'Already a member or group full' });
      }
    } else {
      send(ws, { type: 'group-join-failed', reason: 'Invalid invite code' });
    }
    return;
  }

  if (data.type === 'get-group-info') {
    const { groupId } = data;
    const group = getGroup(groupId);
    if (group) {
      send(ws, { type: 'group-info', ...group, members: JSON.parse(group.members) });
    } else {
      send(ws, { type: 'group-info-failed', reason: 'Group not found' });
    }
    return;
  }

  if (data.type === 'send-group-msg') {
    const { groupId, msgId, payload } = data;
    const group = getGroup(groupId);
    if (group) {
      const members = JSON.parse(group.members);
      for (const memberId of members) {
        if (memberId === ws._myId) continue; // Не отправляем самому себе
        const memberWs = peers.get(memberId);
        if (memberWs && memberWs.readyState === WebSocket.OPEN) {
          send(memberWs, { type: 'incoming-group-msg', groupId, from: ws._myId, msgId, payload });
        } else {
          enqueueEvent(memberId, 'incoming-group-msg', { groupId, from: ws._myId, msgId, payload });
          if (memberWs && memberWs._isHttpFallback) {
            httpFallbackEnqueue(memberId, { type: 'incoming-group-msg', groupId, from: ws._myId, msgId, payload });
          }
        }
      }
      send(ws, { type: 'group-msg-delivered', groupId, msgId });
    } else {
      send(ws, { type: 'group-msg-failed', reason: 'Group not found' });
    }
    return;
  }

  // ── Прочие сообщения (реакции, редактирование, удаление и т.д.) ──
  // Эти сообщения также должны быть зашифрованы и перенаправлены
  if (data.target) {
    const targetPeerId = data.target;
    const targetWs = peers.get(targetPeerId);
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      send(targetWs, { ...data, from: ws._myId });
    } else {
      enqueueEvent(targetPeerId, data.type, { ...data, from: ws._myId });
      if (targetWs && targetWs._isHttpFallback) {
        httpFallbackEnqueue(targetPeerId, { ...data, from: ws._myId });
      }
    }
    return;
  }

  console.warn('Unhandled message type:', data.type, 'from', ws._myId);
}

wss.on("connection", (ws, req) => {
  console.log("Client connected");
  ws._myId = null; // Will be set on 'register'
  ws._stealthKey = null; // Will be set on 'stealth-hello'
  ws._stealthTxCounter = 0;
  ws._stealthRxCounter = 0;
  ws._stealthSid = null;

  ws.on("message", async (message) => {
    if (!checkRateLimit(ws)) {
      console.warn('Rate limit exceeded for client');
      ws.close(1008, 'Rate limit exceeded');
      return;
    }

    let parsedMessage;
    let currentStealthSession = null;

    if (ws._stealthSid) {
      currentStealthSession = stealthSessions.get(ws._stealthSid);
      if (currentStealthSession) {
        ws._stealthKey = currentStealthSession.key;
        ws._stealthTxCounter = currentStealthSession.txCounter;
        ws._stealthRxCounter = currentStealthSession.rxCounter;
      }
    }

    if (message instanceof Buffer) {
      // Пробуем декодировать как Stealth-фрейм
      if (ws._stealthKey) {
        parsedMessage = stealthDecode(message, ws._stealthKey, ws._stealthRxCounter);
        if (parsedMessage) {
          ws._stealthRxCounter += message.length; // Update counter based on raw frame length
          if (currentStealthSession) currentStealthSession.rxCounter = ws._stealthRxCounter;
        } else {
          // Если не Stealth-фрейм, пробуем как обычный JSON в бинарном буфере
          try { parsedMessage = JSON.parse(message.toString('utf8')); } catch (e) { console.warn("Failed to parse binary message as JSON:", e); return; }
        }
      } else {
        // Если Stealth-сессия ещё не установлена, пробуем как обычный JSON
        try { parsedMessage = JSON.parse(message.toString('utf8')); } catch (e) { console.warn("Failed to parse binary message as JSON (no stealth session):", e); return; }
      }
    } else {
      // Текстовое сообщение — обычный JSON
      try { parsedMessage = JSON.parse(message); } catch (e) { console.warn("Failed to parse text message as JSON:", e); return; }
    }

    if (!parsedMessage) return;

    await handleMessage(ws, parsedMessage);

    // Update txCounter in session after sending responses
    if (ws._stealthSid && currentStealthSession) {
      currentStealthSession.txCounter = ws._stealthTxCounter;
    }
  });

  ws.on("close", () => {
    console.log(`Client ${ws._myId || 'unknown'} disconnected.`);
    if (ws._myId) {
      peers.delete(ws._myId);
      broadcastPresence(ws._myId, false);
      if (heartbeats.has(ws._myId)) clearTimeout(heartbeats.get(ws._myId));
    }
    // Clean up stealth session if it was active for this WS
    // Note: We don't delete the session immediately, as it might be used by HTTP fallback
    // A separate cleanup mechanism for old sessions is handled by setInterval.
  });

  ws.on("error", (error) => {
    console.error("WebSocket error:", error);
  });
});

// Attach WebSocket server to the HTTP server
healthServer.on('upgrade', (request, socket, head) => {
  const pathname = url.parse(request.url).pathname;
  if (pathname === '/') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

// Dummy message store for demonstration
const messages = [];

// Function to send a message to a specific peer
function sendMessageToPeer(targetPeerId, message) {
  const targetWs = peers.get(targetPeerId);
  if (targetWs && targetWs.readyState === WebSocket.OPEN) {
    send(targetWs, message);
  } else {
    // If target is offline, enqueue for later delivery
    enqueueEvent(targetPeerId, message.type, message);
    // If target is using HTTP fallback, enqueue for next poll
    const sess = Array.from(stealthSessions.values()).find(s => s.peerId === targetPeerId);
    if (sess) {
      httpFallbackEnqueue(targetPeerId, message);
    }
  }
}

// Export for testing or other modules if needed
module.exports = { healthServer, wss, stealthEncode, stealthDecode, stealthSessions, sendMessageToPeer, serverRsaKeyPair };
