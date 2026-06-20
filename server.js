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
const zlib = require("zlib");

// ═══════════════════════════════════════════════════════════════════════════
// ── STEALTH-TRANSPORT v9 (Multi-Bridge) ────────────────────────────────────
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
// НОВОЕ в v9 (Multi-Bridge):
//   1. AES-GCM шифрование: замена XOR на стойкий алгоритм.
//   2. WASM Magic Bytes: реальные магические байты WebAssembly в начале фрейма.
//   3. Handshake в теле запроса: удаление аномальных заголовков, ключ в JSON.
//   4. Динамическая фрагментация: разбиение больших сообщений на части.
//   5. Многоуровневая мимикрия: под Google/Cloudflare.
//
// ОПТИМИЗАЦИИ ДЛЯ ПЛОХОГО ИНТЕРНЕТА (2G/3G/H):
//   1. WebSocket perMessageDeflate — сжатие каждого фрейма
//   2. TCP keepalive на сокетах — быстрое обнаружение обрыва
//   3. Адаптивный HEARTBEAT — короткий таймаут + ping/pong
//   4. Сжатие HTTP fallback ответов через gzip
//   5. Приоритизация очереди сообщений
//   6. Батчинг событий при reconnect
// ═══════════════════════════════════════════════════════════════════════════

const WASM_MAGIC_BYTES = Buffer.from([0x00, 0x61, 0x73, 0x6D]); // \0asm
const WASM_VERSION = Buffer.from([0x01, 0x00, 0x00, 0x00]);
const WASM_SECTION_ID_DATA = 0x0B;
const WASM_MEMORY_INDEX = 0x00;
const WASM_OPCODE_I32_CONST = 0x41;
const WASM_OPCODE_END = 0x0B;

// Helper to encode LEB128 (used for WASM sizes)
function encodeLeb128(value) {
  const bytes = [];
  while (true) {
    let byte = value & 0x7f;
    value >>= 7;
    if (value !== 0) {
      byte |= 0x80;
    }
    bytes.push(byte);
    if (value === 0) {
      break;
    }
  }
  return Buffer.from(bytes);
}

// Wraps a buffer into a valid WASM Data Section
function wrapInWasmDataSection(payload) {
  // Module header
  const header = Buffer.concat([
    WASM_MAGIC_BYTES,
    WASM_VERSION,
  ]);

  // Data section content
  // Vector of data segments: count (1) + segment
  // Segment: memory_index (0x00) + offset_expr (i32.const 0) + size (payload.length)
  const dataSegmentCount = encodeLeb128(1);
  const memoryIndex = Buffer.from([WASM_MEMORY_INDEX]);
  const offsetExpr = Buffer.concat([
    Buffer.from([WASM_OPCODE_I32_CONST]),
    encodeLeb128(0),
    Buffer.from([WASM_OPCODE_END]),
  ]);
  const payloadSize = encodeLeb128(payload.length);

  const dataSectionContent = Buffer.concat([
    dataSegmentCount,
    memoryIndex,
    offsetExpr,
    payloadSize,
    payload,
  ]);

  // Data section header: section_id (0x0B) + section_size
  const dataSectionHeader = Buffer.concat([
    Buffer.from([WASM_SECTION_ID_DATA]),
    encodeLeb128(dataSectionContent.length),
  ]);

  return Buffer.concat([header, dataSectionHeader, dataSectionContent]);
}

// Unwraps a buffer from a WASM Data Section
function unwrapFromWasmDataSection(wasmBuffer) {
  let offset = 0;

  // Check WASM magic and version
  if (wasmBuffer.length < WASM_MAGIC_BYTES.length + WASM_VERSION.length) return null;
  if (!wasmBuffer.slice(offset, offset + WASM_MAGIC_BYTES.length).equals(WASM_MAGIC_BYTES)) return null;
  offset += WASM_MAGIC_BYTES.length;
  if (!wasmBuffer.slice(offset, offset + WASM_VERSION.length).equals(WASM_VERSION)) return null;
  offset += WASM_VERSION.length;

  // Read Data Section ID
  if (wasmBuffer.length < offset + 1) return null;
  const sectionId = wasmBuffer[offset];
  offset += 1;
  if (sectionId !== WASM_SECTION_ID_DATA) return null; // Not a data section

  // Read Data Section Size (LEB128)
  let sectionSize = 0;
  let shift = 0;
  let byte;
  do {
    if (wasmBuffer.length < offset + 1) return null;
    byte = wasmBuffer[offset];
    offset += 1;
    sectionSize |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);

  const sectionEnd = offset + sectionSize;
  if (wasmBuffer.length < sectionEnd) return null;

  // Read Data Segment Count (LEB128)
  let segmentCount = 0;
  shift = 0;
  do {
    if (wasmBuffer.length < offset + 1) return null;
    byte = wasmBuffer[offset];
    offset += 1;
    segmentCount |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);

  if (segmentCount !== 1) return null; // Expecting exactly one data segment

  // Read Memory Index
  if (wasmBuffer.length < offset + 1) return null;
  const memoryIndex = wasmBuffer[offset];
  offset += 1;
  if (memoryIndex !== WASM_MEMORY_INDEX) return null;

  // Read Offset Expression (i32.const 0 + end)
  if (wasmBuffer.length < offset + 1) return null;
  if (wasmBuffer[offset] !== WASM_OPCODE_I32_CONST) return null;
  offset += 1;

  // Read i32.const value (LEB128 for 0)
  shift = 0;
  let constValue = 0;
  do {
    if (wasmBuffer.length < offset + 1) return null;
    byte = wasmBuffer[offset];
    offset += 1;
    constValue |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);
  if (constValue !== 0) return null;

  if (wasmBuffer.length < offset + 1) return null;
  if (wasmBuffer[offset] !== WASM_OPCODE_END) return null;
  offset += 1;

  // Read Payload Size (LEB128)
  let payloadSize = 0;
  shift = 0;
  do {
    if (wasmBuffer.length < offset + 1) return null;
    byte = wasmBuffer[offset];
    offset += 1;
    payloadSize |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);

  if (wasmBuffer.length < offset + payloadSize) return null;

  return wasmBuffer.slice(offset, offset + payloadSize);
}
const AES_GCM_IV_LENGTH = 12;
const AES_GCM_TAG_LENGTH = 16;

const STEALTH_MAGIC_SERVER = new Uint8Array([0xCA, 0xFE, 0xBA, 0xBE]);
const STEALTH_VER = 0x09; // Версия протокола Stealth-Transport
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

// AES-GCM шифрование/дешифрование
async function _stealthEncryptAESGCM(payload, key) {
  const iv = crypto.randomBytes(AES_GCM_IV_LENGTH);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const encrypted = Buffer.concat([cipher.update(payload), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return { iv, encrypted, authTag };
}

async function _stealthDecryptAESGCM(encryptedPayload, key, iv, authTag) {
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(authTag);
  const decrypted = Buffer.concat([decipher.update(encryptedPayload), decipher.final()]);
  return decrypted;
}

// Упаковать JSON-объект в Stealth-фрейм
async function stealthEncode(obj, key) {
  const jsonBuf = Buffer.from(JSON.stringify(obj), "utf8");

  // Динамический padding внутри зашифрованного блока
  const randomPadLen = STEALTH_PAD_MIN + Math.floor(Math.random() * (STEALTH_PAD_MAX - STEALTH_PAD_MIN));
  const padding = _stealthRandBytes(randomPadLen);
  const paddedPayload = Buffer.concat([jsonBuf, padding]);

  // Шифруем payload AES-GCM
  const { iv, encrypted, authTag } = await _stealthEncryptAESGCM(paddedPayload, key);

  // Собираем фрейм: IV + Auth Tag + Encrypted Payload
  let rawFrame = Buffer.concat([iv, authTag, encrypted]);

  // Dynamic Traffic Shaping (Bimodal Distribution) - apply to the *entire* frame after encryption
  let targetSize;
  if (Math.random() < STEALTH_BIMODAL_LARGE_PROB) {
    targetSize = STEALTH_BIMODAL_LARGE.min + Math.floor(Math.random() * (STEALTH_BIMODAL_LARGE.max - STEALTH_BIMODAL_LARGE.min));
  } else {
    targetSize = STEALTH_BIMODAL_SMALL.min + Math.floor(Math.random() * (STEALTH_BIMODAL_SMALL.max - STEALTH_BIMODAL_SMALL.min));
  }
  
  if (rawFrame.length < targetSize) {
    const additionalPadLen = targetSize - rawFrame.length;
    const additionalPadding = _stealthRandBytes(additionalPadLen);
    rawFrame = Buffer.concat([rawFrame, additionalPadding]);
  } else if (rawFrame.length > targetSize) {
    rawFrame = rawFrame.slice(0, targetSize);
  }

  // Оборачиваем в WASM Data Section
  return wrapInWasmDataSection(rawFrame);
}

// Распаковать Stealth-фрейм обратно в объект
async function stealthDecode(buf, key) {
  const unwrapped = unwrapFromWasmDataSection(buf);
  if (!unwrapped) return null;

  if (unwrapped.length < AES_GCM_IV_LENGTH + AES_GCM_TAG_LENGTH) return null;

  const iv = unwrapped.slice(0, AES_GCM_IV_LENGTH);
  const authTag = unwrapped.slice(AES_GCM_IV_LENGTH, AES_GCM_IV_LENGTH + AES_GCM_TAG_LENGTH);
  const encryptedPayload = unwrapped.slice(AES_GCM_IV_LENGTH + AES_GCM_TAG_LENGTH);

  try {
    const paddedPayload = await _stealthDecryptAESGCM(encryptedPayload, key, iv, authTag);
    // Удаляем padding
    let jsonEnd = paddedPayload.length - 1;
    while (jsonEnd >= 0 && paddedPayload[jsonEnd] === 0) {
      jsonEnd--;
    }
    const jsonBuf = paddedPayload.slice(0, jsonEnd + 1);
    return JSON.parse(jsonBuf.toString("utf8"));
  } catch (e) {
    console.error("[Stealth] Decryption error:", e.message);
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
// ── ОПТИМИЗАЦИИ ДЛЯ ПЛОХОГО ИНТЕРНЕТА ────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════

// Heartbeat: короткий таймаут + активный ping/pong для быстрого обнаружения обрыва
// На 2G/3G соединение может "зависнуть" без явного закрытия сокета
const HEARTBEAT_TIMEOUT = 90_000;    // 90 секунд — если нет pong, закрываем
const HEARTBEAT_INTERVAL = 30_000;   // ping каждые 30 секунд
const WS_PING_TIMEOUT = 20_000;      // ждём pong 20 секунд

// ─── Утилита: gzip-сжатие буфера ─────────────────────────────────────────────
function gzipBuffer(buf) {
  return new Promise((resolve, reject) => {
    zlib.gzip(buf, { level: zlib.constants.Z_BEST_SPEED }, (err, result) => {
      if (err) reject(err);
      else resolve(result);
    });
  });
}

// ─── Утилита: gunzip буфера ───────────────────────────────────────────────────
function gunzipBuffer(buf) {
  return new Promise((resolve, reject) => {
    zlib.gunzip(buf, (err, result) => {
      if (err) reject(err);
      else resolve(result);
    });
  });
}

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

// Буфер HTTP-fallback сообщений: peerId → [{obj, ts, priority}]
// Сервер складывает сюда исходящие сообщения для клиентов без WS
const httpFallbackQueues = new Map();

// Приоритеты сообщений (меньше = важнее)
const MSG_PRIORITY = {
  'registered': 0,
  'incoming-msg': 1,
  'msg-delivered': 2,
  'msg-read': 2,
  'file-available': 3,
  'file-upload-complete': 3,
  'presence': 5,
  'pong': 4,
  'default': 10
};

function getMsgPriority(obj) {
  return MSG_PRIORITY[obj.type] !== undefined ? MSG_PRIORITY[obj.type] : MSG_PRIORITY['default'];
}

function httpFallbackEnqueue(peerId, obj) {
  if (!httpFallbackQueues.has(peerId)) httpFallbackQueues.set(peerId, []);
  const q = httpFallbackQueues.get(peerId);
  const priority = getMsgPriority(obj);
  q.push({ obj, ts: Date.now(), priority });
  // Сортируем по приоритету (важные — первыми)
  q.sort((a, b) => a.priority - b.priority);
  // Не копим больше 500 сообщений (увеличено для плохого интернета)
  if (q.length > 500) q.splice(500);
}

// Очистка старых HTTP-fallback буферов (старше 10 минут)
setInterval(() => {
  const cutoff = Date.now() - 10 * 60 * 1000;
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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Session-Id, X-Seq, X-Client-Public-Key, Accept-Encoding');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // ── Health-check ──
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
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
    res.end(JSON.stringify({
      activeConnections: peers.size,
      queuedEvents: eventCount,
      httpFallbackClients: httpFallbackQueues.size,
      uptime: process.uptime()
    }));
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

  // ── Stealth Init: обрабатываем все validPaths для POST ──
  if (validPaths.includes(req.url) && req.method === 'POST') {
    // Определяем тип запроса по наличию X-Session-Id или Cookie _ga
    const sid = getCookieSid(req) || req.headers['x-session-id'];
    const existingSess = sid ? stealthSessions.get(sid) : null;

    // Если сессия уже есть — это HTTP fallback poll
    if (existingSess) {
      try {
        const body = await readBody(req);
        const decoded = await stealthDecode(body, existingSess.key);
        if (!decoded) {
          res.writeHead(400);
          return res.end();
        }

        existingSess.rxCounter = (existingSess.rxCounter || 0) + body.length;

        // Создаём виртуальный WS-объект для HTTP-клиентов
        const responseBuffer = [];
        const virtualWs = {
          readyState: WebSocket.OPEN,
          _isHttpFallback: true,
          _peerId: null,
          _responseBuffer: responseBuffer,
          _stealthKey: existingSess.key,
          _stealthTxCounter: existingSess.txCounter || 0,
          send(jsonStr) {
            try {
              const obj = JSON.parse(jsonStr);
              responseBuffer.push(obj);
            } catch(e) {}
          }
        };

        // Если у нас есть peerId из сессии — добавляем накопленные сообщения
        if (existingSess.peerId) {
          const q = httpFallbackQueues.get(existingSess.peerId) || [];
          for (const item of q) responseBuffer.push(item.obj);
          httpFallbackQueues.delete(existingSess.peerId);
        }

        // Обрабатываем входящее сообщение
        await handleMessage(virtualWs, decoded, sid);

        // Если зарегистрировался — запоминаем peerId в сессии
        if (virtualWs._peerId) existingSess.peerId = virtualWs._peerId;

        // Кодируем все ответы в один бинарный фрейм
        const encoded = await stealthEncode({ batch: responseBuffer }, existingSess.key);

        // ── Сжатие ответа gzip для экономии трафика на 2G/3G ──
        const acceptEncoding = req.headers['accept-encoding'] || '';
        let responseData = encoded;
        let contentEncoding = null;
        if (acceptEncoding.includes('gzip') && encoded.length > 512) {
          try {
            responseData = await gzipBuffer(encoded);
            contentEncoding = 'gzip';
          } catch(e) {
            responseData = encoded;
          }
        }

        const headers = {
          'Content-Type': 'application/wasm',
          'Content-Length': responseData.length,
          'Connection': 'keep-alive',
          'X-Stealth-Version': STEALTH_VER,
          'X-Content-Type-Options': 'nosniff',
          'Cache-Control': 'no-store',
        };
        if (contentEncoding) headers['Content-Encoding'] = contentEncoding;

        res.writeHead(200, headers);
        res.end(responseData);
      } catch(e) {
        console.error('[Stealth-HTTP] Poll error:', e.message);
        res.writeHead(500);
        res.end();
      }
      return;
    }

    // Нет сессии — это Stealth Init (DH key exchange)
    try {
      const body = await readBody(req);
      let parsedBody;
      try {
        parsedBody = JSON.parse(body.toString("utf8"));
      } catch(e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid JSON body" }));
        return;
      }

      // Поддерживаем оба формата: clientPublicKeyB64 (старый) и payload (новый от клиента)
      const clientPublicKeyB64 = parsedBody.clientPublicKeyB64 || parsedBody.payload;
      if (!clientPublicKeyB64) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Missing clientPublicKeyB64 in body" }));
        return;
      }

      // ИСПРАВЛЕНИЕ: Поддерживаем ECDH P-256 (новый клиент) и классический DH (старый)
      // Клиент отправляет SPKI-encoded ECDH P-256 public key в base64
      const clientPublicKeyBuf = Buffer.from(clientPublicKeyB64, "base64");
      const newSid = crypto.randomBytes(16).toString('hex');
      let sessionKey;
      let serverPublicKeyB64 = null;

      // Пробуем ECDH P-256 (новый протокол)
      let ecdhSuccess = false;
      try {
        // Генерируем серверную ECDH пару
        const serverEcdh = crypto.createECDH('prime256v1'); // P-256
        serverEcdh.generateKeys();

        // Вычисляем shared secret из клиентского публичного ключа
        // Клиент отправляет SPKI-encoded ключ — извлекаем raw точку (последние 65 байт)
        let clientRawKey = clientPublicKeyBuf;
        // SPKI для P-256 имеет 91 байт: 26 байт заголовка + 65 байт ключа
        if (clientPublicKeyBuf.length === 91) {
          clientRawKey = clientPublicKeyBuf.slice(26); // raw uncompressed point
        } else if (clientPublicKeyBuf.length === 65) {
          clientRawKey = clientPublicKeyBuf; // уже raw
        }

        const sharedSecret = serverEcdh.computeSecret(clientRawKey);
        sessionKey = crypto.createHash('sha256').update(sharedSecret).digest().slice(0, 32);

        // Отправляем серверный публичный ключ в SPKI формате (для совместимости)
        // ECDH getPublicKey() возвращает raw uncompressed point (65 байт)
        // Оборачиваем в SPKI: 26-байтовый заголовок P-256 + 65 байт ключа
        const spkiHeader = Buffer.from('3059301306072a8648ce3d020106082a8648ce3d030107034200', 'hex');
        const serverSpki = Buffer.concat([spkiHeader, serverEcdh.getPublicKey()]);
        serverPublicKeyB64 = serverSpki.toString('base64');
        ecdhSuccess = true;
        console.log(`[Stealth-ECDH] P-256 session ${newSid} initialized`);
      } catch(e) {
        console.warn('[Stealth-ECDH] P-256 failed, trying classic DH:', e.message);
      }

      // Fallback: классический DH (старый протокол)
      if (!ecdhSuccess) {
        try {
          const dh = crypto.createDiffieHellman(DH_PRIME, DH_GENERATOR);
          const serverDhPublicKey = dh.generateKeys();
          let sharedSecret;
          try {
            // Пробуем RSA расшифровку (старый клиент)
            const decrypted = crypto.privateDecrypt(
              { key: serverRsaKeyPair.privateKey, padding: crypto.constants.RSA_PKCS1_OAEP_PADDING },
              clientPublicKeyBuf
            );
            sharedSecret = dh.computeSecret(decrypted);
          } catch(e2) {
            // Используем напрямую
            try { sharedSecret = dh.computeSecret(clientPublicKeyBuf); }
            catch(e3) { sharedSecret = crypto.randomBytes(32); }
          }
          sessionKey = crypto.createHash('sha256').update(sharedSecret).digest().slice(0, 32);
          serverPublicKeyB64 = serverDhPublicKey.toString('base64');
          console.log(`[Stealth-DH] Classic DH session ${newSid} initialized`);
        } catch(e) {
          // Последний fallback: случайный ключ (sid-based на клиенте)
          sessionKey = crypto.randomBytes(32);
          console.warn(`[Stealth] Using random key for session ${newSid}`);
        }
      }

      stealthSessions.set(newSid, {
        key: sessionKey,
        txCounter: 0,
        rxCounter: 0,
        createdAt: Date.now(),
        peerId: parsedBody.client_id || null
      });

      const responseBody = { sid: newSid };
      if (serverPublicKeyB64) responseBody.publicKey = serverPublicKeyB64;

      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
        'Vary': 'Accept-Encoding',
      });
      res.end(JSON.stringify(responseBody));
    } catch (e) {
      console.error("[Stealth-DH] Init error:", e);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Server error' }));
    }
    return;
  }

  // Всё остальное — имитируем обычный веб-сервер (404)
  res.writeHead(404, { 'Content-Type': 'text/html' });
  res.end('<html><head><title>404 Not Found</title></head><body><h1>Not Found</h1></body></html>');
});

// ── TCP keepalive на HTTP-сервере для быстрого обнаружения обрыва ──
healthServer.on('connection', (socket) => {
  // Включаем TCP keepalive: начинаем через 30с, интервал 10с, 3 попытки
  socket.setKeepAlive(true, 30000);
  // Таймаут неактивного соединения — 120 секунд
  socket.setTimeout(120000);
  socket.on('timeout', () => {
    socket.destroy();
  });
});

healthServer.listen(HEALTH_PORT, () => {
  console.log(`[Health+Stealth] listening on port ${HEALTH_PORT}`);
});

// ─── WebSocket Server ─────────────────────────────────────────────────────────
const wss = new WebSocket.Server({
  noServer: true, // Attach to healthServer later
  // ── Оптимизация для плохого интернета: сжатие WS фреймов ──
  // perMessageDeflate значительно уменьшает размер передаваемых данных
  // Особенно эффективно для JSON-сообщений (сжатие до 70-80%)
  perMessageDeflate: {
    zlibDeflateOptions: {
      chunkSize: 1024,
      memLevel: 7,
      level: 3 // Быстрое сжатие (не максимальное) для экономии CPU
    },
    zlibInflateOptions: {
      chunkSize: 10 * 1024
    },
    clientNoContextTakeover: true,  // Экономим память на клиенте
    serverNoContextTakeover: true,  // Экономим память на сервере
    serverMaxWindowBits: 10,        // Меньше памяти, чуть хуже сжатие
    concurrencyLimit: 10,
    threshold: 512 // Сжимаем только если больше 512 байт
  },
  maxPayload: 64 * 1024 * 1024, // 64 МБ — разумный лимит (было 1 ГБ!)
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
const pingTimers = new Map(); // Таймеры ping для каждого клиента

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
      try { ws.send(JSON.stringify(obj)); } catch(e2) {}
    }
    return;
  }

  // Обычный JSON (клиент без Stealth)
  try { ws.send(JSON.stringify(obj)); } catch(e) {}
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

// ── Heartbeat с активным ping/pong ──
// На плохом интернете TCP соединение может "зависнуть" без явного закрытия.
// Активный ping/pong позволяет быстро обнаружить это и переподключиться.
function resetHeartbeat(peerId) {
  if(heartbeats.has(peerId)) clearTimeout(heartbeats.get(peerId));
  heartbeats.set(peerId, setTimeout(() => {
    const ws = peers.get(peerId);
    if(ws) {
      console.log(`[Heartbeat] Timeout for ${peerId}, closing connection`);
      ws.terminate(); // terminate() вместо close() — немедленно закрываем
    }
  }, HEARTBEAT_TIMEOUT));
}

function startPingTimer(peerId) {
  stopPingTimer(peerId);
  const timer = setInterval(() => {
    const ws = peers.get(peerId);
    if(!ws || ws.readyState !== WebSocket.OPEN) {
      stopPingTimer(peerId);
      return;
    }
    // Отправляем WebSocket ping frame (не JSON ping, а нативный WS ping)
    ws.ping(Buffer.from('k'), false, (err) => {
      if(err) {
        console.log(`[Ping] Error for ${peerId}:`, err.message);
        stopPingTimer(peerId);
      }
    });
    // Если pong не пришёл за WS_PING_TIMEOUT — закрываем
    const pongTimeout = setTimeout(() => {
      const currentWs = peers.get(peerId);
      if(currentWs && currentWs.readyState === WebSocket.OPEN && !currentWs._lastPong) {
        console.log(`[Ping] No pong from ${peerId}, terminating`);
        currentWs.terminate();
      }
      if(currentWs) currentWs._lastPong = false;
    }, WS_PING_TIMEOUT);
    if(ws._pongTimeout) clearTimeout(ws._pongTimeout);
    ws._pongTimeout = pongTimeout;
    ws._lastPong = false;
  }, HEARTBEAT_INTERVAL);
  pingTimers.set(peerId, timer);
}

function stopPingTimer(peerId) {
  if(pingTimers.has(peerId)) {
    clearInterval(pingTimers.get(peerId));
    pingTimers.delete(peerId);
  }
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
  let myId = ws._myId || null;

  if(myId) resetHeartbeat(myId);

  // ── Stealth-handshake: клиент сообщает свой sessionId ──
  if(data.type === 'stealth-hello') {
    const sid = data.sid;
    const sess = stealthSessions.get(sid);
    if(sess) {
      ws._stealthKey = sess.key;
      ws._stealthTxCounter = sess.txCounter || 0;
      ws._stealthRxCounter = sess.rxCounter || 0;
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
      try { existingWs.terminate(); } catch(e) {}
    }

    ws._myId = peerId;
    if(ws._isHttpFallback) ws._peerId = peerId;
    peers.set(peerId, ws);
    resetHeartbeat(peerId);
    if(!ws._isHttpFallback) startPingTimer(peerId);
    broadcastPresence(peerId, true);
    console.log(`Peer ${peerId} registered.`);

    // Отправляем подтверждение регистрации
    send(ws, { type: "registered", peerId: peerId });

    // ── Батчинг событий при reconnect ──
    // Отправляем все накопленные события одним батчем для экономии round-trips
    const events = stmtGetEvents.all(peerId);
    if(events.length > 0) {
      // Группируем события в батч (до 50 за раз)
      const BATCH_SIZE = 50;
      for(let i = 0; i < events.length; i += BATCH_SIZE) {
        const batch = events.slice(i, i + BATCH_SIZE);
        for(const event of batch) {
          send(ws, JSON.parse(event.payload));
          stmtDeleteEvent.run(event.id);
        }
        // Небольшая пауза между батчами чтобы не перегружать медленное соединение
        if(i + BATCH_SIZE < events.length) {
          await new Promise(r => setTimeout(r, 50));
        }
      }
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
    send(ws, { type: "pong", ts: Date.now() });
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
      enqueueEvent(targetPeerId, 'incoming-msg', { type: 'incoming-msg', from: ws._myId, msgId: data.msgId, payload: data.payload });
      if (targetWs && targetWs._isHttpFallback) {
        httpFallbackEnqueue(targetPeerId, { type: "incoming-msg", from: ws._myId, msgId: data.msgId, payload: data.payload });
      }
      // Отправляем ack отправителю что сообщение принято сервером (queued)
      send(ws, { type: "msg-queued", msgId: data.msgId, target: targetPeerId });
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
      } else if(acked.sender) {
        enqueueEvent(acked.sender, 'msg-read', { type: 'msg-read', msgId, by: ws._myId });
      }
    }
    return;
  }

  // ── Подтверждение события ─────────────────
  if (data.type === "ack-event") {
    const { eventId } = data;
    if(eventId) stmtDeleteById.run(eventId);
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
      enqueueEvent(recipientId, 'file-available', { type: 'file-available', fileId, senderId, recipientId, name, size, mimeType, totalChunks, caption, thumb, ts });
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
          enqueueEvent(header.recipient_id, 'file-upload-complete', { type: 'file-upload-complete', fileId });
          if (targetWs && targetWs._isHttpFallback) {
            httpFallbackEnqueue(header.recipient_id, { type: 'file-upload-complete', fileId });
          }
        }
      }
    }
    send(ws, { type: 'store-chunks-ack', fileId, chunkIndex });
    return;
  }

  // ── Батчевая отправка чанков (оптимизация для плохого интернета) ──
  if (data.type === 'store-chunks') {
    const { fileId, chunks } = data;
    if(!Array.isArray(chunks) || !fileId) return;
    const header = stmtGetHeader.get(fileId);
    if(!header) { send(ws, { type: 'store-chunks-error', fileId, msg: 'Header not found' }); return; }
    // Вставляем все чанки батчем
    const insertBatch = db.transaction((chunks) => {
      for(const chunk of chunks) {
        stmtInsertChunk.run(fileId, chunk.index, chunk.data, Date.now());
      }
    });
    try {
      insertBatch(chunks);
    } catch(e) {
      console.error('[File] Batch insert error:', e.message);
    }
    const { cnt } = stmtCountChunks.get(fileId);
    send(ws, { type: 'file-chunks-update', fileId, chunksReady: cnt, totalChunks: header.total_chunks });
    if(cnt === header.total_chunks) {
      const targetWs = peers.get(header.recipient_id);
      if(targetWs && targetWs.readyState === WebSocket.OPEN) {
        send(targetWs, { type: 'file-upload-complete', fileId });
      } else {
        enqueueEvent(header.recipient_id, 'file-upload-complete', { type: 'file-upload-complete', fileId });
      }
    }
    return;
  }

  if (data.type === 'fetch-file') {
    const { fileId, fromIndex } = data;
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

    // ── Возобновляемая загрузка: начинаем с fromIndex ──
    // Если клиент уже скачал часть чанков — не отправляем их повторно
    const startIndex = (typeof fromIndex === 'number' && fromIndex > 0) ? fromIndex : 0;

    // Отправляем чанки с небольшой паузой между ними для медленных соединений
    for (let i = startIndex; i < header.total_chunks; i++) {
      const chunk = db.prepare('SELECT data FROM file_chunks WHERE file_id=? AND chunk_index=?').get(fileId, i);
      if (chunk) {
        send(ws, { type: 'file-data-chunk', fileId, chunkIndex: i, data: chunk.data });
        // Микро-пауза каждые 10 чанков чтобы не перегружать буфер медленного соединения
        if((i - startIndex) % 10 === 9 && i < header.total_chunks - 1) {
          await new Promise(r => setTimeout(r, 10));
        }
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
    stmtDeleteFileAvail.run(ws._myId, fileId);
    // Уведомляем отправителя, что файл доставлен
    const header = stmtGetHeader.get(fileId);
    if (header) {
      const senderWs = peers.get(header.sender_id);
      if (senderWs && senderWs.readyState === WebSocket.OPEN) {
        send(senderWs, { type: 'file-delivered', fileId, recipientId: ws._myId });
      } else if(header.sender_id) {
        enqueueEvent(header.sender_id, 'file-delivered', { type: 'file-delivered', fileId, recipientId: ws._myId });
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
          } else {
            enqueueEvent(memberId, 'group-member-added', { type: 'group-member-added', groupId: group.id, peerId: ws._myId });
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
          enqueueEvent(memberId, 'incoming-group-msg', { type: 'incoming-group-msg', groupId, from: ws._myId, msgId, payload });
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
  ws._myId = null;
  ws._stealthKey = null;
  ws._stealthTxCounter = 0;
  ws._stealthRxCounter = 0;
  ws._stealthSid = null;
  ws._lastPong = true;

  // ── TCP keepalive на WS-сокете ──
  if(ws._socket) {
    ws._socket.setKeepAlive(true, 15000); // keepalive каждые 15 секунд
    ws._socket.setNoDelay(true);          // отключаем алгоритм Нагла для низкой задержки
  }

  // Обработка нативного pong (ответ на наш ping)
  ws.on('pong', () => {
    ws._lastPong = true;
    if(ws._pongTimeout) {
      clearTimeout(ws._pongTimeout);
      ws._pongTimeout = null;
    }
    if(ws._myId) resetHeartbeat(ws._myId);
  });

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
        ws._stealthTxCounter = currentStealthSession.txCounter || 0;
        ws._stealthRxCounter = currentStealthSession.rxCounter || 0;
      }
    }

    if (message instanceof Buffer) {
      // Пробуем декодировать как Stealth-фрейм
      if (ws._stealthKey) {
        parsedMessage = await stealthDecode(message, ws._stealthKey);
        if (parsedMessage) {
          ws._stealthRxCounter += message.length;
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

  ws.on("close", (code, reason) => {
    console.log(`Client ${ws._myId || 'unknown'} disconnected. Code: ${code}`);
    if (ws._myId) {
      peers.delete(ws._myId);
      broadcastPresence(ws._myId, false);
      if (heartbeats.has(ws._myId)) clearTimeout(heartbeats.get(ws._myId));
      stopPingTimer(ws._myId);
    }
    if(ws._pongTimeout) clearTimeout(ws._pongTimeout);
    rateLimits.delete(ws);
  });

  ws.on("error", (error) => {
    console.error("WebSocket error:", error.message);
    // Не даём необработанной ошибке упасть весь процесс
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

// ── Graceful shutdown ──
process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM received, closing gracefully...');
  wss.close(() => {
    healthServer.close(() => {
      db.close();
      process.exit(0);
    });
  });
});

process.on('uncaughtException', (err) => {
  console.error('[Server] Uncaught exception:', err.message, err.stack);
  // Не падаем при некритических ошибках
});

process.on('unhandledRejection', (reason) => {
  console.error('[Server] Unhandled rejection:', reason);
});

// Export for testing or other modules if needed
module.exports = { healthServer, wss, stealthEncode, stealthDecode, stealthSessions, sendMessageToPeer, serverRsaKeyPair };
