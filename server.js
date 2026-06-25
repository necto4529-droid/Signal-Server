const WebSocket = require('ws');
const http = require('http');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');
const crypto = require('crypto');
const { spawn } = require('child_process');
const os = require('os');

// ─── SQLite WAL ───────────────────────────────────────────────────────────────
// ОПТИМИЗАЦИЯ 2: WAL-режим — параллельное чтение/запись без блокировок
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
    created_at   INTEGER NOT NULL,
    retry_count  INTEGER NOT NULL DEFAULT 0,
    last_retry   INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_recipient ON events (recipient_id);
  CREATE INDEX IF NOT EXISTS idx_events_type ON events (type);

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

// ОПТИМИЗАЦИЯ 2: Миграция — добавляем колонку thumb если её нет (для существующих БД)
try {
  db.exec(`ALTER TABLE file_headers ADD COLUMN thumb TEXT DEFAULT ''`);
} catch(e) {
  // Колонка уже существует — игнорируем ошибку
}

// ГАРАНТИРОВАННАЯ ДОСТАВКА: Миграция — добавляем колонки retry_count и last_retry
// для существующих БД без этих колонок
try {
  db.exec(`ALTER TABLE events ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0`);
} catch(e) { /* Колонка уже существует */ }
try {
  db.exec(`ALTER TABLE events ADD COLUMN last_retry INTEGER NOT NULL DEFAULT 0`);
} catch(e) { /* Колонка уже существует */ }

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

// ОПТИМИЗАЦИЯ 1: stmtInsertHeader теперь включает поле thumb
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
const stmtGetReceivedChunks = db.prepare(`SELECT chunk_index FROM file_chunks WHERE file_id=? ORDER BY chunk_index ASC`);
const stmtGetPendingFiles = db.prepare(`SELECT * FROM file_headers WHERE recipient_id=?`);

const stmtDeleteFileAvail = db.prepare(`
  DELETE FROM events
  WHERE recipient_id=? AND type='file-available'
    AND json_extract(payload,'$.fileId')=?
`);

// ГАРАНТИРОВАННАЯ ДОСТАВКА: Дополнительные prepared statements
// Для повторной отправки недоставленных событий (аналог Telegram)
const stmtGetPendingEvents  = db.prepare(`
  SELECT * FROM events
  WHERE recipient_id=? AND last_retry < ? AND retry_count < 10
  ORDER BY created_at ASC
  LIMIT 50
`);
const stmtUpdateRetry       = db.prepare(`
  UPDATE events SET retry_count=retry_count+1, last_retry=? WHERE id=?
`);
const stmtGetAllPending     = db.prepare(`
  SELECT DISTINCT recipient_id FROM events
  WHERE last_retry < ? AND retry_count < 10
`);

// Дедупликация: проверяем есть ли уже такой же тип события для этого получателя
const stmtCheckDupEvent     = db.prepare(`
  SELECT id FROM events
  WHERE recipient_id=? AND type=? AND json_extract(payload,'$.msgId')=?
  LIMIT 1
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
// Адаптивные пресеты для голосовых сообщений:
// - ultra: 8 kbps / 8 kHz  — EDGE/GPRS (голос весит ~1 КБ/с)
// - low:   16 kbps / 16 kHz — 2G/3G (голос весит ~2 КБ/с)
// - normal: 32 kbps / 24 kHz — 4G/WiFi (стандарт)
async function optimizeAudioWithFFmpeg(inputPath, outputPath, preset = 'normal') {
  return new Promise((resolve, reject) => {
    let bitrate, sampleRate, filterComplex;
    if (preset === 'ultra') {
      // EDGE/GPRS: ультра-сжатие — 8 kbps, 8 kHz (телефонное качество)
      bitrate = '8k'; sampleRate = '8000';
      filterComplex = 'aresample=8000,acompressor=ratio=3:attack=5:release=50:threshold=-20:detection=peak,loudnorm=I=-16:TP=-1.5:LRA=11';
    } else if (preset === 'low') {
      // 2G/3G: низкое сжатие — 16 kbps, 16 kHz
      bitrate = '16k'; sampleRate = '16000';
      filterComplex = 'afftdn=nr=5:nf=-20,acompressor=ratio=2:attack=3:release=30:threshold=-18:detection=peak,loudnorm=I=-16:TP=-1.5:LRA=11';
    } else {
      // 4G/WiFi: стандартное сжатие — 32 kbps, 24 kHz
      bitrate = '32k'; sampleRate = '24000';
      filterComplex = 'afftdn=nr=5:nf=-20:rn=0.005:rf=0.005,adeclick,acompressor=ratio=2:attack=3:release=30:threshold=-18:detection=peak,loudnorm=I=-16:TP=-1.5:LRA=11';
    }
    const ffmpegArgs = [
      '-i', inputPath,
      '-c:a', 'libopus',
      '-b:a', bitrate,
      '-vbr', 'on',
      '-compression_level', '10',
      '-ar', sampleRate,
      '-ac', '1',
      '-application', 'voip',
      '-filter_complex', filterComplex,
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

// Адаптивные пресеты для видео-кружков:
// - ultra: 320x320, 15fps, 80k  — EDGE/GPRS (кружок весит ~10 КБ/с)
// - low:   480x480, 20fps, 120k — 2G/3G
// - normal: 480x480, 20fps, 200k — 4G/WiFi
async function optimizeVideoWithFFmpeg(inputPath, outputPath, preset = 'normal') {
  return new Promise((resolve, reject) => {
    let size, fps, videoBitrate, maxrate, bufsize, audioBitrate, audioRate;
    if (preset === 'ultra') {
      // EDGE/GPRS: минимальный размер и битрейт
      size = '320:320'; fps = '15'; videoBitrate = '80k'; maxrate = '120k'; bufsize = '240k';
      audioBitrate = '8k'; audioRate = '8000';
    } else if (preset === 'low') {
      // 2G/3G: средний размер
      size = '480:480'; fps = '20'; videoBitrate = '120k'; maxrate = '180k'; bufsize = '360k';
      audioBitrate = '16k'; audioRate = '16000';
    } else {
      // 4G/WiFi: стандарт
      size = '480:480'; fps = '20'; videoBitrate = '200k'; maxrate = '300k'; bufsize = '600k';
      audioBitrate = '24k'; audioRate = '24000';
    }
    const ffmpegArgs = [
      '-i', inputPath,
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-profile:v', 'main',
      '-crf', '28',
      '-g', '20',
      '-keyint_min', '20',
      '-sc_threshold', '0',
      '-r', fps,
      '-b:v', videoBitrate,
      '-maxrate', maxrate,
      '-bufsize', bufsize,
      '-pix_fmt', 'yuv420p',
      '-movflags', '+faststart',
      '-c:a', 'libopus',
      '-b:a', audioBitrate,
      '-ar', audioRate,
      '-ac', '1',
      '-filter:v', `fps=fps=${fps}:round=near,scale=${size}:force_original_aspect_ratio=increase,crop=${size}`,
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

// preset определяется по размеру входного файла:
// если голос уже маленький (записан на EDGE) — используем ultra
async function processVoiceMessage(voiceData, preset = 'auto') {
  let inputPath, outputPath;
  try {
    inputPath = await saveTemporaryFile(voiceData, '.webm');
    outputPath = inputPath.replace('.webm', '-optimized.webm');
    // Авто-определение пресета по размеру входного файла:
    // Маленький файл = записан на слабой сети (ultra-сжатие уже применено)
    if (preset === 'auto') {
      const inputSize = fs.statSync(inputPath).size;
      const durationEstSec = inputSize / 2000; // грубая оценка длительности
      const bitrateKbps = (inputSize * 8) / (durationEstSec * 1000);
      if (bitrateKbps < 20) preset = 'ultra';      // уже сжатый — применяем ultra
      else if (bitrateKbps < 50) preset = 'low';   // среднее сжатие
      else preset = 'normal';                       // стандарт
    }
    await optimizeAudioWithFFmpeg(inputPath, outputPath, preset);
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

// ИСПРАВЛЕНИЕ: Очередь для последовательной записи чанков (Race Condition Fix)
const chunkQueues = new Map();
async function enqueueChunkWrite(fileId, writeFn) {
  if (!chunkQueues.has(fileId)) chunkQueues.set(fileId, Promise.resolve());
  const promise = chunkQueues.get(fileId).then(writeFn);
  chunkQueues.set(fileId, promise);
  // Очистка очереди после завершения
  promise.finally(() => { if (chunkQueues.get(fileId) === promise) chunkQueues.delete(fileId); });
  return promise;
}
// ОПТИМИЗАЦИЯ 2: perMessageDeflate — сжатие каждого WS-фрейма (уровень 3, быстрое)
// Для JSON-сообщений даёт 70-80% сжатие. Особенно важно на 2G где каждый байт на счету.
const wss = new WebSocket.Server({
  port: PORT,
  maxPayload: 1024 * 1024 * 1024,  // 1 ГБ
    perMessageDeflate: {
    zlibDeflateOptions: { level: 6 }, // Чуть выше уровень сжатия для экономии трафика
    zlibInflateOptions: { chunkSize: 16 * 1024 },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    threshold: 128 // Сжимаем даже маленькие JSON-пакеты
  },
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
// ФИКС: Подписки на стриминг файлов. fileId -> Set(ws)
// Когда прилетает новый чанк, мы пушим его всем подписчикам.
const activeFetchers = new Map();
const heartbeats = new Map();
// ОПТИМИЗАЦИЯ 5: HEARTBEAT_TIMEOUT = 90с (было 600с = 10 минут!)
// На 2G/3G соединение может «зависнуть» без явного разрыва — теперь это обнаруживается за 90с.
// АДАПТИВНЫЙ ТАЙМАУТ: Увеличиваем до 120с для 2G, чтобы не разрывать связь при долгих задержках
const HEARTBEAT_TIMEOUT = 120_000;

// Rate limiting: не более 30 сообщений в секунду с одного подключения
const rateLimits = new Map();
function checkRateLimit(ws) {
  const now = Date.now();
  const window = 1000; // 1 секунда
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

// ─── МАСКИРОВКА ТРАФИКА (BINARY OBFUSCATION) ────────────────────────────────
const MAGIC_BYTE = 0x4B; // 'K'

function encodeMessage(obj) {
  const json = JSON.stringify(obj);
  const data = Buffer.from(json, 'utf8');
  const keyLen = 4 + Math.floor(Math.random() * 8); // Динамический ключ 4-12 байт
  const key = crypto.randomBytes(keyLen);
  
  const buffer = Buffer.alloc(2 + keyLen + data.length);
  buffer[0] = MAGIC_BYTE;
  buffer[1] = keyLen;
  key.copy(buffer, 2);
  
  for (let i = 0; i < data.length; i++) {
    buffer[2 + keyLen + i] = data[i] ^ key[i % keyLen];
  }
  return buffer;
}

function decodeMessage(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 2 || buffer[0] !== MAGIC_BYTE) {
    return null;
  }
  const keyLen = buffer[1];
  if (buffer.length < 2 + keyLen) return null;
  
  const key = buffer.slice(2, 2 + keyLen);
  const encryptedData = buffer.slice(2 + keyLen);
  const data = Buffer.alloc(encryptedData.length);
  
  for (let i = 0; i < encryptedData.length; i++) {
    data[i] = encryptedData[i] ^ key[i % keyLen];
  }
  
  try {
    return JSON.parse(data.toString('utf8'));
  } catch (e) {
    return null;
  }
}

function send(ws, obj) {
  if(ws?.readyState === WebSocket.OPEN) {
    ws.send(encodeMessage(obj));
  }
}

function flushPriorityQueue(userId) {
  const ws = peers.get(userId);
  if(!ws || ws.readyState !== WebSocket.OPEN) return;
  const queue = priorityQueues.get(userId) || [];
  while(queue.length > 0) {
    const msg = queue.shift();
    try { ws.send(encodeMessage(msg)); } catch(e) { break; }
  }
  if(queue.length === 0) priorityQueues.delete(userId);
  else priorityQueues.set(userId, queue);
}

function broadcastPresence(peerId, isOnline) {
  const msgObj = { type: 'presence', peerId, online: isOnline };
  const encoded = encodeMessage(msgObj);
  for(const [, ws] of peers) if(ws.readyState === WebSocket.OPEN) ws.send(encoded);
}
function resetHeartbeat(peerId) {
  if(heartbeats.has(peerId)) clearTimeout(heartbeats.get(peerId));
  heartbeats.set(peerId, setTimeout(() => { peers.get(peerId)?.close(); }, HEARTBEAT_TIMEOUT));
}
// ГАРАНТИРОВАННАЯ ДОСТАВКА: Улучшенный enqueueEvent с дедупликацией
// Для сообщений (тип incoming-msg) проверяем чтобы не дублировать одинаковые msgId
function enqueueEvent(recipientId, type, payload) {
  // Дедупликация для incoming-msg: если сообщение с таким msgId уже есть в очереди — не добавляем
  if (type === 'incoming-msg' && payload.msgId) {
    const dup = stmtCheckDupEvent.get(recipientId, type, payload.msgId);
    if (dup) {
      console.log(`[Dedup] Event already queued for ${recipientId}: ${type}/${payload.msgId}`);
      return dup.id;
    }
  }
  return stmtInsertEvent.run(recipientId, type, JSON.stringify(payload), Date.now()).lastInsertRowid;
}

// ГАРАНТИРОВАННАЯ ДОСТАВКА: Отправка всех недоставленных событий пользователю при подключении
// Аналог Telegram: все недоставленные сообщения приходят при первом же подключении
function flushEventsToUser(userId, ws) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const now = Date.now();
  // Берём все события которые ещё не были доставлены (retry_count < 10)
  const events = stmtGetEvents.all(userId);
  for (const ev of events) {
    try {
      const payload = JSON.parse(ev.payload);
      send(ws, { type: ev.type, ...payload, eventId: ev.id });
      stmtUpdateRetry.run(now, ev.id);
    } catch(e) {
      console.error('[FlushEvents] Error sending event', ev.id, e.message);
    }
  }
}

// ГАРАНТИРОВАННАЯ ДОСТАВКА: Периодическая повторная отправка для онлайн пользователей
// Если пользователь онлайн, но не отправил ack-event — повторяем отправку через 30 секунд
const RETRY_INTERVAL_MS = 30_000; // 30 секунд
setInterval(() => {
  const now = Date.now();
  const cutoff = now - RETRY_INTERVAL_MS;
  // Находим всех пользователей у которых есть недоставленные события
  const pendingUsers = stmtGetAllPending.all(cutoff);
  for (const { recipient_id } of pendingUsers) {
    const userWs = peers.get(recipient_id);
    if (userWs && userWs.readyState === WebSocket.OPEN) {
      // Пользователь онлайн — повторяем отправку недоставленных событий
      const events = stmtGetPendingEvents.all(recipient_id, cutoff);
      for (const ev of events) {
        try {
          const payload = JSON.parse(ev.payload);
          send(userWs, { type: ev.type, ...payload, eventId: ev.id });
          stmtUpdateRetry.run(now, ev.id);
        } catch(e) {
          console.error('[Retry] Error resending event', ev.id, e.message);
        }
      }
      if (events.length > 0) {
        console.log(`[Retry] Resent ${events.length} events to online user ${recipient_id}`);
      }
    }
  }
}, RETRY_INTERVAL_MS);

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

// ─── Соединения ──────────────────────────────────────────────────────────────
// ОПТИМИЗАЦИЯ 4: Активный ping/pong — сервер шлёт нативный WS ping каждые 60с,
// ждёт pong 40с. На мобильных сетях (2G/3G) задержки могут быть большие.
// ИСПРАВЛЕНИЕ: Увеличили интервалы, чтобы избежать ложных разрывов соединения.
const PING_INTERVAL = 60_000;  // 60 секунд (было 30)
const PONG_TIMEOUT  = 40_000;  // 40 секунд (было 20)

wss.on('connection', (ws) => {
  // ОПТИМИЗАЦИЯ 3: TCP keepalive — быстрое обнаружение «зависших» соединений
  ws._socket.setTimeout(0);
  ws._socket.setNoDelay(true);      // Отключаем алгоритм Нагла — меньше задержка пакетов
  ws._socket.setKeepAlive(true, 15000); // Кеепалайв каждые 15с (было 60с)

  // ОПТИМИЗАЦИЯ 4: Активный ping/pong
  let _pingTimer = null;
  let _pongTimer = null;
  function _schedulePing() {
    _pingTimer = setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      try { ws.ping(); } catch(e) {}
      _pongTimer = setTimeout(() => {
        console.log('[Ping] No pong received, terminating connection');
        try { ws.terminate(); } catch(e) {}
      }, PONG_TIMEOUT);
    }, PING_INTERVAL);
  }
  ws.on('pong', () => {
    if(_pongTimer) clearTimeout(_pongTimer);
    _pongTimer = null;
    _schedulePing();
  });
  _schedulePing();

  let myId = null;

    // УЛЬТИМАТИВНАЯ ОПТИМИЗАЦИЯ: Request-Response Deduplication
    const processedRequests = new Set();
    
    ws.on('message', async (raw) => {
      if (!checkRateLimit(ws)) {
        ws.close(4001, 'Rate limit exceeded');
        return;
      }
  
      let data;
      // Пробуем расшифровать бинарное сообщение
      if (Buffer.isBuffer(raw)) {
        data = decodeMessage(raw);
      } else {
        // Если пришел текст (для обратной совместимости или ошибок), пробуем JSON
        try { data = JSON.parse(raw); } catch { return; }
      }
      if (!data) return;

      // Дедупликация: если этот requestId уже обрабатывался в рамках текущей сессии — игнорируем
      if (data.requestId) {
        if (processedRequests.has(data.requestId)) {
          console.log(`[Dedup] Skipping duplicate request: ${data.requestId}`);
          return;
        }
        processedRequests.add(data.requestId);
        // Ограничиваем размер кэша дедупликации
        if (processedRequests.size > 500) {
          const first = processedRequests.values().next().value;
          processedRequests.delete(first);
        }
      }

    // ИСПРАВЛЕНИЕ: НЕ вызываем resetHeartbeat при каждом сообщении
    // Это конфликтует с активным ping/pong и может привести к разрывам
    // if(myId) resetHeartbeat(myId);

    // ── Регистрация ──────────────────────────────────────────────────────────────
    if (data.type === 'ping') {
      send(ws, { type: 'pong' });
      return;
    }

    if(data.type === 'register' || data.type === 'auth') {
      const newId = (data.peerId || data.id || '').toLowerCase().trim();
      if(!newId) return;
      const old = peers.get(newId);
      if(old && old !== ws && old.readyState === WebSocket.OPEN) {
        // Если заходит тот же пользователь с другого сокета, мягко закрываем старый
        old.onclose = null;
        old.close(1000, 'New session started');
      }
      myId = newId;
      peers.set(myId, ws);
      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);
      // ИСПРАВЛЕНИЕ: Не используем resetHeartbeat — используем только активный ping/pong
      // resetHeartbeat(myId);
  
      flushPriorityQueue(myId);
      
      // ГАРАНТИРОВАННАЯ ДОСТАВКА: При подключении отправляем ВСЕ накопленные события
      // Аналог Telegram — все офлайн-сообщения приходят при первом же подключении
      // flushEventsToUser обновляет retry_count и last_retry для каждого события
      flushEventsToUser(myId, ws);

      // ГАРАНТИРОВАННАЯ ДОСТАВКА: при reconnect показываем file-available ТОЛЬКО если оно ЕЩЁ НЕ в очереди events
      // (flushEventsToUser уже отправил его если оно было в очереди)
      // Также отправляем файлы которые ещё загружаются (нет в events, но есть в file_headers)
      const stmtCheckFileAvailInEvents = db.prepare(`
        SELECT id FROM events WHERE recipient_id=? AND type='file-available' AND json_extract(payload,'$.fileId')=? LIMIT 1
      `);
      const pendingFiles = stmtGetPendingFiles.all(myId);
      for(const h of pendingFiles) {
        const cnt = stmtCountChunks.get(h.file_id);
        if(!cnt || cnt.cnt < 1) continue;
        // Проверяем: если file-available уже есть в events — flushEventsToUser уже его отправил, не дублируем
        const alreadyInQueue = stmtCheckFileAvailInEvents.get(myId, h.file_id);
        if (alreadyInQueue) continue;
        const isComplete = cnt.cnt >= h.total_chunks;
        send(ws, {
          type: 'file-available',
          fileId: h.file_id,
          senderId: h.sender_id,
          name: h.name,
          size: h.size,
          mimeType: h.mime_type,
          totalChunks: h.total_chunks,
          caption: h.caption,
          // Миниатюра передаётся ТОЛЬКО если файл полностью загружен
          thumb: isComplete ? (h.thumb || '') : '',
          ts: h.ts,
          chunksReady: cnt.cnt
        });
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

    // ГАРАНТИРОВАННАЯ ДОСТАВКА: Delta-Sync Handler
    // Отправляем все события начиная с sinceId, обновляем retry_count
    if (data.type === 'delta-sync') {
      if (!myId) return;
      const sinceId = parseInt(data.sinceId) || 0;
      const now = Date.now();
      // Выбираем только те события, которые клиент ещё не получал
      const rows = db.prepare('SELECT * FROM events WHERE recipient_id=? AND id > ? ORDER BY id ASC').all(myId, sinceId);
      for (const r of rows) {
        send(ws, { type: r.type, ...JSON.parse(r.payload), eventId: r.id });
        stmtUpdateRetry.run(now, r.id);
      }
      return;
    }

    if (data.type === 'get-file-status') {
      if (!myId) return;
      const header = stmtGetHeader.get(data.fileId);
      if (!header) {
        send(ws, { type: 'file-status', fileId: data.fileId, exists: false });
      } else {
        const received = stmtGetReceivedChunks.all(data.fileId).map(r => r.chunk_index);
        send(ws, { 
          type: 'file-status', 
          fileId: data.fileId, 
          exists: true, 
          receivedChunks: received,
          totalChunks: header.total_chunks
        });
      }
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

    // ── file‑available‑ack ────────────────────────────────────────────────
    if(data.type === 'file-available-ack') {
      if(!myId) return;
      const header = stmtGetHeader.get(data.fileId);
      if(header && header.recipient_id === myId) {
        // Уведомляем отправителя о доставке (✔✔) ТОЛЬКО когда файл полностью загружен на сервер
        // и получатель подтвердил получение заголовка
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
      // ОПТИМИЗАЦИЯ 1: сохраняем thumb из заголовка
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

      // ИСПРАВЛЕНИЕ: Последовательная запись через очередь (Race Condition Fix)
      await enqueueChunkWrite(data.fileId, async () => {
        const runTransaction = db.transaction((chunksData) => {
          for(const chunk of chunksData) {
            stmtInsertChunk.run(data.fileId, chunk.index, chunk.data, Date.now());
          }
        });
        runTransaction(chunks);
      });

      const cnt = stmtCountChunks.get(data.fileId);
      const receivedCount = cnt ? cnt.cnt : 0;

      // ИСПРАВЛЕНИЕ: НЕ отправляем file-available (с миниатюрой) до полной загрузки файла.
      // Пока файл загружается — шлём только file-chunks-update (прогресс без thumb).
      // file-available с thumb отправляется ТОЛЬКО когда receivedCount >= total_chunks
      // (т.е. отправитель полностью загрузил файл на сервер).
      if(receivedCount < header.total_chunks) {
        // Файл ещё не загружен полностью — только обновляем прогресс у получателя
        const recipientWs = peers.get(header.recipient_id);
        if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          send(recipientWs, { type: 'file-chunks-update', fileId: data.fileId, chunksReady: receivedCount, totalChunks: header.total_chunks });
        }
      }

      // Проверяем завершение загрузки
      if(receivedCount >= header.total_chunks) {
        // ── Пост-обработка видео-кружков на сервере ──
        if (header.name.includes('__vnote__')) {
          (async () => {
            let inputPath, outputPath;
            try {
              const chunks = db.prepare(`SELECT * FROM file_chunks WHERE file_id=? ORDER BY chunk_index`).all(data.fileId);
              const buffer = Buffer.concat(chunks.map(c => Buffer.from(c.data, 'base64')));
              inputPath = await saveTemporaryFile(buffer, '.webm');
              outputPath = inputPath.replace('.webm', '-optimized.webm');
              
              // Адаптивный пресет по размеру входного файла:
              // Маленький файл = записан на EDGE/GPRS
              const inputFileSize = Buffer.concat(
                db.prepare(`SELECT data FROM file_chunks WHERE file_id=? ORDER BY chunk_index`)
                  .all(data.fileId).map(c => Buffer.from(c.data, 'base64'))
              ).length;
              let vnPreset = 'normal';
              if (inputFileSize < 500 * 1024) vnPreset = 'ultra';      // < 500 KB = EDGE
              else if (inputFileSize < 2 * 1024 * 1024) vnPreset = 'low'; // < 2 MB = 3G
              await optimizeVideoWithFFmpeg(inputPath, outputPath, vnPreset);
              
              const optimizedData = fs.readFileSync(outputPath);
              // Адаптивный размер чанка при пересборке:
              // Маленький файл = используем 8 КБ чанки (для EDGE)
              let reChunkSize;
              if (vnPreset === 'ultra') reChunkSize = 8 * 1024;
              else if (vnPreset === 'low') reChunkSize = 64 * 1024;
              else reChunkSize = 256 * 1024;
              const newTotalChunks = Math.ceil(optimizedData.length / reChunkSize);
              
              // Обновляем хедер и чанки
              const updateVnTransaction = db.transaction((optData, reSize, total) => {
                stmtDeleteChunks.run(data.fileId);
                db.prepare(`UPDATE file_headers SET size=?, total_chunks=? WHERE file_id=?`).run(optData.length, total, data.fileId);
                for (let i = 0; i < total; i++) {
                  const start = i * reSize;
                  const chunk = optData.slice(start, start + reSize);
                  stmtInsertChunk.run(data.fileId, i, chunk.toString('base64'), Date.now());
                }
              });
              updateVnTransaction(optimizedData, reChunkSize, newTotalChunks);
              
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
              
              // Обновляем событие в очереди (удаляем старое, добавляем новое с актуальными данными)
              stmtDeleteFileAvail.run(updatedHeader.recipient_id, data.fileId);
              const eventId = enqueueEvent(updatedHeader.recipient_id, 'file-available', payload);
              const recipientWs = peers.get(updatedHeader.recipient_id);
              if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
                send(recipientWs, { type: 'file-available', ...payload, eventId });
              }
              send(ws, { type: 'file-upload-complete', fileId: data.fileId });
            } catch (e) {
              console.error('[VN-Optimize] Error:', e);
              // Если ошибка — отправляем как есть
              const updHeader = stmtGetHeader.get(data.fileId);
              const payload = { fileId: data.fileId, senderId: header.sender_id, name: header.name, size: header.size, mimeType: header.mime_type, totalChunks: header.total_chunks, caption: header.caption, thumb: header.thumb||'', ts: header.ts, chunksReady: header.total_chunks };
              stmtDeleteFileAvail.run(header.recipient_id, data.fileId);
              const eventId = enqueueEvent(header.recipient_id, 'file-available', payload);
              const recipientWs = peers.get(header.recipient_id);
              if(recipientWs && recipientWs.readyState === WebSocket.OPEN) send(recipientWs, { type: 'file-available', ...payload, eventId });
              send(ws, { type: 'file-upload-complete', fileId: data.fileId });
            } finally {
              cleanupTemporaryFile(inputPath);
              cleanupTemporaryFile(outputPath);
            }
          })();
          return;
        }

        // ФИКС: PUSH-стриминг чанков.
        // Если кто-то сейчас «слушает» этот файл (fetch-file), проталкиваем ему чанки мгновенно.
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

        // ИСПРАВЛЕНИЕ: Файл полностью загружен — ТЕПЕРЬ отправляем file-available с thumb получателю.
        // Миниатюра появляется ТОЛЬКО после полной 100% загрузки отправителя.
        const completePayload = {
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
        // Проверяем: уже ли отправляли file-available этому получателю для этого файла?
        const alreadyNotified = db.prepare(
          `SELECT id FROM events WHERE recipient_id=? AND type='file-available' AND json_extract(payload,'$.fileId')=? LIMIT 1`
        ).get(header.recipient_id, data.fileId);
        if(!alreadyNotified) {
          // Первый раз — сохраняем в очередь и отправляем
          const eventId = enqueueEvent(header.recipient_id, 'file-available', completePayload);
          if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            flushPriorityQueue(header.recipient_id);
            send(recipientWs, { type: 'file-available', ...completePayload, eventId });
          } else {
            const queue = priorityQueues.get(header.recipient_id) || [];
            queue.push({ type: 'file-available', ...completePayload, eventId });
            priorityQueues.set(header.recipient_id, queue);
            sendPush(header.recipient_id, `📎 ${header.name}`);
          }
        } else {
          // Уже уведомляли — обновляем событие в очереди и отправляем обновленное сообщение с thumb
          stmtDeleteFileAvail.run(header.recipient_id, data.fileId);
          const eventId = enqueueEvent(header.recipient_id, 'file-available', completePayload);
          if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            send(recipientWs, { type: 'file-available', ...completePayload, eventId });
          }
        }

        // Отправляем уведомление о завершении загрузки отправителю
        send(ws, { type: 'file-upload-complete', fileId: data.fileId });
        
        // ИСПРАВЛЕНИЕ БАГА: НЕ отправляем file-delivered автоматически при завершении загрузки на сервер.
        // file-delivered должен приходить ТОЛЬКО когда получатель реально скачал файл
        // (через file-available-ack или через зашифрованный сигнал от получателя).
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

      // ФИКС: Подписываем клиента на новые чанки этого файла (PUSH-стриминг)
      // Теперь сервер будет сам «проталкивать» новые чанки по мере их поступления
      if (!activeFetchers.has(fileId)) activeFetchers.set(fileId, new Set());
      activeFetchers.get(fileId).add(ws);

      const chunks = db.prepare(`SELECT * FROM file_chunks WHERE file_id=? ORDER BY chunk_index`).all(fileId);
      
      // Отправляем заголовок с информацией о текущем прогрессе на сервере
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

      // Отправляем те чанки, которые УЖЕ есть в базе
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
          // Это ack от получателя — файл успешно скачан и сохранён
          // ПРОВЕРКА: Удаляем только если файл реально полностью загружен на сервер!
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
          // Это ack от отправителя — НЕ удаляем, файл может быть нужен получателю
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

      // ИСПРАВЛЕНИЕ: ephemeral-сообщения (typing, activity) НЕ сохраняем в БД и НЕ добавляем в очередь.
      // Если получатель офлайн — епхемеральный статус дропаем (как в Telegram).
      if (data.ephemeral) {
        // Только пересылаем если получатель онлайн — в очередь НЕ кладём
        const targetWs = peers.get(target);
        if(targetWs && targetWs.readyState === WebSocket.OPEN) {
          send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        }
        // Если офлайн — молча дропаем (устаревший статус не нужен)
        return;
      }

      // ПРИОРИТЕТ ТЕКСТОВЫХ СООБЩЕНИЙ:
      // Текстовые сообщения отправляются мгновенно, не ждут в очереди за файлами.
      // Определяем является ли сообщение текстовым (пайлоад не содержит файловых данных)
      let isTextOnly = false;
      try {
        const p = typeof payload === 'string' ? JSON.parse(payload) : payload;
        // Текстовое: нет media, voice, file, videoNote
        isTextOnly = !p.media && !p.voice && !p.file && !p.videoNote && !p.sticker;
      } catch(e) {}
      const eventId = enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
      const targetWs = peers.get(target);
      if(targetWs && targetWs.readyState === WebSocket.OPEN) {
        // Текстовые сообщения отправляем мгновенно
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload, eventId, priority: isTextOnly ? 'high' : 'normal' });
        // ГАРАНТИРОВАННАЯ ДОСТАВКА: Обновляем retry_count даже если пользователь онлайн
        // Событие остаётся в БД до получения ack-event от клиента
        stmtUpdateRetry.run(Date.now(), eventId);
      } else {
        const queue = priorityQueues.get(target) || [];
        // Текстовые сообщения добавляем в начало очереди (перед файлами)
        if (isTextOnly) {
          queue.unshift({ type: 'incoming-msg', from: myId, msgId, payload, eventId, priority: 'high' });
        } else {
          queue.push({ type: 'incoming-msg', from: myId, msgId, payload, eventId, priority: 'normal' });
        }
        priorityQueues.set(target, queue);
        sendPush(target, `💬 ${myId}`);
      }
      return;
    }

    // ── ack-msg: получатель реально открыл чат / увидел сообщение ───────────
    if(data.type === 'ack-msg') {
      if(!myId || !data.msgId) return;
      const row = stmtAckMsg.get(myId, data.msgId);
      if(!row) return;
      stmtDeleteById.run(row.id);
      if(!row.sender) return;
      const deliveryPayload = { msgId: data.msgId, by: myId };
      // ГАРАНТИРОВАННАЯ ДОСТАВКА: msg-delivered всегда сохраняется в БД
      // Если отправитель офлайн — он получит уведомление при следующем подключении
      const deliveredEventId = enqueueEvent(row.sender, 'msg-delivered', deliveryPayload);
      const senderWs = peers.get(row.sender);
      if(senderWs && senderWs.readyState === WebSocket.OPEN) {
        send(senderWs, { type: 'msg-delivered', ...deliveryPayload, eventId: deliveredEventId });
        stmtUpdateRetry.run(Date.now(), deliveredEventId);
      }
      console.log(`[Delivery] msg-delivered enqueued for ${row.sender}: msgId=${data.msgId}`);
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

    // ── voice-listened: получатель прослушал голосовое ────────────────────
    if(data.type === 'voice-listened') {
      if(!myId) return;
      const target = (data.target || '').toLowerCase();
      const eventId = enqueueEvent(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
      const tw = peers.get(target);
      if(tw) send(tw, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
      return;
    }

    // ── vn-watched: получатель просмотрел видео-кружок ────────────────────
    if(data.type === 'vn-watched') {
      if(!myId) return;
      const target = (data.target || '').toLowerCase();
      if(!target || !data.vnMsgId) return;
      const eventPayload = { from: myId, vnMsgId: data.vnMsgId };
      const eventId = enqueueEvent(target, 'vn-watched', eventPayload);
      const tw = peers.get(target);
      if(tw) send(tw, { type: 'vn-watched', from: myId, vnMsgId: data.vnMsgId, eventId });
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
        const tw = peers.get(mid); if(tw) send(tw, ev); else enqueueEvent(mid, 'group-updated', ev);
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
        const tw = peers.get(mid); if(tw) send(tw, ev); else enqueueEvent(mid, 'group-updated', ev);
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
      if(tw) send(tw, removedEv); else enqueueEvent(data.targetPeerId, 'group-member-removed', removedEv);
      JSON.parse(updated.members).forEach(mid => {
        const ev = { type: 'group-updated', groupId: data.groupId, members: JSON.parse(updated.members) };
        const mw = peers.get(mid); if(mw) send(mw, ev); else enqueueEvent(mid, 'group-updated', ev);
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
        const mw = peers.get(mid); if(mw) send(mw, ev); else enqueueEvent(mid, 'group-updated', ev);
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
        const mw = peers.get(mid); if(mw) send(mw, ev); else enqueueEvent(mid, 'group-msg-read', ev);
      });
      return;
    }

    if(data.type === 'signal' && data.target) {
      const tw = peers.get(data.target.toLowerCase());
      if(tw) send(tw, { type: 'signal', from: myId, payload: data.payload });
    }
  });

  ws.on('close', () => {
    // Очищаем ping/pong таймеры при закрытии соединения
    if(_pingTimer) clearTimeout(_pingTimer);
    if(_pongTimer) clearTimeout(_pongTimer);
    _pingTimer = null;
    _pongTimer = null;
    if(myId) {
      if(heartbeats.has(myId)) clearTimeout(heartbeats.get(myId));
      heartbeats.delete(myId);
      if(peers.get(myId) === ws) { 
        peers.delete(myId); 
        broadcastPresence(myId, false); 
      }
      
      // ФИКС: Очищаем подписки этого клиента на стриминг файлов
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

// ─── Health‑check /stats endpoint ────────────────────────────────────────────
const STATS_TOKEN = process.env.STATS_TOKEN || '';
// ОПТИМИЗАЦИЯ 6: gzip сжатие HTTP fallback — уменьшает трафик в 3-5 раз
const zlib = require('zlib');
function _sendGzip(req, res, statusCode, headers, body) {
  const acceptEncoding = req.headers['accept-encoding'] || '';
  if (acceptEncoding.includes('gzip')) {
    zlib.gzip(Buffer.isBuffer(body) ? body : Buffer.from(body), (err, compressed) => {
      if (err) {
        res.writeHead(statusCode, headers);
        res.end(body);
        return;
      }
      res.writeHead(statusCode, {
        ...headers,
        'Content-Encoding': 'gzip',
        'Content-Length': compressed.length
      });
      res.end(compressed);
    });
  } else {
    res.writeHead(statusCode, headers);
    res.end(body);
  }
}

const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
  } else if (req.url === '/stats' && STATS_TOKEN) {
    const auth = req.headers['authorization'] || '';
    if (auth !== `Bearer ${STATS_TOKEN}`) {
      res.writeHead(403);
      return res.end('Forbidden');
    }
    const eventCount = db.prepare('SELECT COUNT(*) as cnt FROM events').get().cnt;
    const body = JSON.stringify({
      activeConnections: peers.size,
      queuedEvents: eventCount
    });
    _sendGzip(req, res, 200, { 'Content-Type': 'application/json' }, body);
  } else {
    res.writeHead(404);
    res.end();
  }
});
healthServer.listen(HEALTH_PORT, () => {
  console.log(`[Health] listening on port ${HEALTH_PORT}`);
});

// ─── Мониторинг (каждые 5 минут) ────────────────────────────────────────────
setInterval(() => {
  console.log(`[Stats] Active connections: ${peers.size}`);
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

console.log(`[K-Chat server] ready on port ${PORT}`);
