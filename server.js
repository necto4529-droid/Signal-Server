const WebSocket = require('ws');
const http = require('http');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const sharp = require('sharp');
const https = require('https');
const { spawn } = require('child_process');
const os = require('os');

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
    compressed   INTEGER DEFAULT 0 NOT NULL,
    caption      TEXT DEFAULT \'\',
    ts           INTEGER NOT NULL,
    created_at   INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_file_headers_recipient ON file_headers (recipient_id);

  CREATE TABLE IF NOT EXISTS file_chunks (
    file_id     TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    data        BLOB NOT NULL,
    compressed  INTEGER DEFAULT 0 NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (file_id, chunk_index)
  );
  CREATE INDEX IF NOT EXISTS idx_file_chunks_file ON file_chunks (file_id);
`);

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
    (file_id, sender_id, recipient_id, name, size, mime_type, total_chunks, compressed, caption, ts, created_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
`);
const stmtInsertChunk     = db.prepare(`
  INSERT OR REPLACE INTO file_chunks (file_id, chunk_index, data, compressed, created_at)
  VALUES (?,?,?,?,?)
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

async function optimizeImage(inputBuffer, mimeType) {
  try {
    let image = sharp(inputBuffer);
    const metadata = await image.metadata();

    // Максимальный размер для изображений (например, 1280px по большей стороне)
    const MAX_IMAGE_DIMENSION = 1280;

    if (metadata.width > MAX_IMAGE_DIMENSION || metadata.height > MAX_IMAGE_DIMENSION) {
      image = image.resize(MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION, { fit: sharp.fit.inside, withoutEnlargement: true });
    }

    // Оптимизация в зависимости от типа
    if (mimeType === 'image/jpeg') {
      image = image.jpeg({ quality: 80, progressive: true });
    } else if (mimeType === 'image/png') {
      image = image.png({ compressionLevel: 9, adaptiveFiltering: true });
    } else if (mimeType === 'image/webp') {
      image = image.webp({ quality: 80 });
    }
    // Для других типов (gif, svg) пока не оптимизируем, возвращаем оригинал
    if (['image/gif', 'image/svg+xml'].includes(mimeType)) {
      return inputBuffer;
    }

    return await image.toBuffer();
  } catch (e) {
    console.error('[Image Optimize] Error:', e);
    return inputBuffer; // В случае ошибки возвращаем оригинал
  }
}

async function optimizeVideoWithFFmpeg(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    // ── ИДЕАЛЬНАЯ ПЛАВНОСТЬ И ОПТИМАЛЬНЫЙ ВЕС (Telegram-уровень) ──
    // ── ИДЕАЛЬНАЯ ПЛАВНОСТЬ И ОПТИМАЛЬНЫЙ ВЕС (Telegram-уровень) ──
    // Переходим на H.264 с жестким контролем FPS и битрейта.
    // - libx264: лучший кодек для широкой совместимости и контроля.
    // - -preset veryfast: быстрый, но качественный пресет.
    // - -profile:v main: широкая совместимость.
    // - -crf 23: оптимальный баланс качества и размера для 720p.
    // - -g 30: GOP (Group of Pictures) каждые 30 кадров для лучшей перемотки.
    // - -keyint_min 30: минимальный интервал между ключевыми кадрами.
    // - -sc_threshold 0: не создавать ключевые кадры при смене сцены (для стабильности).
    // - -r 30: принудительно 30 FPS для плавности.
    // - -b:v 400k: целевой битрейт 400 kbps (примерно 3 МБ/мин).
    // - -maxrate 600k: максимальный битрейт.
    // - -bufsize 800k: размер буфера.
    // - -pix_fmt yuv420p: для лучшей совместимости.
    // - -movflags +faststart: для быстрой загрузки в браузере.
    const ffmpegArgs = [
      '-i', inputPath,
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-profile:v', 'main',
      '-crf', '23', // Оптимальный баланс качества и размера для 720p
      '-g', '30',
      '-keyint_min', '30',
      '-sc_threshold', '0',
      '-r', '30',
      '-b:v', '400k', // Целевой битрейт 400 kbps (~3 МБ/мин)
      '-maxrate', '600k', // Максимальный битрейт
      '-bufsize', '800k', // Размер буфера
      '-pix_fmt', 'yuv420p', // Для лучшей совместимости
      '-movflags', '+faststart', // Для быстрой загрузки в браузере
      '-c:a', 'libopus',
      '-b:a', '128k',
      '-ar', '48000',
      '-ac', '1',
      '-filter:v', 'fps=fps=30:round=near,scale=640:640:force_original_aspect_ratio=increase,crop=640:640', // Жесткая стабилизация FPS и квадрат для кружка
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
const heartbeats = new Map();
const HEARTBEAT_TIMEOUT = 600_000;   // 10 минут

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

function send(ws, obj) {
  if(ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function flushPriorityQueue(userId) {
  const ws = peers.get(userId);
  if(!ws || ws.readyState !== WebSocket.OPEN) return;
  const queue = priorityQueues.get(userId) || [];
  while(queue.length > 0) {
    const msg = queue.shift();
    try { ws.send(JSON.stringify(msg)); } catch(e) { break; }
  }
  if(queue.length === 0) priorityQueues.delete(userId);
  else priorityQueues.set(userId, queue);
}

function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for(const [, ws] of peers) if(ws.readyState === WebSocket.OPEN) ws.send(msg);
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

// ─── Соединения ──────────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  ws._socket.setTimeout(0);
  ws._socket.setNoDelay(true);
  ws._socket.setKeepAlive(true, 60000);

  let myId = null;

  ws.on('message', async (message) => {
    if (!checkRateLimit(ws)) {
      ws.close(4001, 'Rate limit exceeded');
      return;
    }

    let data;
    try { data = JSON.parse(raw); } catch { return; }

    if(myId) resetHeartbeat(myId);

    // ── Регистрация ──────────────────────────────────────────────────────────
    if(data.type === 'register') {
      const newId = (data.peerId || '').toLowerCase().trim();
      if(!newId) return;
      const old = peers.get(newId);
      if(old && old !== ws && old.readyState === WebSocket.OPEN) old.close();
      myId = newId;
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
        if(cnt && cnt.cnt >= h.total_chunks) {
          send(ws, {
            type: 'file-available',
            fileId: h.file_id,
            senderId: h.sender_id,
            name: h.name,
            size: h.size,
            mimeType: h.mime_type,
            totalChunks: h.total_chunks,
            caption: h.caption,
            ts: h.ts
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

    // ── file‑available‑ack ────────────────────────────────────────────────
    if(data.type === 'file-available-ack') {
      if(!myId) return;
      const header = stmtGetHeader.get(data.fileId);
      if(header && header.recipient_id === myId) {
        const deliveryPayload = { fileId: data.fileId, by: myId };
        const eventId = enqueueEvent(header.sender_id, 'file-delivered', deliveryPayload);
        const senderWs = peers.get(header.sender_id);
        if(senderWs) send(senderWs, { type: 'file-delivered', ...deliveryPayload, eventId });
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
        data.mimeType, data.totalChunks, 0, data.caption || 
        data.ts || Date.now(), Date.now()
      );
      send(ws, { type: 'store-file-header-ack', fileId: data.fileId });
      return;
    }

    if(data.type === 'store-chunks') {
      if(!myId) return;
      const chunks = data.chunks;
      if(!Array.isArray(chunks)) return;

      let isCompressed = 0;
      let isImage = false;
      let isText = false;

      // Определяем тип файла для оптимизации/сжатия
      if (header.mime_type.startsWith("image/") && !header.name.includes("__vnote__")) {
        isImage = true;
      } else if (["text/plain", "application/json", "text/html", "text/css", "text/javascript"].includes(header.mime_type)) {
        isText = true;
      }

      if (isImage) {
        const rawBuffer = Buffer.concat(chunks.map(c => Buffer.from(c.data, 'base64')));
        const optimizedBuffer = await optimizeImage(rawBuffer, header.mime_type);
        const newTotalChunks = Math.ceil(optimizedBuffer.length / (256 * 1024));

        db.transaction(() => {
          stmtDeleteChunks.run(data.fileId);
          db.prepare(`UPDATE file_headers SET size=?, total_chunks=?, compressed=? WHERE file_id=?`).run(optimizedBuffer.length, newTotalChunks, 0, data.fileId);
          for (let i = 0; i < newTotalChunks; i++) {
            const start = i * (256 * 1024);
            const chunk = optimizedBuffer.slice(start, start + (256 * 1024));
            stmtInsertChunk.run(data.fileId, i, chunk.toString('base64'), 0, Date.now()); // 0 = not compressed, as sharp handles compression
          }
        })();
      } else if (isText) {
        for(const chunk of chunks) {
          const compressedData = zlib.gzipSync(Buffer.from(chunk.data, 'base64'));
          stmtInsertChunk.run(data.fileId, chunk.index, compressedData.toString('base64'), 1, Date.now());
        }
        isCompressed = 1;
      } else {
        for(const chunk of chunks) {
          stmtInsertChunk.run(data.fileId, chunk.index, chunk.data, 0, Date.now());
        }
      }

      const header = stmtGetHeader.get(data.fileId);
      if(!header) return;
      const cnt = stmtCountChunks.get(data.fileId);
      if(cnt && cnt.cnt >= header.total_chunks) {
        let finalPayload = null;
        let inputPath = null;
        let outputPath = null;

        // ── Пост-обработка видео-кружков на сервере ──
        if (header.name.includes('__vnote__')) {
          try {
            const rawChunks = db.prepare(`SELECT * FROM file_chunks WHERE file_id=? ORDER BY chunk_index`).all(data.fileId);
            const buffer = Buffer.concat(rawChunks.map(c => Buffer.from(c.data, 'base64')));
            inputPath = await saveTemporaryFile(buffer, '.webm');
            outputPath = inputPath.replace('.webm', '-optimized.mp4'); // Изменяем расширение на mp4
            
            await optimizeVideoWithFFmpeg(inputPath, outputPath);
            
            const optimizedData = fs.readFileSync(outputPath);
            const newTotalChunks = Math.ceil(optimizedData.length / (256 * 1024)); // Используем CHUNK_SIZE из клиента

            // Обновляем хедер и чанки
            db.transaction(() => {
              stmtDeleteChunks.run(data.fileId);
              db.prepare(`UPDATE file_headers SET size=?, total_chunks=?, mime_type=?, compressed=? WHERE file_id=?`).run(optimizedData.length, newTotalChunks, 'video/mp4', 0, data.fileId);
              for (let i = 0; i < newTotalChunks; i++) {
                const start = i * (256 * 1024);
                const chunk = optimizedData.slice(start, start + (256 * 1024));
                stmtInsertChunk.run(data.fileId, i, chunk.toString(\'base64\'), 0, Date.now());
              }
            })();
            
            const updatedHeader = stmtGetHeader.get(data.fileId);
            finalPayload = {
              fileId: data.fileId,
              senderId: header.sender_id,
              name: updatedHeader.name,
              size: updatedHeader.size,
              mimeType: updatedHeader.mime_type,
              totalChunks: updatedHeader.total_chunks,
              caption: updatedHeader.caption,
              ts: updatedHeader.ts
            };
            
          } catch (e) {
            console.error('[VN-Optimize] Error:', e);
            // Если ошибка — отправляем как есть (оригинальный файл)
            finalPayload = { fileId: data.fileId, senderId: header.sender_id, name: header.name, size: header.size, mimeType: header.mime_type, totalChunks: header.total_chunks, caption: header.caption, ts: header.ts };
          } finally {
            cleanupTemporaryFile(inputPath);
            cleanupTemporaryFile(outputPath);
          }
        } else { // Для обычных файлов и голосовых сообщений
          finalPayload = {
            fileId: data.fileId,
            senderId: myId,
            name: header.name,
            size: header.size,
            mimeType: header.mime_type,
            totalChunks: header.total_chunks,
            compressed: isCompressed,
            caption: header.caption,
            ts: header.ts
          };
        }

        // Отправляем file-available только после завершения всех операций
        if (finalPayload) {
          const eventId = enqueueEvent(header.recipient_id, 'file-available', finalPayload);
          const recipientWs = peers.get(header.recipient_id);
          if(recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            flushPriorityQueue(header.recipient_id);
            send(recipientWs, { type: 'file-available', ...finalPayload, eventId });
          } else {
            const queue = priorityQueues.get(header.recipient_id) || [];
            queue.push({ type: 'file-available', ...finalPayload, eventId });
            priorityQueues.set(header.recipient_id, queue);
            sendPush(header.recipient_id, `📎 ${header.name}`);
          }
        }
      }
      return;
    }

    if(data.type === 'fetch-file') {
      if(!myId) return;
      const fileId = data.fileId;
      const header = stmtGetHeader.get(fileId);
      if(!header) { send(ws, { type: 'file-fetch-error', fileId, msg: 'File not found' }); return; }
      if(header.recipient_id !== myId) { send(ws, { type: 'file-fetch-error', fileId, msg: 'Access denied' }); return; }

      const chunks = db.prepare(`SELECT * FROM file_chunks WHERE file_id=? ORDER BY chunk_index`).all(fileId);
      if(chunks.length < header.total_chunks) {
        send(ws, { type: 'file-fetch-partial', fileId, received: chunks.length, total: header.total_chunks });
        return;
      }

      send(ws, { type: 'file-data-header', fileId, name: header.name, size: header.size, mimeType: header.mime_type, totalChunks: header.total_chunks, compressed: header.compressed });
      for(const chunk of chunks) {
        let chunkData = chunk.data;
        if (header.compressed) {
          chunkData = zlib.gunzipSync(Buffer.from(chunk.data, 'base64')).toString('base64');
        }
        send(ws, { type: 'file-data-chunk', fileId, index: chunk.chunk_index, data: chunkData });
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
        const queue = priorityQueues.get(target) || [];
        queue.push({ type: 'incoming-msg', from: myId, msgId, payload, eventId });
        priorityQueues.set(target, queue);
        sendPush(target, `💬 ${myId}`);
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
    if(myId) {
      clearTimeout(heartbeats.get(myId));
      heartbeats.delete(myId);
      if(peers.get(myId) === ws) { peers.delete(myId); broadcastPresence(myId, false); }
    }
  });
  ws.on('error', (err) => console.error('WS error:', err.message));
});

// ─── Health‑check /stats endpoint ────────────────────────────────────────────
const STATS_TOKEN = process.env.STATS_TOKEN || '';
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
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      activeConnections: peers.size,
      queuedEvents: eventCount
    }));
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
