const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

// --- НАСТРОЙКИ SUPABASE (ТВОИ КЛЮЧИ) ---
const SUPABASE_URL = 'https://lnrrnhpzudcsyjijbjrh.supabase.co';
const SUPABASE_ANON_KEY = 'sb_publishable_Q-1rp2b2aWmw5TpNC7Lw7Q_luaAVrUY';
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// --- НАСТРОЙКИ БЭКАПА ---
const BACKUP_BUCKET = 'backups';          // имя бакета, который ты создал в Supabase Storage
const BACKUP_FILE_NAME = 'offline_messages.json';

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

// Локальный файл с офлайн-очередью
const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');
let queue = {};

// --- ФУНКЦИИ ДЛЯ РАБОТЫ С БЭКАПОМ В SUPABASE ---

// Скачать бэкап из Supabase (если локальный файл отсутствует)
async function downloadBackup() {
  try {
    const { data, error } = await supabase
      .storage
      .from(BACKUP_BUCKET)
      .download(BACKUP_FILE_NAME);

    if (error || !data) {
      console.log('[backup] Бэкап в Supabase не найден, начинаем с пустой очереди.');
      return null;
    }

    const text = await data.text();
    const parsed = JSON.parse(text);
    console.log('[backup] Очередь загружена из Supabase.');
    return parsed;
  } catch (e) {
    console.error('[backup] Ошибка загрузки из Supabase:', e.message);
    return null;
  }
}

// Загрузить текущую очередь в Supabase (перезаписывая старый файл)
async function uploadBackup(queueData) {
  try {
    const blob = new Blob([JSON.stringify(queueData)], { type: 'application/json' });
    const { error } = await supabase
      .storage
      .from(BACKUP_BUCKET)
      .upload(BACKUP_FILE_NAME, blob, { upsert: true });   // upsert: true -> перезаписывает файл, если он уже есть

    if (error) throw error;
    console.log('[backup] Очередь загружена в Supabase.');
  } catch (e) {
    console.error('[backup] Ошибка загрузки в Supabase:', e.message);
  }
}

// --- ИНИЦИАЛИЗАЦИЯ ОЧЕРЕДИ ПРИ ЗАПУСКЕ СЕРВЕРА ---
async function initQueue() {
  try {
    if (fs.existsSync(STORAGE_FILE)) {
      // Локальный файл есть — загружаем из него
      queue = JSON.parse(fs.readFileSync(STORAGE_FILE, 'utf8'));
      console.log('[server] Очередь загружена с локального диска.');
    } else {
      // Локального файла нет — пробуем скачать из Supabase
      const backup = await downloadBackup();
      if (backup) {
        queue = backup;
        fs.writeFileSync(STORAGE_FILE, JSON.stringify(queue));
        console.log('[server] Очередь восстановлена из бэкапа Supabase.');
      } else {
        queue = {};
        console.log('[server] Создана новая пустая очередь.');
      }
    }
  } catch (e) {
    console.error('[server] Критическая ошибка загрузки очереди:', e);
    queue = {};
  }
}

// Сохранить очередь локально И отправить бэкап в Supabase
function persist() {
  try {
    // Сохраняем локально
    fs.writeFileSync(STORAGE_FILE, JSON.stringify(queue));
    // Отправляем в Supabase (асинхронно, чтобы не тормозить сервер)
    uploadBackup(queue).catch(e => console.error('[backup] Ошибка асинхронной загрузки:', e));
  } catch (e) {
    console.error('[server] Ошибка сохранения очереди:', e);
  }
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ WEBSOCKET ---
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for (const [, ws] of peers) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

// --- ОСНОВНАЯ ЛОГИКА WEBSOCKET (ТВОЙ СТАРЫЙ ПРОВЕРЕННЫЙ КОД) ---
wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── REGISTER ────────────────────────────────────────────────
    if (data.type === 'register') {
      myId = (data.peerId || '').toLowerCase();
      if (!myId) return;
      peers.set(myId, ws);
      console.log(`[${myId}] registered`);

      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);

      // Доставка накопленных офлайн-сообщений
      const pending = queue[myId] || [];
      if (pending.length > 0) {
        console.log(`[${myId}] delivering ${pending.length} queued items`);
        for (const m of pending) {
          if (m.payload) {
            send(ws, { type: 'incoming-msg', from: m.from, msgId: m.msgId, payload: m.payload });
          } else if (m.type === 'voice-listened') {
            send(ws, { type: 'voice-listened', from: m.from, voiceMsgId: m.voiceMsgId });
          }
        }
        // Удаляем доставленные сообщения из очереди
        queue[myId] = pending.filter(m => m.payload && !m.ephemeral);
        if (!queue[myId].length) delete queue[myId];
        persist();
      }
      return;
    }

    // ── SEND-MSG ────────────────────────────────────────────────
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      const targetWs = peers.get(target);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        // Получатель онлайн – доставляем сразу
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        console.log(`[${myId}] → [${target}] live delivery`);
        if (!ephemeral) {
          if (!queue[target]) queue[target] = [];
          if (!queue[target].find(m => m.msgId === msgId)) {
            queue[target].push({ msgId, from: myId, payload, ts: Date.now(), ephemeral: false });
            persist();
          }
        }
      } else {
        // Получатель офлайн – сохраняем в очередь
        if (!ephemeral) {
          if (!queue[target]) queue[target] = [];
          if (!queue[target].find(m => m.msgId === msgId)) {
            queue[target].push({ msgId, from: myId, payload, ts: Date.now(), ephemeral: false });
            persist();
            console.log(`[${myId}] → [${target}] queued (offline)`);
          }
        }
      }
      return;
    }

    // ── ACK-MSG ────────────────────────────────────────────────
    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      if (!msgId) return;

      const pending = queue[myId] || [];
      const msgObj = pending.find(m => m.msgId === msgId);
      const senderId = msgObj?.from;

      if (queue[myId]) {
        queue[myId] = queue[myId].filter(m => m.msgId !== msgId);
        if (!queue[myId].length) delete queue[myId];
        persist();
      }

      console.log(`[${myId}] ACK ${msgId}, sender=${senderId}`);

      if (senderId) {
        const senderWs = peers.get(senderId);
        if (senderWs && senderWs.readyState === WebSocket.OPEN) {
          send(senderWs, { type: 'msg-delivered', msgId, by: myId });
        }
      }
      return;
    }

    // ── QUERY-PRESENCE ─────────────────────────────────────────
    if (data.type === 'query-presence') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const isOnline = peers.has(target);
      send(ws, { type: 'presence-reply', target, online: isOnline });
      return;
    }

    // ── VOICE-LISTENED ─────────────────────────────────────────
    if (data.type === 'voice-listened') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const targetWs = peers.get(target);

      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        send(targetWs, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId });
        console.log(`[${myId}] → [${target}] voice-listened live`);
      } else {
        if (!queue[target]) queue[target] = [];
        queue[target].push({
          type: 'voice-listened',
          from: myId,
          voiceMsgId: data.voiceMsgId,
          ts: Date.now()
        });
        persist();
        console.log(`[${myId}] → [${target}] voice-listened queued (offline)`);
      }
      return;
    }

    // ── Legacy signal ──────────────────────────────────────────
    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        send(targetWs, { type: 'signal', from: myId, payload: data.payload });
      }
    }
  });

  ws.on('close', () => {
    if (myId) {
      peers.delete(myId);
      console.log(`[${myId}] disconnected`);
      broadcastPresence(myId, false);
    }
  });

  ws.on('error', e => console.error('ws error', e.message));
});

// --- ЗАПУСК СЕРВЕРА ---
initQueue().then(() => {
  console.log(`[server] BloodyChat running on port ${process.env.PORT || 3000}`);
});
