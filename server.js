const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

// --- Твои ключи Supabase ---
const SUPABASE_URL = 'https://lnrrnhpzudcsyjijbjrh.supabase.co';
const SUPABASE_ANON_KEY = 'sb_publishable_Q-1rp2b2aWmw5TpNC7Lw7Q_luaAVrUY';
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');
let queue = {};

// --- ФУНКЦИИ БЭКАПА В ТАБЛИЦУ SUPABASE ---
async function downloadBackup() {
  try {
    const { data, error } = await supabase
      .from('backups')
      .select('data')
      .eq('id', 1)
      .maybeSingle();

    if (error || !data) {
      console.log('[backup] Бэкап в Supabase не найден, начинаем с пустой очереди.');
      return null;
    }
    console.log('[backup] Очередь загружена из таблицы Supabase.');
    return data.data;
  } catch (e) {
    console.error('[backup] Ошибка загрузки из Supabase:', e.message);
    return null;
  }
}

async function uploadBackup(queueData) {
  try {
    const { error } = await supabase
      .from('backups')
      .upsert({ id: 1, data: queueData, updated_at: new Date() });

    if (error) throw error;
    console.log('[backup] Очередь загружена в таблицу Supabase.');
  } catch (e) {
    console.error('[backup] Ошибка загрузки в Supabase:', e.message);
  }
}

// --- ИНИЦИАЛИЗАЦИЯ ОЧЕРЕДИ ---
async function initQueue() {
  try {
    if (fs.existsSync(STORAGE_FILE)) {
      queue = JSON.parse(fs.readFileSync(STORAGE_FILE, 'utf8'));
      console.log('[server] Очередь загружена с локального диска.');
    } else {
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

function persist() {
  try {
    fs.writeFileSync(STORAGE_FILE, JSON.stringify(queue));
    uploadBackup(queue).catch(e => console.error('[backup] Ошибка асинхронной загрузки:', e));
  } catch (e) {
    console.error('[server] Ошибка сохранения очереди:', e);
  }
}

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
        queue[myId] = pending.filter(m => m.payload && !m.ephemeral);
        if (!queue[myId].length) delete queue[myId];
        persist();
      }
      return;
    }

    // ── SEND-MSG (с подтверждением msg-queued) ─────────────────
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      // Сразу подтверждаем отправителю, что сообщение принято сервером
      send(ws, { type: 'msg-queued', msgId });

      const targetWs = peers.get(target);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
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
