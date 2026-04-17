const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

// Офлайн-очередь сообщений
const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');
let queue = {};

// Push-подписки
const PUSH_SUBS_FILE = path.join(__dirname, 'push_subscriptions.json');
let pushSubscriptions = {}; // userId -> playerId

try {
  if (fs.existsSync(STORAGE_FILE)) {
    queue = JSON.parse(fs.readFileSync(STORAGE_FILE, 'utf8'));
    console.log('[server] Loaded offline queue from disk');
  }
} catch (e) {
  console.error('[server] Failed to load queue', e);
}

try {
  if (fs.existsSync(PUSH_SUBS_FILE)) {
    pushSubscriptions = JSON.parse(fs.readFileSync(PUSH_SUBS_FILE, 'utf8'));
    console.log('[server] Loaded push subscriptions from disk');
  }
} catch (e) {
  console.error('[server] Failed to load push subscriptions', e);
}

function persistQueue() {
  try { fs.writeFileSync(STORAGE_FILE, JSON.stringify(queue)); }
  catch (e) { console.error('[server] persist queue error', e); }
}

function persistPushSubs() {
  try { fs.writeFileSync(PUSH_SUBS_FILE, JSON.stringify(pushSubscriptions)); }
  catch (e) { console.error('[server] persist push subs error', e); }
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify(obj));
}

function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for (const [, ws] of peers) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

// --- Настройки OneSignal ---
const ONESIGNAL_APP_ID = 'c5b0ecd0-3e67-47a0-823d-771a7c4de3be';
const ONESIGNAL_REST_API_KEY = 'os_v2_app_ywyozub6m5d2bar5o4nhytpdx3m4534ns6ju3kudjv33pr4d26pdc2q54mewvbiihqhnhkdsiqacc4immi4ec7ymjn2t4mrakaf6juq';

async function sendPushNotification(userId, message) {
  const playerId = pushSubscriptions[userId];
  if (!playerId) {
    console.log(`[push] Нет подписки для ${userId}`);
    return;
  }

  try {
    const response = await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${ONESIGNAL_REST_API_KEY}`
      },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_player_ids: [playerId],
        contents: { "en": message },
        headings: { "en": "K-Chat" }
      })
    });

    const data = await response.json();
    if (data.errors) {
      console.error('[push] Ошибка OneSignal:', data.errors);
    } else {
      console.log(`[push] Уведомление отправлено для ${userId}`);
    }
  } catch (e) {
    console.error('[push] Исключение при отправке:', e.message);
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
        persistQueue();
      }
      return;
    }

    // ── REGISTER-PUSH (сохранение подписки) ─────────────────────
    if (data.type === 'register-push') {
      if (!myId) return;
      const { playerId } = data;
      if (!playerId) return;

      pushSubscriptions[myId] = playerId;
      persistPushSubs();
      console.log(`[push] Подписка сохранена для ${myId}`);
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
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        console.log(`[${myId}] → [${target}] live delivery`);
        if (!ephemeral) {
          if (!queue[target]) queue[target] = [];
          if (!queue[target].find(m => m.msgId === msgId)) {
            queue[target].push({ msgId, from: myId, payload, ts: Date.now(), ephemeral: false });
            persistQueue();
          }
        }
      } else {
        if (!ephemeral) {
          if (!queue[target]) queue[target] = [];
          if (!queue[target].find(m => m.msgId === msgId)) {
            queue[target].push({ msgId, from: myId, payload, ts: Date.now(), ephemeral: false });
            persistQueue();
            console.log(`[${myId}] → [${target}] queued (offline)`);
          }
        }
        // Отправляем пуш-уведомление получателю (если он офлайн)
        sendPushNotification(target, 'У вас новое сообщение').catch(console.warn);
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
        persistQueue();
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
      const targetWs = peers.get(target);
      const isOnline = !!(targetWs && targetWs.readyState === WebSocket.OPEN);
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
        persistQueue();
        console.log(`[${myId}] → [${target}] voice-listened queued (offline)`);
      }
      return;
    }

    // ── Legacy signal ──────────────────────────────────────────
    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs && targetWs.readyState === WebSocket.OPEN)
        send(targetWs, { type: 'signal', from: myId, payload: data.payload });
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

console.log(`[server] BloodyChat + OneSignal running on port ${process.env.PORT || 3000}`);
