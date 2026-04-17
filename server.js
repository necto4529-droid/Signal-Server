const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');
const TMP_FILE = STORAGE_FILE + '.tmp';

// Атомарная запись JSON
function atomicWrite(filePath, data) {
  try {
    fs.writeFileSync(TMP_FILE, JSON.stringify(data));
    fs.renameSync(TMP_FILE, filePath);
  } catch (e) {
    console.error('[server] atomicWrite error', e);
  }
}

// Загрузка очереди
let queue = {};
try {
  if (fs.existsSync(STORAGE_FILE)) {
    queue = JSON.parse(fs.readFileSync(STORAGE_FILE, 'utf8'));
    console.log('[server] Loaded offline queue from disk');
  }
} catch (e) {
  console.error('[server] Failed to load queue', e);
}

function persist() {
  atomicWrite(STORAGE_FILE, queue);
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

// --- Push-подписки (без изменений) ---
const pushSubscriptionsFile = path.join(__dirname, 'push_subscriptions.json');
let pushSubscriptions = {};
try {
  if (fs.existsSync(pushSubscriptionsFile)) {
    pushSubscriptions = JSON.parse(fs.readFileSync(pushSubscriptionsFile, 'utf8'));
  }
} catch (e) {}

const ONESIGNAL_APP_ID = 'c5b0ecd0-3e67-47a0-823d-771a7c4de3be';
const ONESIGNAL_REST_API_KEY = 'os_v2_app_ywyozub6m5d2bar5o4nhytpdxzr72sz2khuemruxqbapncfalaxcwfqlqoxvcenyxr6sa5uvelsbqwpwrwihgdpwn4ectomaup5byuq';

async function sendPushNotification(userId, message) {
  const playerId = pushSubscriptions[userId];
  if (!playerId) return;
  try {
    await fetch('https://onesignal.com/api/v1/notifications', {
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
    console.log(`[push] Уведомление для ${userId}`);
  } catch (e) {}
}

// Вспомогательные функции для работы с очередью
function generateEventId() {
  return 'evt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
}

function enqueueEvent(recipientId, type, payload) {
  if (!queue[recipientId]) queue[recipientId] = [];
  const eventId = generateEventId();
  queue[recipientId].push({
    id: eventId,
    type: type,
    payload: payload,
    ts: Date.now()
  });
  persist();
  return eventId;
}

function removeEvent(recipientId, eventId) {
  if (!queue[recipientId]) return false;
  const before = queue[recipientId].length;
  queue[recipientId] = queue[recipientId].filter(e => e.id !== eventId);
  if (queue[recipientId].length === 0) delete queue[recipientId];
  if (before !== queue[recipientId]?.length || 0) {
    persist();
    return true;
  }
  return false;
}

function getAndClearEvents(recipientId, confirmedEventIds) {
  if (!queue[recipientId]) return [];
  const events = queue[recipientId];
  // Удаляем только те события, которые клиент подтвердил
  if (confirmedEventIds && confirmedEventIds.length > 0) {
    queue[recipientId] = events.filter(e => !confirmedEventIds.includes(e.id));
    if (queue[recipientId].length === 0) delete queue[recipientId];
    persist();
  }
  return events;
}

wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // --- REGISTER ---
    if (data.type === 'register') {
      myId = (data.peerId || '').toLowerCase();
      if (!myId) return;
      peers.set(myId, ws);
      console.log(`[${myId}] registered`);

      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);

      // Отправляем все события, но НЕ удаляем их (удаление только по подтверждению)
      const pending = queue[myId] || [];
      if (pending.length > 0) {
        console.log(`[${myId}] delivering ${pending.length} queued events`);
        for (const ev of pending) {
          send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
        }
      }
      return;
    }

    // --- REGISTER-PUSH ---
    if (data.type === 'register-push') {
      if (!myId) return;
      const { playerId } = data;
      if (!playerId) return;
      pushSubscriptions[myId] = playerId;
      fs.writeFileSync(pushSubscriptionsFile, JSON.stringify(pushSubscriptions));
      console.log(`[push] ${myId} subscribed`);
      return;
    }

    // --- SEND-MSG ---
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      const targetWs = peers.get(target);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        console.log(`[${myId}] → [${target}] live`);
        if (!ephemeral) {
          enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
        }
      } else {
        if (!ephemeral) {
          enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
          console.log(`[${myId}] → [${target}] queued`);
        }
        sendPushNotification(target, 'Новое сообщение').catch(console.warn);
      }
      return;
    }

    // --- ACK-MSG (подтверждение получения сообщения) ---
    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      if (!msgId) return;

      // Найти событие incoming-msg с таким msgId
      const events = queue[myId] || [];
      const ev = events.find(e => e.type === 'incoming-msg' && e.payload.msgId === msgId);
      if (ev) {
        removeEvent(myId, ev.id);
        console.log(`[${myId}] ACK ${msgId}, removed from queue`);

        // Уведомить отправителя о доставке
        const senderId = ev.payload.from;
        if (senderId) {
          const senderWs = peers.get(senderId);
          const deliveryEventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
          if (senderWs && senderWs.readyState === WebSocket.OPEN) {
            send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId: deliveryEventId });
            // Если отправитель онлайн, можно сразу удалить событие из его очереди (после подтверждения от него)
            // Пока оставим – клиент отправит подтверждение
          }
        }
      }
      return;
    }

    // --- ACK-EVENT (подтверждение получения любого события) ---
    if (data.type === 'ack-event') {
      if (!myId) return;
      const { eventId } = data;
      if (!eventId) return;
      removeEvent(myId, eventId);
      console.log(`[${myId}] ACK event ${eventId}`);
      return;
    }

    // --- QUERY-PRESENCE ---
    if (data.type === 'query-presence') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const isOnline = peers.has(target);
      send(ws, { type: 'presence-reply', target, online: isOnline });
      return;
    }

    // --- VOICE-LISTENED ---
    if (data.type === 'voice-listened') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const targetWs = peers.get(target);

      const eventId = enqueueEvent(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        send(targetWs, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
        console.log(`[${myId}] → [${target}] voice-listened live`);
      } else {
        console.log(`[${myId}] → [${target}] voice-listened queued`);
      }
      return;
    }

    // --- Legacy signal ---
    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs) send(targetWs, { type: 'signal', from: myId, payload: data.payload });
    }
  });

  ws.on('close', () => {
    if (myId) {
      peers.delete(myId);
      console.log(`[${myId}] disconnected`);
      broadcastPresence(myId, false);
    }
  });
});

console.log(`[server] Atomic queue + event ACKs running on ${process.env.PORT || 3000}`);
