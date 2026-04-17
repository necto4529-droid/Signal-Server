const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

// --- База данных SQLite ---
const dbPath = path.join(__dirname, 'offline_queue.db');
const db = new Database(dbPath);

// Создаём таблицу для офлайн-очереди
db.exec(`
  CREATE TABLE IF NOT EXISTS offline_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    timestamp INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_recipient ON offline_queue (recipient_id);
`);

// --- Push-подписки (файл) ---
const pushSubscriptionsFile = path.join(__dirname, 'push_subscriptions.json');
let pushSubscriptions = {};
try {
  if (fs.existsSync(pushSubscriptionsFile)) {
    pushSubscriptions = JSON.parse(fs.readFileSync(pushSubscriptionsFile, 'utf8'));
  }
} catch (e) {}

// --- OneSignal ---
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

// --- WebSocket ---
const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for (const [, ws] of peers) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

// Добавление события в очередь
function enqueue(recipientId, type, payload) {
  const stmt = db.prepare(`
    INSERT INTO offline_queue (recipient_id, type, payload, timestamp)
    VALUES (?, ?, ?, ?)
  `);
  stmt.run(recipientId, type, JSON.stringify(payload), Date.now());
}

// Извлечение и удаление всех событий для пользователя
function dequeueAll(recipientId) {
  const stmt = db.prepare(`SELECT * FROM offline_queue WHERE recipient_id = ? ORDER BY timestamp`);
  const rows = stmt.all(recipientId);
  if (rows.length > 0) {
    const deleteStmt = db.prepare(`DELETE FROM offline_queue WHERE recipient_id = ?`);
    deleteStmt.run(recipientId);
  }
  return rows.map(r => ({ type: r.type, payload: JSON.parse(r.payload) }));
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

      const events = dequeueAll(myId);
      if (events.length > 0) {
        console.log(`[${myId}] delivering ${events.length} queued events`);
        for (const ev of events) {
          send(ws, { type: ev.type, ...ev.payload });
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
          enqueue(target, 'incoming-msg', { from: myId, msgId, payload });
        }
      } else {
        if (!ephemeral) {
          enqueue(target, 'incoming-msg', { from: myId, msgId, payload });
          console.log(`[${myId}] → [${target}] queued`);
        }
        sendPushNotification(target, 'Новое сообщение').catch(console.warn);
      }
      return;
    }

    // --- ACK-MSG ---
    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      if (!msgId) return;

      // Удаляем сообщение из очереди получателя (помечаем как доставленное)
      const deleteStmt = db.prepare(`DELETE FROM offline_queue WHERE recipient_id = ? AND type = 'incoming-msg' AND json_extract(payload, '$.msgId') = ?`);
      const info = deleteStmt.run(myId, msgId);
      
      if (info.changes > 0) {
        // Находим отправителя этого сообщения
        const row = db.prepare(`SELECT json_extract(payload, '$.from') as sender FROM offline_queue WHERE recipient_id = ? AND type = 'incoming-msg' AND json_extract(payload, '$.msgId') = ?`).get(myId, msgId);
        const senderId = row?.sender;
        
        console.log(`[${myId}] ACK ${msgId}, sender=${senderId}`);
        
        if (senderId) {
          const senderWs = peers.get(senderId);
          if (senderWs && senderWs.readyState === WebSocket.OPEN) {
            send(senderWs, { type: 'msg-delivered', msgId, by: myId });
          } else {
            enqueue(senderId, 'msg-delivered', { msgId, by: myId });
            console.log(`[${myId}] msg-delivered queued for ${senderId}`);
          }
        }
      }
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

      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        send(targetWs, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId });
        console.log(`[${myId}] → [${target}] voice-listened live`);
      } else {
        enqueue(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
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

console.log(`[server] BloodyChat SQLite running on ${process.env.PORT || 3000}`);
