const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https'); // Модуль для самопинга

// --- БД SQLite с WAL-режимом ---
const dbPath = path.join(__dirname, 'offline_queue.db');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_recipient ON events (recipient_id);
`);

// --- Push-подписки ---
const pushSubscriptionsFile = path.join(__dirname, 'push_subscriptions.json');
let pushSubscriptions = {};
try {
  if (fs.existsSync(pushSubscriptionsFile))
    pushSubscriptions = JSON.parse(fs.readFileSync(pushSubscriptionsFile, 'utf8'));
} catch (e) {}

const ONESIGNAL_APP_ID = 'c5b0ecd0-3e67-47a0-823d-771a7c4de3be';
const ONESIGNAL_REST_API_KEY = 'os_v2_app_ywyozub6m5d2bar5o4nhytpdxzr72sz2khuemruxqbapncfalaxcwfqlqoxvcenyxr6sa5uvelsbqwpwrwihgdpwn4ectomaup5byuq';

async function sendPushNotification(userId, message) {
  const playerId = pushSubscriptions[userId];
  if (!playerId) return;
  try {
    await fetch('https://onesignal.com', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${ONESIGNAL_REST_API_KEY}`
      },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_player_ids: [playerId],
        contents: { en: message },
        headings: { en: 'K-Chat' }
      })
    });
  } catch (e) {}
}

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });
const peers = new Map();

// Heartbeat
const HEARTBEAT_TIMEOUT = 60000;
const heartbeats = new Map();

function send(ws, obj) {
  if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for (const [, ws] of peers) if (ws.readyState === WebSocket.OPEN) ws.send(msg);
}

function resetHeartbeat(peerId) {
  if (heartbeats.has(peerId)) clearTimeout(heartbeats.get(peerId));
  const timeout = setTimeout(() => {
    console.log(`[${peerId}] heartbeat timeout, forcing offline`);
    if (peers.has(peerId)) peers.get(peerId).close();
  }, HEARTBEAT_TIMEOUT);
  heartbeats.set(peerId, timeout);
}

function enqueueEvent(recipientId, type, payload) {
  const stmt = db.prepare(
    `INSERT INTO events (recipient_id, type, payload, created_at) VALUES (?, ?, ?, ?)`
  );
  const info = stmt.run(recipientId, type, JSON.stringify(payload), Date.now());
  return info.lastInsertRowid;
}

function getEvents(recipientId) {
  return db
    .prepare(`SELECT * FROM events WHERE recipient_id = ? ORDER BY created_at`)
    .all(recipientId)
    .map(r => ({ id: r.id, type: r.type, payload: JSON.parse(r.payload) }));
}

function deleteEvent(eventId) {
  db.prepare(`DELETE FROM events WHERE id = ?`).run(eventId);
}

function ackIncomingMsg(recipientId, msgId) {
  const row = db
    .prepare(
      `SELECT id, json_extract(payload, '$.from') as sender
       FROM events
       WHERE recipient_id = ? AND type = 'incoming-msg'
         AND json_extract(payload, '$.msgId') = ?
       LIMIT 1`
    )
    .get(recipientId, msgId);

  if (!row) return null;
  db.prepare(`DELETE FROM events WHERE id = ?`).run(row.id);
  return row.sender;
}

wss.on('connection', (ws, req) => {
  let myId = null;

  // Если это обычный HTTP запрос (для пинга), закрываем соединение корректно
  if (req.headers.upgrade !== 'websocket') {
    ws.terminate();
    return;
  }

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    if (data.type === 'register') {
      const newId = (data.peerId || '').toLowerCase();
      if (!newId) return;

      if (peers.has(newId)) {
        const oldWs = peers.get(newId);
        if (oldWs && oldWs !== ws && oldWs.readyState === WebSocket.OPEN) {
          oldWs.close();
        }
      }

      myId = newId;
      peers.set(myId, ws);
      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);
      resetHeartbeat(myId);

      const events = getEvents(myId);
      for (const ev of events) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
      return;
    }

    if (data.type === 'ping') {
      if (myId) resetHeartbeat(myId);
      return;
    }

    if (data.type === 'register-push') {
      if (!myId) return;
      const { playerId } = data;
      if (playerId) {
        pushSubscriptions[myId] = playerId;
        fs.writeFileSync(pushSubscriptionsFile, JSON.stringify(pushSubscriptions));
      }
      return;
    }

    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      const targetWs = peers.get(target);
      if (targetWs) {
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
      } else {
        sendPushNotification(target, 'Новое сообщение').catch(() => {});
      }
      if (!ephemeral) enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
      return;
    }

    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      const senderId = ackIncomingMsg(myId, msgId);
      if (senderId) {
        const eventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
        const senderWs = peers.get(senderId);
        if (senderWs) send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId });
      }
      return;
    }

    if (data.type === 'ack-event') {
      if (myId && data.eventId) deleteEvent(data.eventId);
      return;
    }

    if (data.type === 'query-presence') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      send(ws, { type: 'presence-reply', target, online: peers.has(target) });
      return;
    }

    if (data.type === 'voice-listened') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const eventId = enqueueEvent(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
      const targetWs = peers.get(target);
      if (targetWs) {
        send(targetWs, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
      }
      return;
    }

    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs) send(targetWs, { type: 'signal', from: myId, payload: data.payload });
    }
  });

  ws.on('close', () => {
    if (myId) {
      if (heartbeats.has(myId)) clearTimeout(heartbeats.get(myId));
      heartbeats.delete(myId);
      if (peers.get(myId) === ws) {
        peers.delete(myId);
        broadcastPresence(myId, false);
      }
    }
  });
});

// --- ЛОГИКА САМОПИНГА (Каждые 4 минуты) ---
const APP_URL = 'https://onrender.com'; 

setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`[Self-Ping] Status: ${res.statusCode} - Keep-alive active`);
  }).on('error', (err) => {
    console.error(`[Self-Ping] Error: ${err.message}`);
  });
}, 4 * 60 * 1000); // 4 минуты

console.log(`[server] SQLite + WebSocket + Self-Ping (4 min) ready on port ${PORT}`);
