const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

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
try { if (fs.existsSync(pushSubscriptionsFile)) pushSubscriptions = JSON.parse(fs.readFileSync(pushSubscriptionsFile, 'utf8')); } catch (e) {}

const ONESIGNAL_APP_ID = 'c5b0ecd0-3e67-47a0-823d-771a7c4de3be';
const ONESIGNAL_REST_API_KEY = 'os_v2_app_ywyozub6m5d2bar5o4nhytpdxzr72sz2khuemruxqbapncfalaxcwfqlqoxvcenyxr6sa5uvelsbqwpwrwihgdpwn4ectomaup5byuq';

async function sendPushNotification(userId, message) {
  const playerId = pushSubscriptions[userId];
  if (!playerId) return;
  try {
    await fetch('https://onesignal.com/api/v1/notifications', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Basic ${ONESIGNAL_REST_API_KEY}` },
      body: JSON.stringify({ app_id: ONESIGNAL_APP_ID, include_player_ids: [playerId], contents: { "en": message }, headings: { "en": "K-Chat" } })
    });
  } catch (e) {}
}

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

function send(ws, obj) { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for (const [, ws] of peers) if (ws.readyState === WebSocket.OPEN) ws.send(msg);
}

// Добавление события (синхронно)
function enqueueEvent(recipientId, type, payload) {
  const stmt = db.prepare(`INSERT INTO events (recipient_id, type, payload, created_at) VALUES (?, ?, ?, ?)`);
  stmt.run(recipientId, type, JSON.stringify(payload), Date.now());
  console.log(`[enqueue] ${recipientId} <- ${type}`);
}

// Получение событий (без удаления)
function getEvents(recipientId) {
  const stmt = db.prepare(`SELECT * FROM events WHERE recipient_id = ? ORDER BY created_at`);
  return stmt.all(recipientId).map(r => ({ id: r.id, type: r.type, payload: JSON.parse(r.payload) }));
}

// Удаление одного события по ID
function deleteEvent(eventId) {
  db.prepare(`DELETE FROM events WHERE id = ?`).run(eventId);
}

// Удаление входящего сообщения по msgId (для ack-msg)
function deleteIncomingMsg(recipientId, msgId) {
  const stmt = db.prepare(`DELETE FROM events WHERE recipient_id = ? AND type = 'incoming-msg' AND json_extract(payload, '$.msgId') = ?`);
  return stmt.run(recipientId, msgId).changes > 0;
}

function getSenderOfMsg(recipientId, msgId) {
  const row = db.prepare(`SELECT json_extract(payload, '$.from') as sender FROM events WHERE recipient_id = ? AND type = 'incoming-msg' AND json_extract(payload, '$.msgId') = ? LIMIT 1`).get(recipientId, msgId);
  return row?.sender;
}

wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    if (data.type === 'register') {
      myId = (data.peerId || '').toLowerCase();
      if (!myId) return;
      peers.set(myId, ws);
      console.log(`[${myId}] registered`);
      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);

      // Отправляем все события (НЕ удаляя)
      const events = getEvents(myId);
      if (events.length) console.log(`[${myId}] delivering ${events.length} events`);
      for (const ev of events) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
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
        console.log(`[${myId}] → [${target}] live`);
      } else {
        console.log(`[${myId}] → [${target}] queued`);
        sendPushNotification(target, 'Новое сообщение').catch(()=>{});
      }
      if (!ephemeral) enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
      return;
    }

    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      if (!msgId) return;
      if (deleteIncomingMsg(myId, msgId)) {
        const senderId = getSenderOfMsg(myId, msgId);
        console.log(`[${myId}] ACK ${msgId}, sender=${senderId}`);
        if (senderId) {
          const eventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
          const senderWs = peers.get(senderId);
          if (senderWs) send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId });
        }
      }
      return;
    }

    if (data.type === 'ack-event') {
      if (!myId) return;
      const { eventId } = data;
      if (eventId) deleteEvent(eventId);
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
      const targetWs = peers.get(target);
      const eventId = enqueueEvent(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
      if (targetWs) send(targetWs, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
      return;
    }

    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs) send(targetWs, { type: 'signal', from: myId, payload: data.payload });
    }
  });

  ws.on('close', () => {
    if (myId) {
      peers.delete(myId);
      broadcastPresence(myId, false);
    }
  });
});

console.log(`[server] SQLite ready on ${process.env.PORT || 3000}`);
