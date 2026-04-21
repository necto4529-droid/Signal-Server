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

  CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    avatar TEXT,
    admin_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    password_hash TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(user_id);
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
    await fetch('https://onesignal.com/api/v1/notifications', {
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

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();
const MAX_GROUP_MEMBERS = 20;

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
  console.log(`[enqueue] ${recipientId} <- ${type} (id=${info.lastInsertRowid})`);
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

// --- Групповые функции ---
function getGroupMembers(groupId) {
  return db.prepare(`SELECT user_id FROM group_members WHERE group_id = ?`).all(groupId).map(r => r.user_id);
}

function isGroupMember(groupId, userId) {
  const row = db.prepare(`SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?`).get(groupId, userId);
  return !!row;
}

function addGroupMember(groupId, userId) {
  db.prepare(`INSERT OR IGNORE INTO group_members (group_id, user_id, joined_at) VALUES (?, ?, ?)`).run(groupId, userId, Date.now());
}

function removeGroupMember(groupId, userId) {
  db.prepare(`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`).run(groupId, userId);
}

function broadcastToGroup(groupId, senderId, message) {
  const members = getGroupMembers(groupId);
  members.forEach(memberId => {
    if (memberId === senderId) return;
    const ws = peers.get(memberId);
    if (ws) {
      send(ws, message);
    } else {
      enqueueEvent(memberId, message.type, message);
    }
  });
}

wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── register ──
    if (data.type === 'register') {
      const newId = (data.peerId || '').toLowerCase();
      if (!newId) return;

      if (peers.has(newId)) {
        const oldWs = peers.get(newId);
        if (oldWs && oldWs !== ws && oldWs.readyState === WebSocket.OPEN) {
          console.log(`[${newId}] duplicate connection, closing old one`);
          oldWs.close();
        }
      }

      myId = newId;
      peers.set(myId, ws);
      console.log(`[${myId}] registered`);
      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);
      resetHeartbeat(myId);

      const events = getEvents(myId);
      if (events.length) console.log(`[${myId}] delivering ${events.length} events`);
      for (const ev of events) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
      return;
    }

    // ── ping ──
    if (data.type === 'ping') {
      if (!myId) return;
      resetHeartbeat(myId);
      return;
    }

    // ── register-push ──
    if (data.type === 'register-push') {
      if (!myId) return;
      const { playerId } = data;
      if (playerId) {
        pushSubscriptions[myId] = playerId;
        fs.writeFileSync(pushSubscriptionsFile, JSON.stringify(pushSubscriptions));
      }
      return;
    }

    // ── create-group ──
    if (data.type === 'create-group') {
      if (!myId) return;
      const { groupId, name, description, avatar, passwordHash } = data;
      if (!groupId || !name || !passwordHash) return;
      try {
        db.prepare(`INSERT INTO groups (id, name, description, avatar, admin_id, created_at, password_hash) VALUES (?,?,?,?,?,?,?)`)
          .run(groupId, name, description || '', avatar || '👥', myId, Date.now(), passwordHash);
        addGroupMember(groupId, myId);
        send(ws, { type: 'group-created', groupId });
      } catch (e) {
        send(ws, { type: 'error', message: 'Group already exists' });
      }
      return;
    }

    // ── join-group ──
    if (data.type === 'join-group') {
      if (!myId) return;
      const { inviteCode, passwordHash } = data;
      const group = db.prepare(`SELECT * FROM groups WHERE id = ?`).get(inviteCode);
      if (!group) { send(ws, { type: 'error', message: 'Group not found' }); return; }
      if (group.password_hash !== passwordHash) { send(ws, { type: 'error', message: 'Wrong password' }); return; }
      const members = getGroupMembers(inviteCode);
      if (members.length >= MAX_GROUP_MEMBERS) { send(ws, { type: 'error', message: 'Group full' }); return; }
      addGroupMember(inviteCode, myId);
      send(ws, { type: 'group-joined', group });
      const joinMsg = { type: 'group-member-joined', groupId: inviteCode, userId: myId };
      broadcastToGroup(inviteCode, myId, joinMsg);
      return;
    }

    // ── leave-group ──
    if (data.type === 'leave-group') {
      if (!myId) return;
      const { groupId } = data;
      removeGroupMember(groupId, myId);
      const members = getGroupMembers(groupId);
      if (members.length === 0) {
        db.prepare(`DELETE FROM groups WHERE id = ?`).run(groupId);
      }
      const leaveMsg = { type: 'group-member-left', groupId, userId: myId };
      broadcastToGroup(groupId, myId, leaveMsg);
      return;
    }

    // ── send-msg ──
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      // Проверяем, группа ли это
      const group = db.prepare(`SELECT id FROM groups WHERE id = ?`).get(target);
      if (group) {
        if (!isGroupMember(target, myId)) return;
        const incoming = { type: 'incoming-msg', from: myId, msgId, payload };
        broadcastToGroup(target, myId, incoming);
        if (!ephemeral) {
          const members = getGroupMembers(target);
          members.forEach(memberId => {
            if (memberId !== myId) enqueueEvent(memberId, 'incoming-msg', { from: myId, msgId, payload });
          });
        }
        return;
      }

      // Обычный чат 1-1
      const targetWs = peers.get(target);
      if (targetWs) {
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        console.log(`[${myId}] → [${target}] live`);
      } else {
        console.log(`[${myId}] → [${target}] queued`);
        sendPushNotification(target, 'Новое сообщение').catch(() => {});
      }
      if (!ephemeral) enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
      return;
    }

    // ── ack-msg ──
    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      if (!msgId) return;

      console.log(`[ack-msg] from ${myId} for msgId ${msgId}`);
      const senderId = ackIncomingMsg(myId, msgId);

      if (senderId) {
        // Проверяем, является ли senderId группой
        const group = db.prepare(`SELECT id FROM groups WHERE id = ?`).get(senderId);
        if (group) {
          // Групповое сообщение: отправляем msg-read всем участникам
          const readMsg = { type: 'msg-read', msgId, groupId: senderId, readerId: myId };
          broadcastToGroup(senderId, myId, readMsg);
          // Сохраняем в офлайн-очередь для отсутствующих
          const members = getGroupMembers(senderId);
          members.forEach(memberId => {
            if (memberId !== myId) enqueueEvent(memberId, 'msg-read', readMsg);
          });
        } else {
          // Личное сообщение: отправляем msg-delivered отправителю
          const eventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
          const senderWs = peers.get(senderId);
          if (senderWs) {
            send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId });
            console.log(`[ack-msg] sent live msg-delivered to ${senderId}`);
          }
        }
      } else {
        console.log(`[ack-msg] incoming msg ${msgId} not found (already acked?)`);
      }
      return;
    }

    // ── ack-event ──
    if (data.type === 'ack-event') {
      if (!myId) return;
      const { eventId } = data;
      if (eventId) deleteEvent(eventId);
      return;
    }

    // ── query-presence ──
    if (data.type === 'query-presence') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      send(ws, { type: 'presence-reply', target, online: peers.has(target) });
      return;
    }

    // ── voice-listened ──
    if (data.type === 'voice-listened') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const eventId = enqueueEvent(target, 'voice-listened', {
        from: myId,
        voiceMsgId: data.voiceMsgId
      });
      const targetWs = peers.get(target);
      if (targetWs) {
        send(targetWs, {
          type: 'voice-listened',
          from: myId,
          voiceMsgId: data.voiceMsgId,
          eventId
        });
      }
      return;
    }

    // ── signal (WebRTC) ──
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
        console.log(`[${myId}] disconnected (clean)`);
      } else {
        console.log(`[${myId}] disconnected but connection already replaced`);
      }
    }
  });
});

console.log(`[server] SQLite + groups + heartbeat ready on ${process.env.PORT || 3000}`);
