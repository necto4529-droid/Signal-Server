const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');

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

db.exec(`
  CREATE TABLE IF NOT EXISTS groups (
    group_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    avatar TEXT DEFAULT '👥',
    creator_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT DEFAULT 'member',
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id)
  );
  CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members (user_id);
  
  CREATE TABLE IF NOT EXISTS group_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_group_events_recipient ON group_events (recipient_id);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS group_messages (
    msg_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_group_messages_group ON group_messages (group_id);
`);

// Push-подписки
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

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });
const peers = new Map();

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
    if (peers.has(peerId)) peers.get(peerId).close();
  }, HEARTBEAT_TIMEOUT);
  heartbeats.set(peerId, timeout);
}

function isGroupMember(groupId, userId) {
  const row = db.prepare(`SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?`).get(groupId, userId);
  return !!row;
}

function getGroupMembers(groupId) {
  return db.prepare(`SELECT user_id, role FROM group_members WHERE group_id = ?`).all(groupId);
}

function enqueueGroupEvent(recipientId, groupId, type, payload) {
  const stmt = db.prepare(
    `INSERT INTO group_events (recipient_id, group_id, type, payload, created_at) VALUES (?, ?, ?, ?, ?)`
  );
  stmt.run(recipientId, groupId, type, JSON.stringify(payload), Date.now());
}

function getGroupEvents(recipientId) {
  return db.prepare(`SELECT * FROM group_events WHERE recipient_id = ? ORDER BY created_at`).all(recipientId)
    .map(r => ({ id: r.id, type: r.type, payload: JSON.parse(r.payload) }));
}

function deleteGroupEvent(eventId) {
  db.prepare(`DELETE FROM group_events WHERE id = ?`).run(eventId);
}

function deleteAllGroupEventsForUserInGroup(recipientId, groupId) {
  db.prepare(`DELETE FROM group_events WHERE recipient_id = ? AND group_id = ?`).run(recipientId, groupId);
}

function saveGroupMessageInfo(msgId, groupId, senderId) {
  db.prepare(`INSERT OR IGNORE INTO group_messages (msg_id, group_id, sender_id, created_at) VALUES (?, ?, ?, ?)`)
    .run(msgId, groupId, senderId, Date.now());
}

function markGroupMessagesRead(groupId, readerId) {
  const messages = db.prepare(`
    SELECT msg_id, sender_id FROM group_messages 
    WHERE group_id = ? AND sender_id != ?
  `).all(groupId, readerId);
  for (const msg of messages) {
    const senderWs = peers.get(msg.sender_id);
    if (senderWs) {
      send(senderWs, { type: 'group-msg-read', groupId, msgId: msg.msg_id, reader: readerId });
    }
  }
  db.prepare(`DELETE FROM group_messages WHERE group_id = ? AND sender_id != ?`).run(groupId, readerId);
}

// Вспомогательные функции для личных сообщений
function enqueueEvent(recipientId, type, payload) {
  const stmt = db.prepare(`INSERT INTO events (recipient_id, type, payload, created_at) VALUES (?, ?, ?, ?)`);
  stmt.run(recipientId, type, JSON.stringify(payload), Date.now());
}

function getEvents(recipientId) {
  return db.prepare(`SELECT * FROM events WHERE recipient_id = ? ORDER BY created_at`).all(recipientId)
    .map(r => ({ id: r.id, type: r.type, payload: JSON.parse(r.payload) }));
}

function deleteEvent(eventId) {
  db.prepare(`DELETE FROM events WHERE id = ?`).run(eventId);
}

function ackIncomingMsg(recipientId, msgId) {
  const row = db.prepare(`
    SELECT id, json_extract(payload, '$.from') as sender
    FROM events WHERE recipient_id = ? AND type = 'incoming-msg' AND json_extract(payload, '$.msgId') = ? LIMIT 1
  `).get(recipientId, msgId);
  if (!row) return null;
  db.prepare(`DELETE FROM events WHERE id = ?`).run(row.id);
  return row.sender;
}

// Обработчик WebSocket
wss.on('connection', (ws, req) => {
  let myId = null;

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
        if (oldWs && oldWs !== ws && oldWs.readyState === WebSocket.OPEN) oldWs.close();
      }
      myId = newId;
      peers.set(myId, ws);
      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);
      resetHeartbeat(myId);

      const events = getEvents(myId);
      for (const ev of events) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
      const groupEvents = getGroupEvents(myId);
      for (const ev of groupEvents) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
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

    // Личные сообщения
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
      if (targetWs) send(targetWs, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
      return;
    }

    // Группы
    if (data.type === 'create-group') {
      if (!myId) return;
      const { groupId, name, description, avatar } = data;
      if (!groupId || !name) return;
      try {
        db.prepare(`INSERT INTO groups (group_id, name, description, avatar, creator_id, created_at) VALUES (?, ?, ?, ?, ?, ?)`)
          .run(groupId, name, description || '', avatar || '👥', myId, Date.now());
        db.prepare(`INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'admin', ?)`)
          .run(groupId, myId, Date.now());
        send(ws, { type: 'group-created', groupId, name });
      } catch (e) {
        send(ws, { type: 'error', message: 'Group creation failed' });
      }
      return;
    }

    if (data.type === 'join-group') {
      if (!myId) return;
      const { groupId } = data;
      if (!groupId) return;
      const group = db.prepare(`SELECT * FROM groups WHERE group_id = ?`).get(groupId);
      if (!group) { send(ws, { type: 'error', message: 'Group not found' }); return; }
      const members = getGroupMembers(groupId);
      if (members.length >= 20) { send(ws, { type: 'error', message: 'Group is full' }); return; }
      if (members.find(m => m.user_id === myId)) { send(ws, { type: 'error', message: 'Already a member' }); return; }
      db.prepare(`INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)`)
        .run(groupId, myId, Date.now());
      send(ws, { type: 'group-joined', groupId, name: group.name, avatar: group.avatar });
      for (const member of members) {
        const memberWs = peers.get(member.user_id);
        if (memberWs) send(memberWs, { type: 'group-member-joined', groupId, userId: myId });
      }
      return;
    }

    if (data.type === 'get-group-info') {
      if (!myId) return;
      const { groupId } = data;
      if (!groupId) return;
      const group = db.prepare(`SELECT * FROM groups WHERE group_id = ?`).get(groupId);
      if (!group) { send(ws, { type: 'error', message: 'Group not found' }); return; }
      const members = getGroupMembers(groupId);
      // Здесь можно добавить имена пользователей из контактов, но для простоты вернём id
      send(ws, {
        type: 'group-info',
        groupId,
        name: group.name,
        description: group.description,
        avatar: group.avatar,
        members: members.map(m => ({ id: m.user_id, role: m.role }))
      });
      return;
    }

    if (data.type === 'send-group-msg') {
      if (!myId) return;
      const { groupId, msgId, payload } = data;
      if (!groupId || !msgId || !payload) return;
      if (!isGroupMember(groupId, myId)) { send(ws, { type: 'error', message: 'Not a member' }); return; }
      saveGroupMessageInfo(msgId, groupId, myId);
      const members = getGroupMembers(groupId);
      const group = db.prepare(`SELECT name FROM groups WHERE group_id = ?`).get(groupId);
      const groupName = group?.name || groupId;
      for (const member of members) {
        if (member.user_id === myId) continue;
        const targetWs = peers.get(member.user_id);
        const eventPayload = { groupId, from: myId, msgId, payload, groupName };
        if (targetWs) {
          send(targetWs, { type: 'group-msg', ...eventPayload });
        } else {
          enqueueGroupEvent(member.user_id, groupId, 'group-msg', eventPayload);
          sendPushNotification(member.user_id, `👥 ${groupName}: новое сообщение`).catch(() => {});
        }
      }
      return;
    }

    if (data.type === 'ack-group-msg') {
      if (!myId) return;
      const { groupId, msgId } = data;
      const row = db.prepare(`
        SELECT id FROM group_events 
        WHERE recipient_id = ? AND type = 'group-msg' AND json_extract(payload, '$.msgId') = ?
      `).get(myId, msgId);
      if (row) db.prepare(`DELETE FROM group_events WHERE id = ?`).run(row.id);
      return;
    }

    if (data.type === 'mark-group-read') {
      if (!myId) return;
      const { groupId } = data;
      if (!groupId || !isGroupMember(groupId, myId)) return;
      markGroupMessagesRead(groupId, myId);
      return;
    }

    if (data.type === 'leave-group') {
      if (!myId) return;
      const { groupId } = data;
      if (!groupId) return;
      db.prepare(`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`).run(groupId, myId);
      deleteAllGroupEventsForUserInGroup(myId, groupId);
      const remaining = getGroupMembers(groupId);
      if (remaining.length === 0) {
        db.prepare(`DELETE FROM groups WHERE group_id = ?`).run(groupId);
        db.prepare(`DELETE FROM group_messages WHERE group_id = ?`).run(groupId);
        db.prepare(`DELETE FROM group_events WHERE group_id = ?`).run(groupId);
      } else {
        const wasAdmin = db.prepare(`SELECT role FROM group_members WHERE group_id = ? AND user_id = ?`).get(groupId, myId);
        if (wasAdmin?.role === 'admin') {
          const newAdmin = remaining[0];
          db.prepare(`UPDATE group_members SET role = 'admin' WHERE group_id = ? AND user_id = ?`).run(groupId, newAdmin.user_id);
        }
        for (const member of remaining) {
          const memberWs = peers.get(member.user_id);
          if (memberWs) send(memberWs, { type: 'group-member-left', groupId, userId: myId });
        }
      }
      send(ws, { type: 'group-left', groupId });
      return;
    }

    if (data.type === 'remove-member') {
      if (!myId) return;
      const { groupId, userId } = data;
      if (!groupId || !userId) return;
      const memberRow = db.prepare(`SELECT role FROM group_members WHERE group_id = ? AND user_id = ?`).get(groupId, myId);
      if (!memberRow || memberRow.role !== 'admin') { send(ws, { type: 'error', message: 'Not admin' }); return; }
      if (userId === myId) { send(ws, { type: 'error', message: 'Cannot remove yourself' }); return; }
      db.prepare(`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`).run(groupId, userId);
      deleteAllGroupEventsForUserInGroup(userId, groupId);
      const targetWs = peers.get(userId);
      if (targetWs) send(targetWs, { type: 'group-kicked', groupId });
      const remaining = getGroupMembers(groupId);
      for (const member of remaining) {
        const memberWs = peers.get(member.user_id);
        if (memberWs) send(memberWs, { type: 'group-member-removed', groupId, userId });
      }
      send(ws, { type: 'member-removed', groupId, userId });
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

// Самопинг
const APP_URL = 'https://onrender.com';
setInterval(() => {
  https.get(APP_URL, (res) => console.log(`[Self-Ping] Status: ${res.statusCode}`)).on('error', (err) => console.error(`[Self-Ping] Error: ${err.message}`));
}, 4 * 60 * 1000);

console.log(`[server] Groups ready on port ${PORT}`);
