const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');

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
    group_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    avatar TEXT DEFAULT '👥',
    invite_id TEXT UNIQUE NOT NULL,
    creator_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    encryption_enabled INTEGER DEFAULT 1
  );

  CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    role TEXT DEFAULT 'member',
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, peer_id),
    FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_group_members_peer ON group_members(peer_id);
`);

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
    console.log(`[${peerId}] heartbeat timeout, forcing offline`);
    if (peers.has(peerId)) peers.get(peerId).close();
  }, HEARTBEAT_TIMEOUT);
  heartbeats.set(peerId, timeout);
}

function enqueueEvent(recipientId, type, payload) {
  const stmt = db.prepare(`INSERT INTO events (recipient_id, type, payload, created_at) VALUES (?, ?, ?, ?)`);
  const info = stmt.run(recipientId, type, JSON.stringify(payload), Date.now());
  return info.lastInsertRowid;
}

function getEvents(recipientId) {
  return db.prepare(`SELECT * FROM events WHERE recipient_id = ? ORDER BY created_at`).all(recipientId)
    .map(r => ({ id: r.id, type: r.type, payload: JSON.parse(r.payload) }));
}

function deleteEvent(eventId) {
  db.prepare(`DELETE FROM events WHERE id = ?`).run(eventId);
}

function ackIncomingMsg(recipientId, msgId) {
  const row = db.prepare(
    `SELECT id, json_extract(payload, '$.from') as sender
     FROM events WHERE recipient_id = ? AND type = 'incoming-msg'
       AND json_extract(payload, '$.msgId') = ? LIMIT 1`
  ).get(recipientId, msgId);
  if (!row) return null;
  db.prepare(`DELETE FROM events WHERE id = ?`).run(row.id);
  return row.sender;
}

function generateInviteId() {
  const r = () => Math.random().toString(36).substring(2, 6);
  return `grp-${r()}-${r()}`;
}

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

      const group = db.prepare(`SELECT group_id FROM groups WHERE group_id = ?`).get(target);
      if (group) {
        const members = db.prepare(`SELECT peer_id FROM group_members WHERE group_id = ?`).all(target).map(r => r.peer_id);
        for (const memberId of members) {
          const memberWs = peers.get(memberId);
          if (memberWs) {
            send(memberWs, { type: 'incoming-msg', from: myId, msgId, payload });
          } else {
            sendPushNotification(memberId, 'Новое сообщение в группе').catch(() => {});
          }
          if (!ephemeral) enqueueEvent(memberId, 'incoming-msg', { from: myId, msgId, payload });
        }
      } else {
        const targetWs = peers.get(target);
        if (targetWs) {
          send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        } else {
          sendPushNotification(target, 'Новое сообщение').catch(() => {});
        }
        if (!ephemeral) enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
      }
      return;
    }

    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      const senderId = ackIncomingMsg(myId, msgId);
      if (senderId) {
        const groupCheck = db.prepare(`SELECT group_id FROM groups WHERE group_id = ?`).get(senderId);
        if (groupCheck) {
          const members = db.prepare(`SELECT peer_id FROM group_members WHERE group_id = ?`).all(senderId).map(r => r.peer_id);
          for (const memberId of members) {
            const eventId = enqueueEvent(memberId, 'group-msg-read', { groupId: senderId, msgId });
            const memberWs = peers.get(memberId);
            if (memberWs) send(memberWs, { type: 'group-msg-read', groupId: senderId, msgId, eventId });
          }
        } else {
          const eventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
          const senderWs = peers.get(senderId);
          if (senderWs) send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId });
        }
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

    // === Групповые операции ===
    if (data.type === 'create-group') {
      if (!myId) return;
      const { name, description, password, avatar } = data;
      if (!name) { send(ws, { type: 'group-created', success: false, error: 'Название обязательно' }); return; }
      const groupId = `grp_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
      const inviteId = generateInviteId();
      const now = Date.now();
      try {
        db.prepare(`INSERT INTO groups (group_id, name, description, avatar, invite_id, creator_id, created_at) VALUES (?,?,?,?,?,?,?)`)
          .run(groupId, name, description || '', avatar || '👥', inviteId, myId, now);
        db.prepare(`INSERT INTO group_members (group_id, peer_id, role, joined_at) VALUES (?,?,?,?)`)
          .run(groupId, myId, 'admin', now);
        send(ws, { type: 'group-created', success: true, groupId, groupName: name, avatar: avatar || '👥' });
      } catch (e) {
        send(ws, { type: 'group-created', success: false, error: 'Ошибка создания группы' });
      }
      return;
    }

    if (data.type === 'join-group') {
      if (!myId) return;
      const { inviteId, password } = data;
      const group = db.prepare(`SELECT * FROM groups WHERE invite_id = ?`).get(inviteId);
      if (!group) { send(ws, { type: 'group-joined', success: false, error: 'Группа не найдена' }); return; }
      const existing = db.prepare(`SELECT * FROM group_members WHERE group_id = ? AND peer_id = ?`).get(group.group_id, myId);
      if (existing) { send(ws, { type: 'group-joined', success: false, error: 'Вы уже в группе' }); return; }
      const count = db.prepare(`SELECT COUNT(*) as cnt FROM group_members WHERE group_id = ?`).get(group.group_id).cnt;
      if (count >= 20) { send(ws, { type: 'group-joined', success: false, error: 'Группа заполнена (макс. 20)' }); return; }
      db.prepare(`INSERT INTO group_members (group_id, peer_id, role, joined_at) VALUES (?,?,?,?)`)
        .run(group.group_id, myId, 'member', Date.now());
      send(ws, { type: 'group-joined', success: true, groupId: group.group_id, groupName: group.name, avatar: group.avatar });
      return;
    }

    if (data.type === 'get-group-info') {
      if (!myId) return;
      const { groupId } = data;
      const group = db.prepare(`SELECT * FROM groups WHERE group_id = ?`).get(groupId);
      if (!group) return;
      const members = db.prepare(`SELECT peer_id, role FROM group_members WHERE group_id = ?`).all(groupId);
      const myRole = members.find(m => m.peer_id === myId)?.role || null;
      if (!myRole) return;
      send(ws, {
        type: 'group-info',
        groupId,
        name: group.name,
        description: group.description,
        inviteId: group.invite_id,
        myRole,
        members: members.map(m => ({ peerId: m.peer_id, role: m.role }))
      });
      return;
    }

    if (data.type === 'remove-member') {
      if (!myId) return;
      const { groupId, peerId } = data;
      const roleRow = db.prepare(`SELECT role FROM group_members WHERE group_id = ? AND peer_id = ?`).get(groupId, myId);
      if (roleRow?.role !== 'admin') return;
      if (peerId === myId) return;
      db.prepare(`DELETE FROM group_members WHERE group_id = ? AND peer_id = ?`).run(groupId, peerId);
      const eventId = enqueueEvent(peerId, 'member-removed', { groupId, peerId });
      const peerWs = peers.get(peerId);
      if (peerWs) send(peerWs, { type: 'member-removed', groupId, peerId, eventId });
      const members = db.prepare(`SELECT peer_id FROM group_members WHERE group_id = ?`).all(groupId);
      for (const m of members) {
        if (m.peer_id === myId) continue;
        const evId = enqueueEvent(m.peer_id, 'member-removed', { groupId, peerId });
        const mWs = peers.get(m.peer_id);
        if (mWs) send(mWs, { type: 'member-removed', groupId, peerId, eventId: evId });
      }
      return;
    }

    if (data.type === 'leave-group') {
      if (!myId) return;
      const { groupId } = data;
      db.prepare(`DELETE FROM group_members WHERE group_id = ? AND peer_id = ?`).run(groupId, myId);
      const remaining = db.prepare(`SELECT COUNT(*) as cnt FROM group_members WHERE group_id = ?`).get(groupId).cnt;
      if (remaining === 0) db.prepare(`DELETE FROM groups WHERE group_id = ?`).run(groupId);
      return;
    }

    if (data.type === 'update-group') {
      if (!myId) return;
      const { groupId, name, description } = data;
      const role = db.prepare(`SELECT role FROM group_members WHERE group_id = ? AND peer_id = ?`).get(groupId, myId)?.role;
      if (role !== 'admin') return;
      if (name) db.prepare(`UPDATE groups SET name = ? WHERE group_id = ?`).run(name, groupId);
      if (description !== undefined) db.prepare(`UPDATE groups SET description = ? WHERE group_id = ?`).run(description, groupId);
      return;
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

const APP_URL = 'https://onrender.com';
setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`[Self-Ping] Status: ${res.statusCode} - Keep-alive active`);
  }).on('error', (err) => {
    console.error(`[Self-Ping] Error: ${err.message}`);
  });
}, 4 * 60 * 1000);

console.log(`[server] SQLite + WebSocket + Groups ready on port ${PORT}`);
