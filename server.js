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

// Таблица для чанков больших файлов
db.exec(`
  CREATE TABLE IF NOT EXISTS file_chunks (
    transfer_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL,
    sender_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    file_name TEXT,
    file_type TEXT,
    file_size INTEGER,
    chunk_data TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (transfer_id, chunk_index)
  );
  CREATE INDEX IF NOT EXISTS idx_chunks_transfer ON file_chunks (transfer_id);
`);

// Таблица групп
db.exec(`
  CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    creator_id TEXT NOT NULL,
    invite_code TEXT UNIQUE NOT NULL,
    avatar TEXT DEFAULT '👥',
    description TEXT DEFAULT '',
    members TEXT DEFAULT '[]',
    created_at INTEGER NOT NULL
  );
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

// Работа с группами
function getGroup(groupId) {
  return db.prepare('SELECT * FROM groups WHERE id = ?').get(groupId);
}

function getGroupByInvite(code) {
  return db.prepare('SELECT * FROM groups WHERE invite_code = ?').get(code);
}

function createGroup(group) {
  const stmt = db.prepare(
    'INSERT INTO groups (id, name, creator_id, invite_code, avatar, description, members, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  );
  stmt.run(
    group.id,
    group.name,
    group.creator_id,
    group.invite_code,
    group.avatar || '👥',
    group.description || '',
    JSON.stringify(group.members || []),
    Date.now()
  );
}

function updateGroup(groupId, changes) {
  const group = getGroup(groupId);
  if (!group) return;
  const newMembers = changes.members
    ? JSON.stringify(changes.members)
    : group.members;
  db.prepare(
    'UPDATE groups SET name = ?, avatar = ?, description = ?, members = ? WHERE id = ?'
  ).run(
    changes.name || group.name,
    changes.avatar || group.avatar,
    changes.description || group.description,
    newMembers,
    groupId
  );
}

function addMember(groupId, peerId) {
  const group = getGroup(groupId);
  if (!group) return false;
  const members = JSON.parse(group.members);
  if (members.includes(peerId)) return false;
  if (members.length >= 20) return false; // лимит
  members.push(peerId);
  db.prepare('UPDATE groups SET members = ? WHERE id = ?').run(
    JSON.stringify(members),
    groupId
  );
  return true;
}

function removeMember(groupId, peerId) {
  const group = getGroup(groupId);
  if (!group) return;
  const members = JSON.parse(group.members).filter(id => id !== peerId);
  db.prepare('UPDATE groups SET members = ? WHERE id = ?').run(
    JSON.stringify(members),
    groupId
  );
}

// --- Работа с чанками ---
function saveChunk(transferId, chunkIndex, totalChunks, senderId, recipientId, fileName, fileType, fileSize, chunkData) {
  const stmt = db.prepare(
    `INSERT OR REPLACE INTO file_chunks (transfer_id, chunk_index, total_chunks, sender_id, recipient_id, file_name, file_type, file_size, chunk_data, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  stmt.run(transferId, chunkIndex, totalChunks, senderId, recipientId, fileName, fileType, fileSize, chunkData, Date.now());
}

function checkTransferComplete(transferId, totalChunks) {
  const count = db.prepare(`SELECT COUNT(*) as cnt FROM file_chunks WHERE transfer_id = ?`).get(transferId).cnt;
  return count >= totalChunks;
}

function assembleAndClearChunks(transferId) {
  const rows = db.prepare(`SELECT * FROM file_chunks WHERE transfer_id = ? ORDER BY chunk_index`).all(transferId);
  if (!rows.length) return null;
  const assembled = {
    fileName: rows[0].file_name,
    fileType: rows[0].file_type,
    fileSize: rows[0].file_size,
    data: rows.map(r => {
      // Извлекаем только base64-контент из Data URL чанка
      const spl = r.chunk_data.split(',');
      return spl[1] || '';
    }).join('')
  };
  // Оборачиваем обратно в Data URL
  assembled.data = `data:${assembled.fileType};base64,${assembled.data}`;
  db.prepare(`DELETE FROM file_chunks WHERE transfer_id = ?`).run(transferId);
  return assembled;
}

function deliverAssembledFile(recipientId, filePayload, senderId, transferId) {
  // Отправляем как обычное входящее сообщение с типом 'file' и собранными данными
  const msgId = 'file_' + transferId;
  const payloadObj = {
    type: 'file',
    id: msgId,
    text: '',
    ts: Date.now(),
    replyTo: null,
    forwarded: false,
    file: {
      name: filePayload.fileName,
      type: filePayload.fileType,
      data: filePayload.data,
      size: filePayload.fileSize
    }
  };
  const payload = JSON.stringify(payloadObj);
  const targetWs = peers.get(recipientId);
  if (targetWs) {
    send(targetWs, { type: 'incoming-msg', from: senderId, msgId, payload: payload });
  } else {
    sendPushNotification(recipientId, '📁 Файл получен').catch(() => {});
  }
  enqueueEvent(recipientId, 'incoming-msg', { from: senderId, msgId, payload: payload });
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

    // Регистрация
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

      // Отправляем накопленные события
      const events = getEvents(myId);
      for (const ev of events) {
        send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
      }
      return;
    }

    // Пинг
    if (data.type === 'ping') {
      if (myId) resetHeartbeat(myId);
      return;
    }

    // Push-подписка
    if (data.type === 'register-push') {
      if (!myId) return;
      const { playerId } = data;
      if (playerId) {
        pushSubscriptions[myId] = playerId;
        fs.writeFileSync(pushSubscriptionsFile, JSON.stringify(pushSubscriptions));
      }
      return;
    }

    // --- ГРУППЫ ---
    if (data.type === 'create-group') {
      if (!myId) return;
      const { groupId, name, avatar, description, inviteCode, creator } = data;
      if (getGroupByInvite(inviteCode) || getGroup(groupId)) {
        send(ws, { type: 'error', msg: 'Группа с таким ID или кодом уже существует' });
        return;
      }
      createGroup({
        id: groupId,
        name,
        creator_id: creator,
        invite_code: inviteCode,
        avatar,
        description,
        members: [creator]
      });
      send(ws, { type: 'group-created', groupId });
      return;
    }

    if (data.type === 'group-info') {
      const group = getGroupByInvite(data.inviteCode);
      if (!group) {
        send(ws, { type: 'error', msg: 'Группа не найдена' });
        return;
      }
      send(ws, {
        type: 'group-info-reply',
        group: {
          groupId: group.id,
          name: group.name,
          avatar: group.avatar,
          description: group.description
        }
      });
      return;
    }

    if (data.type === 'join-group') {
      if (!myId) return;
      const group = getGroupByInvite(data.inviteCode);
      if (!group) {
        send(ws, { type: 'error', msg: 'Группа не найдена' });
        return;
      }
      if (JSON.parse(group.members).length >= 20) {
        send(ws, { type: 'error', msg: 'Группа переполнена (макс. 20)' });
        return;
      }
      if (!addMember(group.id, data.peerId)) {
        send(ws, { type: 'error', msg: 'Вы уже в группе' });
        return;
      }

      const updatedGroup = getGroup(group.id);
      // Подтверждение вступившему
      send(ws, {
        type: 'group-joined',
        groupId: group.id,
        name: group.name,
        avatar: group.avatar,
        description: group.description,
        inviteCode: group.invite_code,
        members: JSON.parse(updatedGroup.members)
      });

      // Уведомляем остальных участников
      const members = JSON.parse(updatedGroup.members);
      members.forEach(memberId => {
        if (memberId === data.peerId) return;
        const targetWs = peers.get(memberId);
        const ev = {
          type: 'group-updated',
          groupId: group.id,
          members: JSON.parse(updatedGroup.members)
        };
        if (targetWs) {
          send(targetWs, ev);
        } else {
          enqueueEvent(memberId, 'group-updated', ev);
        }
      });
      return;
    }

    if (data.type === 'group-update') {
      if (!myId) return;
      const group = getGroup(data.groupId);
      if (!group || group.creator_id !== myId) return;
      updateGroup(data.groupId, data.changes);

      const updated = getGroup(data.groupId);
      JSON.parse(updated.members).forEach(memberId => {
        const targetWs = peers.get(memberId);
        const ev = {
          type: 'group-updated',
          groupId: data.groupId,
          changes: data.changes
        };
        if (targetWs) {
          send(targetWs, ev);
        } else {
          enqueueEvent(memberId, 'group-updated', ev);
        }
      });
      return;
    }

    if (data.type === 'group-remove-member') {
      if (!myId) return;
      const group = getGroup(data.groupId);
      if (!group || group.creator_id !== myId) return;
      if (data.targetPeerId === myId) return; // нельзя удалить себя
      removeMember(data.groupId, data.targetPeerId);

      const updated = getGroup(data.groupId);
      // Удалённому
      const removedWs = peers.get(data.targetPeerId);
      if (removedWs) {
        send(removedWs, {
          type: 'group-member-removed',
          groupId: data.groupId,
          targetPeerId: data.targetPeerId
        });
      } else {
        enqueueEvent(data.targetPeerId, 'group-member-removed', {
          groupId: data.groupId,
          targetPeerId: data.targetPeerId
        });
      }

      // Остальным
      const members = JSON.parse(updated.members);
      members.forEach(memberId => {
        if (memberId === data.targetPeerId) return;
        const targetWs = peers.get(memberId);
        const ev = {
          type: 'group-updated',
          groupId: data.groupId,
          members: JSON.parse(updated.members)
        };
        if (targetWs) {
          send(targetWs, ev);
        } else {
          enqueueEvent(memberId, 'group-updated', ev);
        }
      });
      return;
    }

    if (data.type === 'group-leave') {
      if (!myId) return;
      const group = getGroup(data.groupId);
      if (!group) return;
      removeMember(data.groupId, data.peerId);

      const updated = getGroup(data.groupId);
      JSON.parse(updated.members).forEach(memberId => {
        const targetWs = peers.get(memberId);
        const ev = {
          type: 'group-updated',
          groupId: data.groupId,
          members: JSON.parse(updated.members)
        };
        if (targetWs) {
          send(targetWs, ev);
        } else {
          enqueueEvent(memberId, 'group-updated', ev);
        }
      });
      return;
    }

    if (data.type === 'group-members') {
      if (!myId) return;
      const group = getGroup(data.groupId);
      if (!group) return;
      send(ws, {
        type: 'group-members-reply',
        groupId: data.groupId,
        members: JSON.parse(group.members)
      });
      return;
    }

    if (data.type === 'group-read') {
      if (!myId) return;
      const group = getGroup(data.groupId);
      if (!group) return;
      JSON.parse(group.members).forEach(memberId => {
        if (memberId === data.readerPeerId) return;
        const targetWs = peers.get(memberId);
        const ev = {
          type: 'group-msg-read',
          groupId: data.groupId,
          msgId: data.msgId
        };
        if (targetWs) {
          send(targetWs, ev);
        } else {
          enqueueEvent(memberId, 'group-msg-read', ev);
        }
      });
      return;
    }

    // --- Передача файлов чанками ---
    if (data.type === 'transfer-start') {
      if (!myId) return;
      const { transferId, totalChunks, fileName, fileType, fileSize, target } = data;
      if (totalChunks > 5000) { send(ws, { type: 'error', msg: 'Слишком много чанков' }); return; }
      send(ws, { type: 'transfer-accepted', transferId });
      return;
    }

    if (data.type === 'chunk-data') {
      if (!myId) return;
      const { transferId, chunkIndex, totalChunks, fileName, fileType, fileSize, target, chunkData } = data;
      if (!transferId || chunkIndex === undefined) return;
      saveChunk(transferId, chunkIndex, totalChunks, myId, target, fileName, fileType, fileSize, chunkData);
      const complete = checkTransferComplete(transferId, totalChunks);
      if (complete) {
        const assembled = assembleAndClearChunks(transferId);
        if (assembled) {
          deliverAssembledFile(target, assembled, myId, transferId);
        }
      }
      return;
    }

    if (data.type === 'transfer-cancel') {
      if (!myId) return;
      const { transferId } = data;
      db.prepare(`DELETE FROM file_chunks WHERE transfer_id = ? AND sender_id = ?`).run(transferId, myId);
      return;
    }

    // --- Обычные сообщения ---
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      if (target.startsWith('grp_')) {
        const group = getGroup(target);
        if (!group) return;
        const members = JSON.parse(group.members);
        members.forEach(memberId => {
          if (memberId === myId) return;
          const memberWs = peers.get(memberId);
          if (memberWs) send(memberWs, { type: 'incoming-msg', from: myId, msgId, payload });
          else sendPushNotification(memberId, 'Новое сообщение в группе').catch(() => {});
          if (!ephemeral) enqueueEvent(memberId, 'incoming-msg', { from: myId, msgId, payload });
        });
        return;
      }

      const targetWs = peers.get(target);
      if (targetWs) send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
      else sendPushNotification(target, 'Новое сообщение').catch(() => {});
      if (!ephemeral) enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
      return;
    }

    // Подтверждение получения сообщения
    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      const senderId = ackIncomingMsg(myId, msgId);
      if (senderId) {
        const eventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
        const senderWs = peers.get(senderId);
        if (senderWs) {
          send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId });
        }
      }
      return;
    }

    // Подтверждение обработки события
    if (data.type === 'ack-event') {
      if (myId && data.eventId) deleteEvent(data.eventId);
      return;
    }

    // Запрос присутствия
    if (data.type === 'query-presence') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      send(ws, { type: 'presence-reply', target, online: peers.has(target) });
      return;
    }

    // Голосовое прослушано
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

    // Произвольный сигнал (если нужен)
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

// --- САМОПИНГ каждые 4 минуты ---
const APP_URL = 'https://onrender.com';
setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`[Self-Ping] Status: ${res.statusCode} - Keep-alive active`);
  }).on('error', (err) => {
    console.error(`[Self-Ping] Error: ${err.message}`);
  });
}, 4 * 60 * 1000); // 4 минуты

console.log(`[server] SQLite + WebSocket + Chunked file transfer ready on port ${PORT}`);
