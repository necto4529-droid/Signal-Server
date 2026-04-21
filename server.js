const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

// --- БД SQLite с WAL-режимом ---
const dbPath = path.join(__dirname, 'offline_queue.db');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

// Таблицы для офлайн-событий (существующие)
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

// Новые таблицы для групп
db.exec(`
  CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    avatar TEXT DEFAULT '👥',
    creator_id TEXT NOT NULL,
    invite_token TEXT UNIQUE NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'member', -- 'admin' или 'member'
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members (user_id);
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

// Добавление события (синхронно), возвращает id новой записи
function enqueueEvent(recipientId, type, payload) {
  const stmt = db.prepare(
    `INSERT INTO events (recipient_id, type, payload, created_at) VALUES (?, ?, ?, ?)`
  );
  const info = stmt.run(recipientId, type, JSON.stringify(payload), Date.now());
  console.log(`[enqueue] ${recipientId} <- ${type} (id=${info.lastInsertRowid})`);
  return info.lastInsertRowid;
}

// Получение всех событий для получателя
function getEvents(recipientId) {
  return db
    .prepare(`SELECT * FROM events WHERE recipient_id = ? ORDER BY created_at`)
    .all(recipientId)
    .map(r => ({ id: r.id, type: r.type, payload: JSON.parse(r.payload) }));
}

// Удаление одного события по ID
function deleteEvent(eventId) {
  db.prepare(`DELETE FROM events WHERE id = ?`).run(eventId);
}

// Подтверждение получения входящего сообщения (возвращает отправителя)
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

// Генерация invite токена
function generateInviteToken() {
  return crypto.randomBytes(12).toString('hex'); // 24 символа
}

// --- Работа с группами ---

// Получить участников группы
function getGroupMembers(groupId) {
  return db.prepare(`SELECT user_id, role FROM group_members WHERE group_id = ?`).all(groupId);
}

// Проверить, является ли пользователь участником
function isGroupMember(groupId, userId) {
  return db.prepare(`SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?`).get(groupId, userId) !== undefined;
}

// Получить роль пользователя в группе
function getUserRole(groupId, userId) {
  const row = db.prepare(`SELECT role FROM group_members WHERE group_id = ? AND user_id = ?`).get(groupId, userId);
  return row ? row.role : null;
}

// Отправить событие всем участникам группы (кроме исключённого)
function broadcastToGroup(groupId, type, payload, excludeUserId = null) {
  const members = getGroupMembers(groupId);
  for (const m of members) {
    if (m.user_id === excludeUserId) continue;
    const targetWs = peers.get(m.user_id);
    const eventId = enqueueEvent(m.user_id, type, payload);
    if (targetWs) {
      send(targetWs, { type, ...payload, eventId });
    } else {
      sendPushNotification(m.user_id, 'Новое сообщение в группе').catch(() => {});
    }
  }
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

    // ── send-msg (личное или групповое) ──
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      // Проверяем, является ли target группой (начинается с "group:")
      if (target.startsWith('group:')) {
        const groupId = target.substring(6);
        // Проверяем, состоит ли отправитель в группе
        if (!isGroupMember(groupId, myId)) {
          console.log(`[send-msg] user ${myId} not in group ${groupId}`);
          return;
        }
        // Рассылаем всем участникам, кроме отправителя
        const members = getGroupMembers(groupId);
        for (const m of members) {
          if (m.user_id === myId) continue;
          const targetWs = peers.get(m.user_id);
          const eventId = ephemeral ? null : enqueueEvent(m.user_id, 'incoming-msg', {
            from: myId,
            msgId,
            payload,
            groupId: groupId
          });
          if (targetWs) {
            send(targetWs, {
              type: 'incoming-msg',
              from: myId,
              msgId,
              payload,
              groupId: groupId,
              eventId
            });
          } else if (!ephemeral) {
            sendPushNotification(m.user_id, 'Новое сообщение в группе').catch(() => {});
          }
        }
        console.log(`[${myId}] → group ${groupId} (${members.length - 1} recipients)`);
        return;
      }

      // Личное сообщение
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
        console.log(`[ack-msg] deleted incoming, sender=${senderId}`);
        const eventId = enqueueEvent(senderId, 'msg-delivered', { msgId, by: myId });
        const senderWs = peers.get(senderId);
        if (senderWs) {
          send(senderWs, { type: 'msg-delivered', msgId, by: myId, eventId });
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
      return;
    }

    // ── create_group ──
    if (data.type === 'create_group') {
      if (!myId) return;
      const { name, description, avatar } = data;
      if (!name) return;

      const groupId = 'grp_' + crypto.randomBytes(8).toString('hex');
      const inviteToken = generateInviteToken();
      const now = Date.now();

      const insertGroup = db.prepare(`
        INSERT INTO groups (id, name, description, avatar, creator_id, invite_token, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      insertGroup.run(groupId, name, description || '', avatar || '👥', myId, inviteToken, now);

      const insertMember = db.prepare(`
        INSERT INTO group_members (group_id, user_id, role, joined_at)
        VALUES (?, ?, 'admin', ?)
      `);
      insertMember.run(groupId, myId, now);

      // Отправляем создателю подтверждение
      send(ws, {
        type: 'group_created',
        groupId,
        name,
        description,
        avatar,
        inviteToken,
        isAdmin: true
      });

      console.log(`[${myId}] created group ${groupId} (${name})`);
      return;
    }

    // ── join_group ──
    if (data.type === 'join_group') {
      if (!myId) return;
      const { inviteToken } = data;
      if (!inviteToken) return;

      const group = db.prepare(`SELECT * FROM groups WHERE invite_token = ?`).get(inviteToken);
      if (!group) {
        send(ws, { type: 'join_group_error', error: 'invalid_token' });
        return;
      }

      const groupId = group.id;
      if (isGroupMember(groupId, myId)) {
        send(ws, { type: 'join_group_error', error: 'already_member' });
        return;
      }

      // Проверяем лимит участников (20)
      const memberCount = db.prepare(`SELECT COUNT(*) as cnt FROM group_members WHERE group_id = ?`).get(groupId).cnt;
      if (memberCount >= 20) {
        send(ws, { type: 'join_group_error', error: 'group_full' });
        return;
      }

      const now = Date.now();
      db.prepare(`INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)`)
        .run(groupId, myId, now);

      // Уведомляем всех участников о новом участнике
      const newMemberEvent = {
        type: 'group_member_joined',
        groupId,
        userId: myId,
        joinedAt: now
      };
      broadcastToGroup(groupId, 'group_member_joined', newMemberEvent, myId);

      // Отправляем информацию о группе новому участнику
      const members = getGroupMembers(groupId).map(m => ({ userId: m.user_id, role: m.role }));
      send(ws, {
        type: 'group_joined',
        groupId,
        name: group.name,
        description: group.description,
        avatar: group.avatar,
        inviteToken: group.invite_token,
        members,
        isAdmin: false
      });

      console.log(`[${myId}] joined group ${groupId}`);
      return;
    }

    // ── get_group_info ──
    if (data.type === 'get_group_info') {
      if (!myId) return;
      const { groupId } = data;
      if (!groupId) return;

      const group = db.prepare(`SELECT * FROM groups WHERE id = ?`).get(groupId);
      if (!group) {
        send(ws, { type: 'group_info_error', error: 'not_found' });
        return;
      }

      if (!isGroupMember(groupId, myId)) {
        send(ws, { type: 'group_info_error', error: 'not_member' });
        return;
      }

      const members = getGroupMembers(groupId).map(m => ({ userId: m.user_id, role: m.role }));
      const role = getUserRole(groupId, myId);
      send(ws, {
        type: 'group_info',
        groupId,
        name: group.name,
        description: group.description,
        avatar: group.avatar,
        inviteToken: group.invite_token,
        members,
        isAdmin: role === 'admin'
      });
      return;
    }

    // ── update_group ──
    if (data.type === 'update_group') {
      if (!myId) return;
      const { groupId, name, description, avatar } = data;
      if (!groupId) return;

      const role = getUserRole(groupId, myId);
      if (role !== 'admin') {
        send(ws, { type: 'update_group_error', error: 'not_admin' });
        return;
      }

      const updates = [];
      const params = [];
      if (name !== undefined) { updates.push('name = ?'); params.push(name); }
      if (description !== undefined) { updates.push('description = ?'); params.push(description); }
      if (avatar !== undefined) { updates.push('avatar = ?'); params.push(avatar); }
      if (updates.length === 0) return;

      params.push(groupId);
      db.prepare(`UPDATE groups SET ${updates.join(', ')} WHERE id = ?`).run(...params);

      // Оповещаем всех участников об обновлении
      broadcastToGroup(groupId, 'group_updated', {
        groupId,
        name,
        description,
        avatar
      });

      send(ws, { type: 'group_updated_ack', groupId });
      return;
    }

    // ── remove_member ──
    if (data.type === 'remove_member') {
      if (!myId) return;
      const { groupId, userId } = data;
      if (!groupId || !userId) return;

      const role = getUserRole(groupId, myId);
      if (role !== 'admin') {
        send(ws, { type: 'remove_member_error', error: 'not_admin' });
        return;
      }

      if (userId === myId) {
        send(ws, { type: 'remove_member_error', error: 'cannot_remove_self' });
        return;
      }

      if (!isGroupMember(groupId, userId)) {
        send(ws, { type: 'remove_member_error', error: 'not_member' });
        return;
      }

      db.prepare(`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`).run(groupId, userId);

      // Уведомляем удалённого пользователя и остальных
      const removedWs = peers.get(userId);
      if (removedWs) {
        send(removedWs, { type: 'kicked_from_group', groupId });
      }
      // Можно также добавить событие в очередь для офлайн
      enqueueEvent(userId, 'kicked_from_group', { groupId });

      broadcastToGroup(groupId, 'group_member_removed', {
        groupId,
        userId,
        by: myId
      });

      send(ws, { type: 'remove_member_ack', groupId, userId });
      return;
    }

    // ── leave_group ──
    if (data.type === 'leave_group') {
      if (!myId) return;
      const { groupId } = data;
      if (!groupId) return;

      if (!isGroupMember(groupId, myId)) {
        send(ws, { type: 'leave_group_error', error: 'not_member' });
        return;
      }

      const role = getUserRole(groupId, myId);
      // Если админ покидает группу и он единственный админ, можно передать права или удалить группу
      // Для простоты: если админ уходит, группа остаётся без админа (можно доработать)
      db.prepare(`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`).run(groupId, myId);

      broadcastToGroup(groupId, 'group_member_left', {
        groupId,
        userId: myId
      });

      send(ws, { type: 'left_group_ack', groupId });
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
        console.log(`[${myId}] disconnected (clean)`);
      } else {
        console.log(`[${myId}] disconnected but connection already replaced`);
      }
    }
  });
});

console.log(`[server] SQLite + groups ready on ${process.env.PORT || 3000}`);
