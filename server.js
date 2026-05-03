const WebSocket = require('ws');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');

// ─── SQLite WAL ───────────────────────────────────────────────────────────────
const dbPath = path.join(__dirname, 'offline_queue.db');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');   // быстрее чем FULL, безопасно при WAL
db.pragma('cache_size = -8000');     // 8 МБ кэш

db.exec(`
  CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id TEXT NOT NULL,
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_recipient ON events (recipient_id);

  CREATE TABLE IF NOT EXISTS groups (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    creator_id  TEXT NOT NULL,
    invite_code TEXT UNIQUE NOT NULL,
    avatar      TEXT DEFAULT '👥',
    description TEXT DEFAULT '',
    members     TEXT DEFAULT '[]',
    created_at  INTEGER NOT NULL
  );
`);

// ─── Подготовленные запросы (быстрее повторных парсингов) ─────────────────────
const stmtInsertEvent  = db.prepare(`INSERT INTO events (recipient_id,type,payload,created_at) VALUES (?,?,?,?)`);
const stmtGetEvents    = db.prepare(`SELECT * FROM events WHERE recipient_id=? ORDER BY created_at`);
const stmtDeleteEvent  = db.prepare(`DELETE FROM events WHERE id=?`);
const stmtAckMsg       = db.prepare(`
  SELECT id, json_extract(payload,'$.from') AS sender
  FROM events
  WHERE recipient_id=? AND type='incoming-msg' AND json_extract(payload,'$.msgId')=?
  LIMIT 1
`);
const stmtDeleteById   = db.prepare(`DELETE FROM events WHERE id=?`);

// Batch-удаление событий (для быстрой очистки файловых чанков)
const stmtDeleteByRecipientType = db.prepare(`DELETE FROM events WHERE recipient_id=? AND type=?`);

// ─── Push ─────────────────────────────────────────────────────────────────────
const pushFile = path.join(__dirname, 'push_subscriptions.json');
let pushSubs = {};
try { if(fs.existsSync(pushFile)) pushSubs = JSON.parse(fs.readFileSync(pushFile, 'utf8')); } catch(e) {}

const ONESIGNAL_APP_ID = 'c5b0ecd0-3e67-47a0-823d-771a7c4de3be';
const ONESIGNAL_KEY    = 'os_v2_app_ywyozub6m5d2bar5o4nhytpdxzr72sz2khuemruxqbapncfalaxcwfqlqoxvcenyxr6sa5uvelsbqwpwrwihgdpwn4ectomaup5byuq';

async function sendPush(userId, message) {
  const playerId = pushSubs[userId];
  if(!playerId) return;
  try {
    await fetch('https://api.onesignal.com/notifications', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Basic ${ONESIGNAL_KEY}` },
      body: JSON.stringify({
        app_id: ONESIGNAL_APP_ID,
        include_player_ids: [playerId],
        contents: { en: message },
        headings: { en: 'K-Chat' }
      })
    });
  } catch(e) {}
}

// ─── WebSocket сервер ─────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({
  port: PORT,
  maxPayload: 256 * 1024 * 1024   // 256 МБ — на случай очень больших чанков
});

const peers = new Map();           // peerId → WebSocket
const heartbeats = new Map();
const HEARTBEAT_TIMEOUT = 60_000;

// ─── Хелперы ─────────────────────────────────────────────────────────────────
function send(ws, obj) {
  if(ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for(const [, ws] of peers) if(ws.readyState === WebSocket.OPEN) ws.send(msg);
}

function resetHeartbeat(peerId) {
  if(heartbeats.has(peerId)) clearTimeout(heartbeats.get(peerId));
  heartbeats.set(peerId, setTimeout(() => {
    console.log(`[${peerId}] heartbeat timeout`);
    peers.get(peerId)?.close();
  }, HEARTBEAT_TIMEOUT));
}

function enqueueEvent(recipientId, type, payload) {
  const info = stmtInsertEvent.run(recipientId, type, JSON.stringify(payload), Date.now());
  return info.lastInsertRowid;
}

function getEvents(recipientId) {
  return stmtGetEvents.all(recipientId).map(r => ({
    id: r.id, type: r.type, payload: JSON.parse(r.payload)
  }));
}

// ─── Группы ──────────────────────────────────────────────────────────────────
const stmtGetGroup        = db.prepare('SELECT * FROM groups WHERE id=?');
const stmtGetGroupInvite  = db.prepare('SELECT * FROM groups WHERE invite_code=?');
const stmtCreateGroup     = db.prepare('INSERT INTO groups (id,name,creator_id,invite_code,avatar,description,members,created_at) VALUES (?,?,?,?,?,?,?,?)');
const stmtUpdateGroupMbrs = db.prepare('UPDATE groups SET members=? WHERE id=?');
const stmtUpdateGroup     = db.prepare('UPDATE groups SET name=?,avatar=?,description=?,members=? WHERE id=?');

const getGroup       = id   => stmtGetGroup.get(id);
const getGroupInvite = code => stmtGetGroupInvite.get(code);

function addMember(groupId, peerId) {
  const g = getGroup(groupId); if(!g) return false;
  const members = JSON.parse(g.members);
  if(members.includes(peerId) || members.length >= 20) return false;
  members.push(peerId);
  stmtUpdateGroupMbrs.run(JSON.stringify(members), groupId);
  return true;
}

function removeMember(groupId, peerId) {
  const g = getGroup(groupId); if(!g) return;
  const members = JSON.parse(g.members).filter(id => id !== peerId);
  stmtUpdateGroupMbrs.run(JSON.stringify(members), groupId);
}

// ─── Соединения ──────────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── Регистрация ──────────────────────────────────────────────────────────
    if(data.type === 'register') {
      const newId = (data.peerId || '').toLowerCase().trim();
      if(!newId) return;

      // Закрываем старое соединение если есть
      const old = peers.get(newId);
      if(old && old !== ws && old.readyState === WebSocket.OPEN) old.close();

      myId = newId;
      peers.set(myId, ws);
      send(ws, { type: 'registered' });
      broadcastPresence(myId, true);
      resetHeartbeat(myId);

      // Доставляем накопленную очередь
      const events = getEvents(myId);
      for(const ev of events) send(ws, { type: ev.type, ...ev.payload, eventId: ev.id });
      return;
    }

    // ── Ping ─────────────────────────────────────────────────────────────────
    if(data.type === 'ping') {
      if(myId) resetHeartbeat(myId);
      return;
    }

    // ── Push-подписка ────────────────────────────────────────────────────────
    if(data.type === 'register-push') {
      if(!myId || !data.playerId) return;
      pushSubs[myId] = data.playerId;
      fs.writeFileSync(pushFile, JSON.stringify(pushSubs));
      return;
    }

    // ── Создание группы ──────────────────────────────────────────────────────
    if(data.type === 'create-group') {
      if(!myId) return;
      if(getGroupInvite(data.inviteCode) || getGroup(data.groupId)) {
        send(ws, { type: 'error', msg: 'Группа уже существует' }); return;
      }
      stmtCreateGroup.run(data.groupId, data.name, data.creator, data.inviteCode,
        data.avatar||'👥', data.description||'', JSON.stringify([data.creator]), Date.now());
      send(ws, { type: 'group-created', groupId: data.groupId });
      return;
    }

    if(data.type === 'group-info') {
      const g = getGroupInvite(data.inviteCode);
      if(!g) { send(ws, { type: 'error', msg: 'Группа не найдена' }); return; }
      send(ws, { type: 'group-info-reply', group: { groupId: g.id, name: g.name, avatar: g.avatar, description: g.description } });
      return;
    }

    if(data.type === 'join-group') {
      if(!myId) return;
      const g = getGroupInvite(data.inviteCode);
      if(!g) { send(ws, { type: 'error', msg: 'Группа не найдена' }); return; }
      if(JSON.parse(g.members).length >= 20) { send(ws, { type: 'error', msg: 'Группа переполнена' }); return; }
      if(!addMember(g.id, data.peerId)) { send(ws, { type: 'error', msg: 'Вы уже в группе' }); return; }
      const updated = getGroup(g.id);
      const members = JSON.parse(updated.members);
      send(ws, { type: 'group-joined', groupId: g.id, name: g.name, avatar: g.avatar,
        description: g.description, inviteCode: g.invite_code, members });
      members.forEach(mid => {
        if(mid === data.peerId) return;
        const ev = { type: 'group-updated', groupId: g.id, members };
        const tw = peers.get(mid);
        if(tw) send(tw, ev); else enqueueEvent(mid, 'group-updated', ev);
      });
      return;
    }

    if(data.type === 'group-update') {
      if(!myId) return;
      const g = getGroup(data.groupId);
      if(!g || g.creator_id !== myId) return;
      stmtUpdateGroup.run(data.changes.name||g.name, data.changes.avatar||g.avatar,
        data.changes.description||g.description,
        data.changes.members ? JSON.stringify(data.changes.members) : g.members, data.groupId);
      const updated = getGroup(data.groupId);
      JSON.parse(updated.members).forEach(mid => {
        const ev = { type: 'group-updated', groupId: data.groupId, changes: data.changes };
        const tw = peers.get(mid);
        if(tw) send(tw, ev); else enqueueEvent(mid, 'group-updated', ev);
      });
      return;
    }

    if(data.type === 'group-remove-member') {
      if(!myId) return;
      const g = getGroup(data.groupId);
      if(!g || g.creator_id !== myId || data.targetPeerId === myId) return;
      removeMember(data.groupId, data.targetPeerId);
      const updated = getGroup(data.groupId);
      const tw = peers.get(data.targetPeerId);
      const removedEv = { type: 'group-member-removed', groupId: data.groupId, targetPeerId: data.targetPeerId };
      if(tw) send(tw, removedEv); else enqueueEvent(data.targetPeerId, 'group-member-removed', removedEv);
      JSON.parse(updated.members).forEach(mid => {
        const ev = { type: 'group-updated', groupId: data.groupId, members: JSON.parse(updated.members) };
        const mw = peers.get(mid);
        if(mw) send(mw, ev); else enqueueEvent(mid, 'group-updated', ev);
      });
      return;
    }

    if(data.type === 'group-leave') {
      if(!myId) return;
      const g = getGroup(data.groupId); if(!g) return;
      removeMember(data.groupId, data.peerId);
      const updated = getGroup(data.groupId);
      JSON.parse(updated.members).forEach(mid => {
        const ev = { type: 'group-updated', groupId: data.groupId, members: JSON.parse(updated.members) };
        const mw = peers.get(mid);
        if(mw) send(mw, ev); else enqueueEvent(mid, 'group-updated', ev);
      });
      return;
    }

    if(data.type === 'group-members') {
      if(!myId) return;
      const g = getGroup(data.groupId); if(!g) return;
      send(ws, { type: 'group-members-reply', groupId: data.groupId, members: JSON.parse(g.members) });
      return;
    }

    if(data.type === 'group-read') {
      if(!myId) return;
      const g = getGroup(data.groupId); if(!g) return;
      JSON.parse(g.members).forEach(mid => {
        if(mid === data.readerPeerId) return;
        const ev = { type: 'group-msg-read', groupId: data.groupId, msgId: data.msgId };
        const mw = peers.get(mid);
        if(mw) send(mw, ev); else enqueueEvent(mid, 'group-msg-read', ev);
      });
      return;
    }

    // ── Отправка сообщений ───────────────────────────────────────────────────
    if(data.type === 'send-msg') {
      if(!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if(!target || !msgId || !payload) return;

      // Групповая отправка
      if(target.startsWith('grp_')) {
        const g = getGroup(target); if(!g) return;
        JSON.parse(g.members).forEach(mid => {
          if(mid === myId) return;
          const mw = peers.get(mid);
          if(mw) {
            send(mw, { type: 'incoming-msg', from: myId, msgId, payload });
          } else {
            sendPush(mid, 'Новое сообщение в группе').catch(() => {});
          }
          if(!ephemeral) enqueueEvent(mid, 'incoming-msg', { from: myId, msgId, payload });
        });
        return;
      }

      // Личная отправка
      const targetWs = peers.get(target);
      if(targetWs) {
        // Получатель онлайн — доставляем напрямую, мгновенно
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        // ВСЕ не-ephemeral сообщения попадают в офлайн-очередь
        // Это включает в себя как обычные сообщения, так и файловые чанки (file-header / file-chunk)
        if(!ephemeral) {
          enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
        }
      } else {
        // Получатель офлайн — сохраняем в очередь
        if(!ephemeral) {
          enqueueEvent(target, 'incoming-msg', { from: myId, msgId, payload });
        }
        sendPush(target, 'Новое сообщение').catch(() => {});
      }
      return;
    }

    // ── ACK сообщения ────────────────────────────────────────────────────────
    if(data.type === 'ack-msg') {
      if(!myId) return;
      const row = stmtAckMsg.get(myId, data.msgId);
      if(row) {
        stmtDeleteById.run(row.id);
        const eventId = enqueueEvent(row.sender, 'msg-delivered', { msgId: data.msgId, by: myId });
        const senderWs = peers.get(row.sender);
        if(senderWs) send(senderWs, { type: 'msg-delivered', msgId: data.msgId, by: myId, eventId });
      }
      return;
    }

    // ── ACK события ─────────────────────────────────────────────────────────
    if(data.type === 'ack-event') {
      if(myId && data.eventId) stmtDeleteEvent.run(data.eventId);
      return;
    }

    // ── Запрос присутствия ───────────────────────────────────────────────────
    if(data.type === 'query-presence') {
      if(!myId) return;
      const target = (data.target || '').toLowerCase();
      send(ws, { type: 'presence-reply', target, online: peers.has(target) });
      return;
    }

    // ── Голосовое прослушано ─────────────────────────────────────────────────
    if(data.type === 'voice-listened') {
      if(!myId) return;
      const target = (data.target || '').toLowerCase();
      const eventId = enqueueEvent(target, 'voice-listened', { from: myId, voiceMsgId: data.voiceMsgId });
      const tw = peers.get(target);
      if(tw) send(tw, { type: 'voice-listened', from: myId, voiceMsgId: data.voiceMsgId, eventId });
      return;
    }

    // ── Сигнал ───────────────────────────────────────────────────────────────
    if(data.type === 'signal' && data.target) {
      const tw = peers.get(data.target.toLowerCase());
      if(tw) send(tw, { type: 'signal', from: myId, payload: data.payload });
    }
  });

  ws.on('close', () => {
    if(myId) {
      clearTimeout(heartbeats.get(myId));
      heartbeats.delete(myId);
      if(peers.get(myId) === ws) {
        peers.delete(myId);
        broadcastPresence(myId, false);
      }
    }
  });

  ws.on('error', (err) => {
    console.error(`WS error [${myId}]:`, err.message);
  });
});

// ─── Самопинг ────────────────────────────────────────────────────────────────
const APP_URL = 'https://signal-server-aipd.onrender.com';
setInterval(() => {
  https.get(APP_URL, res => {
    console.log(`[Self-Ping] ${res.statusCode}`);
  }).on('error', err => {
    console.error(`[Self-Ping] Error: ${err.message}`);
  });
}, 4 * 60 * 1000);

console.log(`[K-Chat server] ready on port ${PORT}`);
