const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });

// peerId -> WebSocket
const peers = new Map();

// Offline message queue: { [recipientId]: [{msgId, from, payload, ts, ephemeral}] }
const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');
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
  try { fs.writeFileSync(STORAGE_FILE, JSON.stringify(queue)); }
  catch (e) { console.error('[server] persist error', e); }
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify(obj));
}

// Broadcast presence change to ALL connected peers
function broadcastPresence(peerId, isOnline) {
  const msg = JSON.stringify({ type: 'presence', peerId, online: isOnline });
  for (const [, ws] of peers) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── REGISTER ────────────────────────────────────────────────
    if (data.type === 'register') {
      myId = (data.peerId || '').toLowerCase();
      if (!myId) return;
      peers.set(myId, ws);
      console.log(`[${myId}] registered`);

      send(ws, { type: 'registered' });

      // Broadcast that this peer is now online
      broadcastPresence(myId, true);

      // Deliver queued messages
      const pending = queue[myId] || [];
      if (pending.length > 0) {
        console.log(`[${myId}] delivering ${pending.length} queued msgs`);
        for (const m of pending) {
          send(ws, { type: 'incoming-msg', from: m.from, msgId: m.msgId, payload: m.payload });
        }
        // Auto-remove ephemeral from queue (they won't be acked)
        queue[myId] = pending.filter(m => !m.ephemeral);
        if (!queue[myId].length) delete queue[myId];
        persist();
      }
      return;
    }

    // ── SEND-MSG ────────────────────────────────────────────────
    if (data.type === 'send-msg') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const { msgId, payload, ephemeral } = data;
      if (!target || !msgId || !payload) return;

      const targetWs = peers.get(target);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        // Online: deliver immediately
        send(targetWs, { type: 'incoming-msg', from: myId, msgId, payload });
        console.log(`[${myId}] → [${target}] live delivery`);
        // For non-ephemeral: also queue temporarily until ACK confirms receipt
        if (!ephemeral) {
          if (!queue[target]) queue[target] = [];
          if (!queue[target].find(m => m.msgId === msgId)) {
            queue[target].push({ msgId, from: myId, payload, ts: Date.now(), ephemeral: false });
            persist();
          }
        }
      } else {
        // Offline: queue
        if (!ephemeral) {
          if (!queue[target]) queue[target] = [];
          if (!queue[target].find(m => m.msgId === msgId)) {
            queue[target].push({ msgId, from: myId, payload, ts: Date.now(), ephemeral: false });
            persist();
            console.log(`[${myId}] → [${target}] queued (offline)`);
          }
        }
      }
      return;
    }

    // ── ACK-MSG: recipient got the message, delete it ────────────
    if (data.type === 'ack-msg') {
      if (!myId) return;
      const { msgId } = data;
      if (!msgId) return;

      const pending = queue[myId] || [];
      const msgObj = pending.find(m => m.msgId === msgId);
      const senderId = msgObj?.from;

      // Remove from queue
      if (queue[myId]) {
        queue[myId] = queue[myId].filter(m => m.msgId !== msgId);
        if (!queue[myId].length) delete queue[myId];
        persist();
      }

      console.log(`[${myId}] ACK ${msgId}, sender=${senderId}`);

      // Notify sender of delivery
      if (senderId) {
        const senderWs = peers.get(senderId);
        if (senderWs && senderWs.readyState === WebSocket.OPEN) {
          send(senderWs, { type: 'msg-delivered', msgId, by: myId });
        }
      }
      return;
    }

    // ── QUERY-PRESENCE: is a specific peer online? ───────────────
    if (data.type === 'query-presence') {
      if (!myId) return;
      const target = (data.target || '').toLowerCase();
      const targetWs = peers.get(target);
      const isOnline = !!(targetWs && targetWs.readyState === WebSocket.OPEN);
      send(ws, { type: 'presence-reply', target, online: isOnline });
      return;
    }

    // ── Legacy signal relay ──────────────────────────────────────
    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs && targetWs.readyState === WebSocket.OPEN)
        send(targetWs, { type: 'signal', from: myId, payload: data.payload });
    }
  });

  ws.on('close', () => {
    if (myId) {
      peers.delete(myId);
      console.log(`[${myId}] disconnected`);
      // Broadcast offline
      broadcastPresence(myId, false);
    }
  });

  ws.on('error', e => console.error('ws error', e.message));
});

console.log(`[server] BloodyChat running on port ${process.env.PORT || 3000}`);
