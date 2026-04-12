const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map(); // peerId -> ws

const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');

// ── Persistent storage ──────────────────────────────────────────────────────
// Structure: { [recipientId]: [ {msgId, from, payload, timestamp, ephemeral?}, ... ] }
let offlineMessages = {};
try {
  if (fs.existsSync(STORAGE_FILE)) {
    offlineMessages = JSON.parse(fs.readFileSync(STORAGE_FILE, 'utf8'));
    console.log('[server] Loaded offline messages from disk');
  }
} catch (e) {
  console.error('[server] Failed to load offline messages', e);
}

function persistMessages() {
  try {
    fs.writeFileSync(STORAGE_FILE, JSON.stringify(offlineMessages));
  } catch (e) {
    console.error('[server] Failed to save messages', e);
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function sendJSON(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function queueForPeer(recipientId, msgObj) {
  if (!offlineMessages[recipientId]) offlineMessages[recipientId] = [];
  // Deduplicate by msgId
  const exists = offlineMessages[recipientId].some(m => m.msgId === msgObj.msgId);
  if (!exists) {
    offlineMessages[recipientId].push(msgObj);
    persistMessages();
  }
}

function removeMessage(recipientId, msgId) {
  if (!offlineMessages[recipientId]) return;
  const before = offlineMessages[recipientId].length;
  offlineMessages[recipientId] = offlineMessages[recipientId].filter(m => m.msgId !== msgId);
  if (offlineMessages[recipientId].length === 0) delete offlineMessages[recipientId];
  if (offlineMessages[recipientId]?.length !== before || before > 0) {
    persistMessages();
  }
}

function pendingCount(peerId) {
  // Only count non-ephemeral messages
  return (offlineMessages[peerId] || []).filter(m => !m.ephemeral).length;
}

// ── Connection handler ────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let peerId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── REGISTER ──────────────────────────────────────────────────────────
    if (data.type === 'register') {
      peerId = data.peerId.toLowerCase();
      peers.set(peerId, ws);
      console.log(`[${peerId}] Registered`);

      sendJSON(ws, { type: 'registered' });

      // Deliver all queued messages (including ephemeral)
      const queue = offlineMessages[peerId] || [];
      if (queue.length > 0) {
        console.log(`[${peerId}] Delivering ${queue.length} queued messages`);
        for (const msg of queue) {
          sendJSON(ws, {
            type: 'incoming-msg',
            from: msg.from,
            msgId: msg.msgId,
            payload: msg.payload
          });
        }
        // Ephemeral messages (typing etc.) auto-remove on delivery
        const ephemeralIds = queue.filter(m => m.ephemeral).map(m => m.msgId);
        for (const id of ephemeralIds) removeMessage(peerId, id);
      }
      return;
    }

    // ── SEND-MSG: sender → server → recipient ──────────────────────────────
    if (data.type === 'send-msg') {
      if (!peerId) return;
      const target = (data.target || '').toLowerCase();
      const msgId = data.msgId;
      const payload = data.payload;
      const ephemeral = !!data.ephemeral;
      if (!target || !msgId || !payload) return;

      const msgObj = { msgId, from: peerId, payload, timestamp: Date.now(), ephemeral };

      const targetWs = peers.get(target);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        // Recipient is online — deliver immediately
        sendJSON(targetWs, { type: 'incoming-msg', from: peerId, msgId, payload });
        console.log(`[${peerId}] → [${target}] delivered immediately`);
        // For ephemeral, don't queue or wait for ack
        if (ephemeral) return;
        // Queue temporarily — recipient must ACK to confirm receipt
        // (handles race condition where WS drops between send and ack)
        queueForPeer(target, msgObj);
      } else {
        // Recipient offline — queue for later
        if (!ephemeral) {
          queueForPeer(target, msgObj);
          console.log(`[${peerId}] → [${target}] queued (offline)`);
        }
      }
      return;
    }

    // ── ACK-MSG: recipient confirms receipt → delete from queue → notify sender ──
    if (data.type === 'ack-msg') {
      if (!peerId) return;
      const msgId = data.msgId;
      if (!msgId) return;

      // Find which sender this message belongs to
      const queue = offlineMessages[peerId] || [];
      const msgObj = queue.find(m => m.msgId === msgId);
      const senderId = msgObj?.from;

      removeMessage(peerId, msgId);
      console.log(`[${peerId}] ACK msgId=${msgId}, sender=${senderId}`);

      // Notify the original sender that message was delivered
      if (senderId) {
        const senderWs = peers.get(senderId);
        if (senderWs && senderWs.readyState === WebSocket.OPEN) {
          sendJSON(senderWs, { type: 'msg-delivered', msgId, by: peerId });
          console.log(`[${senderId}] ← delivered notification for msgId=${msgId}`);
        }
      }
      return;
    }

    // ── CHECK-PENDING: sender asks how many messages are queued for a peer ──
    // Used to determine online/offline status of the peer
    if (data.type === 'check-pending') {
      if (!peerId) return;
      const target = (data.target || '').toLowerCase();
      const pending = pendingCount(target);
      sendJSON(ws, { type: 'pending-status', target, pending });
      console.log(`[${peerId}] check-pending [${target}] → ${pending}`);
      return;
    }

    // ── Legacy signal relay (kept for compatibility) ──────────────────────
    if (data.type === 'signal' && data.target) {
      const targetWs = peers.get(data.target.toLowerCase());
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        sendJSON(targetWs, { type: 'signal', from: peerId, payload: data.payload });
      }
    }
  });

  ws.on('close', () => {
    if (peerId) {
      peers.delete(peerId);
      console.log(`[${peerId}] Disconnected`);
    }
  });

  ws.on('error', (e) => {
    console.error('WS error', e.message);
  });
});

const port = process.env.PORT || 3000;
console.log(`[server] BloodyChat signal server running on port ${port}`);
