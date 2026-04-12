const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();
const STORAGE_FILE = path.join(__dirname, 'offline_messages.json');

// Загружаем сохранённые сообщения при старте
let offlineMessages = {};
try {
  if (fs.existsSync(STORAGE_FILE)) {
    offlineMessages = JSON.parse(fs.readFileSync(STORAGE_FILE, 'utf8'));
    console.log('Loaded offline messages from disk');
  }
} catch (e) {
  console.error('Failed to load offline messages', e);
}

function saveMessages() {
  try {
    fs.writeFileSync(STORAGE_FILE, JSON.stringify(offlineMessages, null, 2));
  } catch (e) {
    console.error('Failed to save offline messages', e);
  }
}

wss.on('connection', (ws) => {
  let peerId = null;

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);
      
      if (data.type === 'register') {
        peerId = data.peerId.toLowerCase();
        peers.set(peerId, ws);
        ws.send(JSON.stringify({ type: 'registered' }));
        console.log(`[${peerId}] Registered`);

        // Отправляем накопленные офлайн-сообщения
        if (offlineMessages[peerId] && offlineMessages[peerId].length > 0) {
          ws.send(JSON.stringify({
            type: 'offline-msgs',
            messages: offlineMessages[peerId]
          }));
          console.log(`[${peerId}] Sent ${offlineMessages[peerId].length} offline messages`);
          // НЕ удаляем сразу – ждём подтверждения от клиента
        }
      }
      
      else if (data.type === 'signal' && data.target) {
        const targetWs = peers.get(data.target.toLowerCase());
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({
            type: 'signal',
            from: peerId,
            payload: data.payload
          }));
        }
      }
      
      // Сохранить офлайн-сообщение
      else if (data.type === 'store-offline') {
        const target = data.target.toLowerCase();
        if (!offlineMessages[target]) offlineMessages[target] = [];
        offlineMessages[target].push({
          from: peerId,
          payload: data.payload,
          timestamp: Date.now()
        });
        saveMessages();
        console.log(`[${peerId}] Stored offline message for ${target}`);
      }
      
      // Подтверждение получения – удаляем доставленные сообщения
      else if (data.type === 'ack-offline') {
        const count = data.count || 0;
        if (peerId && offlineMessages[peerId]) {
          offlineMessages[peerId] = offlineMessages[peerId].slice(count);
          if (offlineMessages[peerId].length === 0) {
            delete offlineMessages[peerId];
          }
          saveMessages();
          console.log(`[${peerId}] Acknowledged ${count} offline messages, remaining: ${offlineMessages[peerId]?.length || 0}`);
        }
      }
    } catch (e) {
      console.error('Invalid message', e);
    }
  });

  ws.on('close', () => {
    if (peerId) {
      peers.delete(peerId);
      console.log(`[${peerId}] Disconnected`);
    }
  });
});

console.log('Signal server with offline storage running on port', process.env.PORT || 3000);
