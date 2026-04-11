const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });

const peers = new Map();
let messageCounter = 0;

wss.on('connection', (ws) => {
  let peerId = null;
  const connId = ++messageCounter;
  console.log(`[${connId}] Новое соединение`);

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);
      console.log(`[${connId}] ← ${data.type}`, data.target ? `для ${data.target}` : '');

      if (data.type === 'register') {
        peerId = data.peerId;
        peers.set(peerId, ws);
        ws.send(JSON.stringify({ type: 'registered' }));
        console.log(`[${connId}] Зарегистрирован как ${peerId}`);
      } 
      else if (data.type === 'signal' && data.target) {
        const targetWs = peers.get(data.target);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          const payload = { type: 'signal', from: peerId, payload: data.payload };
          targetWs.send(JSON.stringify(payload));
          console.log(`[${connId}] → сигнал от ${peerId} к ${data.target}`);
        } else {
          console.log(`[${connId}] ❌ Цель ${data.target} не найдена или не открыта`);
        }
      }
    } catch (e) {
      console.error(`[${connId}] Ошибка парсинга:`, e.message);
    }
  });

  ws.on('close', () => {
    if (peerId) {
      peers.delete(peerId);
      console.log(`[${connId}] ${peerId} отключился`);
    }
  });

  ws.on('error', (err) => {
    console.error(`[${connId}] Ошибка WebSocket:`, err.message);
  });
});

console.log('🚀 Сигнальный сервер запущен на порту', process.env.PORT || 3000);
