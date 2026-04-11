const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 3000 });
const peers = new Map();

wss.on('connection', (ws) => {
  let peerId = null;
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      if (data.type === 'register') {
        peerId = data.peerId;
        peers.set(peerId, ws);
        ws.send(JSON.stringify({ type: 'registered' }));
      } else if (data.type === 'signal' && data.target) {
        const targetWs = peers.get(data.target);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({ type: 'signal', from: peerId, payload: data.payload }));
        }
      }
    } catch (e) {}
  });
  ws.on('close', () => { if (peerId) peers.delete(peerId); });
});
