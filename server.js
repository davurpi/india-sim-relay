const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const rooms = new Map();
let nextPeerId = 1;

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'GET' && req.url === '/rooms') {
    const list = [];
    for (const [name, room] of rooms) {
      list.push({
        name,
        host: room.hostName,
        players: room.peers.size
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(list));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('India Street Simulator Relay v1.0');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  let url;
  try {
    url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch {
    ws.close();
    return;
  }

  const action = url.searchParams.get('action');
  const roomName = url.searchParams.get('room');
  const playerName = decodeURIComponent(url.searchParams.get('name') || 'Jugador');

  ws._peerId = 0;
  ws._room = null;
  ws._name = playerName;

  if (!roomName || !action) {
    ws.send(JSON.stringify({ type: 'error', msg: 'Faltan parametros' }));
    ws.close();
    return;
  }

  if (action === 'create') {
    if (rooms.has(roomName)) {
      ws.send(JSON.stringify({ type: 'error', msg: 'La sala ya existe' }));
      ws.close();
      return;
    }
    ws._peerId = 1;
    ws._room = roomName;
    rooms.set(roomName, {
      hostName: playerName,
      peers: new Map([[1, ws]])
    });
    ws.send(JSON.stringify({ type: 'created', peerId: 1, room: roomName }));
    console.log(`[+] Room "${roomName}" created by ${playerName}`);

  } else if (action === 'join') {
    const room = rooms.get(roomName);
    if (!room) {
      ws.send(JSON.stringify({ type: 'error', msg: 'Sala no encontrada' }));
      ws.close();
      return;
    }
    nextPeerId++;
    ws._peerId = nextPeerId;
    ws._room = roomName;
    room.peers.set(ws._peerId, ws);

    ws.send(JSON.stringify({ type: 'joined', peerId: ws._peerId, room: roomName }));

    for (const [id, peer] of room.peers) {
      if (id !== ws._peerId && peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify({ type: 'peer_joined', peerId: ws._peerId, name: playerName }));
      }
    }
    for (const [id, peer] of room.peers) {
      if (id !== ws._peerId) {
        ws.send(JSON.stringify({ type: 'peer_joined', peerId: id, name: peer._name }));
      }
    }
    console.log(`[+] ${playerName} joined "${roomName}" as peer ${ws._peerId}`);

  } else {
    ws.send(JSON.stringify({ type: 'error', msg: 'Accion invalida' }));
    ws.close();
    return;
  }

  ws.on('message', (data) => {
    const room = rooms.get(ws._room);
    if (!room) return;
    try {
      const msg = JSON.parse(data.toString());
      msg.from = ws._peerId;
      const out = JSON.stringify(msg);
      for (const [id, peer] of room.peers) {
        if (id !== ws._peerId && peer.readyState === WebSocket.OPEN) {
          peer.send(out);
        }
      }
    } catch {
      // ignore malformed
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws._room);
    if (!room) return;
    room.peers.delete(ws._peerId);

    for (const [id, peer] of room.peers) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify({ type: 'peer_left', peerId: ws._peerId }));
      }
    }

    if (ws._peerId === 1 || room.peers.size === 0) {
      for (const [id, peer] of room.peers) {
        if (peer.readyState === WebSocket.OPEN) {
          peer.send(JSON.stringify({ type: 'room_closed' }));
          peer.close();
        }
      }
      rooms.delete(ws._room);
      console.log(`[-] Room "${ws._room}" closed`);
    } else {
      console.log(`[-] Peer ${ws._peerId} left "${ws._room}"`);
    }
  });

  ws.on('error', (err) => {
    console.error(`[!] Peer ${ws._peerId} error:`, err.message);
  });
});

setInterval(() => {
  for (const [name, room] of rooms) {
    let alive = false;
    for (const [, peer] of room.peers) {
      if (peer.readyState === WebSocket.OPEN) { alive = true; break; }
    }
    if (!alive) {
      rooms.delete(name);
      console.log(`[GC] Stale room "${name}" removed`);
    }
  }
}, 300000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`India Sim Relay running on port ${PORT}`);
});
