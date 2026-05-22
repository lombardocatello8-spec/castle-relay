/* ============================================================
   Castle of Fate — WebSocket relay server
   ------------------------------------------------------------
   LOCAL (quick session):
     1.  npm install ws        (first time only)
     2.  node server.js
     Optionally expose with cloudflared (free, no limits):
       cloudflared tunnel --url ws://localhost:8080
     Copy the *.trycloudflare.com URL and paste it into the game.

   CLOUD / ALWAYS-ON (recommended — no tunnel, no local machine):
     Deploy to Railway, Render, Fly.io, or any Node host.
     Set PORT env var if needed (Railway does this automatically).
     Your deployed URL (e.g. wss://castle-relay.up.railway.app)
     goes directly into the game's MULTIPLAYER lobby.

   The relay tracks rooms keyed by a short code. The first client in a
   room is the host (slot 0, game-authoritative); joiners get the next
   free slot (1..3). The server stamps `from` on every relayed message
   and routes directed (`to`) vs broadcast traffic.
   ============================================================ */
const http   = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;

const rooms = new Map(); // code -> { sockets: Map<slot, ws>, subMode }

// Plain HTTP server — handles health checks (Railway/Render ping GET /health)
// and serves as the upgrade target for WebSocket connections.
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Castle of Fate relay — connect via WebSocket.\n');
});

const wss = new WebSocketServer({ server });

function genCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no ambiguous 0/O/1/I
  let s = '';
  for (let i = 0; i < 5; i++) s += A[Math.floor(Math.random() * A.length)];
  return s;
}
function send(ws, obj) {
  if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (_) {} }
}
function nextSlot(room) { let s = 1; while (room.sockets.has(s)) s++; return s; }
function roster(room, exceptSlot) {
  return [...room.sockets.entries()]
    .filter(([slot]) => slot !== exceptSlot)
    .sort((a, b) => a[0] - b[0])
    .map(([slot, s]) => ({ id: slot, name: s.playerName || ('P' + slot) }));
}

wss.on('connection', ws => {
  ws.roomCode = null;
  ws.slot = null;

  ws.on('message', data => {
    let m;
    try { m = JSON.parse(data); } catch (_) { return; }
    if (!m || typeof m !== 'object') return;

    // ----- HOST: create a fresh room -----
    if (m.type === 'host') {
      let code; do { code = genCode(); } while (rooms.has(code));
      const subMode = m.subMode === 'story' ? 'story' : 'endless';
      const room = { sockets: new Map(), subMode };
      rooms.set(code, room);
      ws.roomCode = code; ws.slot = 0;
      ws.playerName = String(m.name || 'Host').slice(0, 16);
      room.sockets.set(0, ws);
      send(ws, { type: 'hosted', code, id: 0, hostId: 0, subMode });
      console.log(`[+] room ${code} hosted by ${ws.playerName} (${subMode})`);
      return;
    }

    // ----- JOINER: enter an existing room -----
    if (m.type === 'join') {
      const code = String(m.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) { send(ws, { type: 'error', text: 'No lobby with that code.' }); return; }
      if (room.sockets.size >= MAX_PLAYERS) { send(ws, { type: 'error', text: 'Room is full.' }); return; }
      const slot = nextSlot(room);
      ws.roomCode = code; ws.slot = slot;
      ws.playerName = String(m.name || ('P' + slot)).slice(0, 16);
      room.sockets.set(slot, ws);
      // newcomer gets the current roster (host + everyone already in, minus self)
      send(ws, {
        type: 'joined', id: slot, hostId: 0, subMode: room.subMode,
        selfName: ws.playerName, peers: roster(room, slot),
      });
      // everyone already in the room hears about the newcomer
      for (const [sid, s] of room.sockets) {
        if (sid !== slot) send(s, { type: 'peer_joined', id: slot, name: ws.playerName });
      }
      console.log(`[+] ${ws.playerName} joined ${code} as slot ${slot} (${room.sockets.size}/${MAX_PLAYERS})`);
      return;
    }

    // ----- RELAY: every other message -----
    const room = rooms.get(ws.roomCode);
    if (!room || ws.slot == null) return;
    m.from = ws.slot; // stamp sender slot
    if (m.to != null) { // directed message
      send(room.sockets.get(m.to), m);
    } else { // broadcast to everyone else
      for (const [sid, s] of room.sockets) if (sid !== ws.slot) send(s, m);
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomCode);
    if (!room || ws.slot == null) return;
    room.sockets.delete(ws.slot);
    for (const [, s] of room.sockets) send(s, { type: 'peer_left', id: ws.slot });

    // host left, or the room is now empty: tear the whole room down
    if (ws.slot === 0 || room.sockets.size === 0) {
      for (const [, s] of room.sockets) { try { s.close(); } catch (_) {} }
      rooms.delete(ws.roomCode);
      console.log(`[-] room ${ws.roomCode} closed`);
    } else {
      console.log(`[-] slot ${ws.slot} left ${ws.roomCode} (${room.sockets.size} remain)`);
    }
  });

  ws.on('error', () => {});
});

server.listen(PORT, () => {
  console.log(`Castle of Fate relay listening on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
  console.log(`Local tunnel:  cloudflared tunnel --url ws://localhost:${PORT}`);
});
