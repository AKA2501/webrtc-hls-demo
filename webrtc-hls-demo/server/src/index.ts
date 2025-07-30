import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import { RoomManager } from './mediasoup/room';
import path from 'path';
import { existsSync, mkdirSync } from 'fs';

const app = express();
const server = createServer(app);

// Configure Socket.IO
const io = new Server(server, {
  cors: {
    origin: ['http://localhost:3000', 'http://localhost:3001'],
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['websocket', 'polling']
});

// Express middleware
app.use(cors({
  origin: ['http://localhost:3000', 'http://localhost:3001'],
  credentials: true
}));
app.use(express.json());

// Ensure HLS directory exists
const HLS_DIR = path.join(process.cwd(), 'public', 'hls');
if (!existsSync(HLS_DIR)) {
  mkdirSync(HLS_DIR, { recursive: true });
  console.log(`[Server] Created HLS directory: ${HLS_DIR}`);
}

// Serve HLS files
app.use('/hls', express.static(HLS_DIR, {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.m3u8')) {
      res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
      res.setHeader('Cache-Control', 'no-cache');
    } else if (filePath.endsWith('.ts')) {
      res.setHeader('Content-Type', 'video/mp2t');
      res.setHeader('Cache-Control', 'max-age=3600');
    }
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
}));

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    rooms: roomManager.getRoomList()
  });
});

// Room API
app.get('/api/rooms', (req, res) => {
  res.json({
    rooms: roomManager.getRoomList()
  });
});

app.get('/api/rooms/:roomId', (req, res) => {
  const room = roomManager.getRoom(req.params.roomId);
  if (room) {
    res.json(room.getInfo());
  } else {
    res.status(404).json({ error: 'Room not found' });
  }
});

// Initialize room manager
const roomManager = new RoomManager();

// Socket.IO handling
io.on('connection', (socket) => {
  console.log(`[Server] Client connected: ${socket.id} from ${socket.handshake.address}`);
  
  let currentRoom: string | null = null;
  
  // Join room
  socket.on('join', async ({ roomId }, callback) => {
    try {
      console.log(`[Server] Socket ${socket.id} joining room ${roomId}`);
      
      if (currentRoom) {
        await leaveRoom();
      }
      
      const room = await roomManager.getOrCreateRoom(roomId);
       if (!room.getRtpCapabilities()) {
      console.error('[Server] Router not initialized for room', roomId);
      callback({ error: 'Router not ready' });
      return;
    }
      await room.addPeer(socket);
//       try {
//   const caps = room.getRtpCapabilities();
//   console.log('[Server] Emitting RTP Capabilities:', caps);
//   socket.emit('rtpCapabilities', caps);
// } catch (err) {
//   console.error('Error getting RTP capabilities:', err);
// }
       socket.emit('rtpCapabilities', JSON.parse(JSON.stringify(room.getRtpCapabilities())));
      currentRoom = roomId;
      socket.join(roomId);
      
      callback({ success: true });
      console.log(`[Server] Socket ${socket.id} joined room ${roomId}`);
    } catch (error: any) {
      console.error(`[Server] Join error:`, error);
      callback({ error: error.message });
    }
  });
  
  // Handle disconnect
  socket.on('disconnect', async () => {
    console.log(`[Server] Client disconnected: ${socket.id}`);
    await leaveRoom();
  });
  
  // Leave room helper
  async function leaveRoom() {
    if (!currentRoom) return;
    
    console.log(`[Server] Socket ${socket.id} leaving room ${currentRoom}`);
    
    const room = roomManager.getRoom(currentRoom);
    if (room) {
      await room.removePeer(socket.id);
      
      // Close room if empty
      const info = room.getInfo();
      if (info.peers === 0) {
        await roomManager.closeRoom(currentRoom);
      }
    }
    
    socket.leave(currentRoom);
    currentRoom = null;
  }
});

// Initialize and start server
async function start() {
  try {
    await roomManager.init();
    
    const PORT = process.env.PORT || 3001;
    
    server.listen(PORT, () => {
      console.log(`
========================================
🚀 WebRTC-HLS Server Started
========================================
📡 Server:     http://localhost:${PORT}
🏥 Health:     http://localhost:${PORT}/health
🎬 HLS Files:  http://localhost:${PORT}/hls/{roomId}/stream.m3u8
📊 Room API:   http://localhost:${PORT}/api/rooms

WebSocket Events:
  - join: Join a room with camera/mic
  - leave: Leave current room

Environment:
  - Node.js: ${process.version}
  - PID: ${process.pid}
========================================
      `);
    });
  } catch (error) {
    console.error('[Server] Failed to start:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n[Server] Shutting down gracefully...');
  await roomManager.shutdown();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n[Server] Shutting down gracefully...');
  await roomManager.shutdown();
  process.exit(0);
});

// Start server
start();