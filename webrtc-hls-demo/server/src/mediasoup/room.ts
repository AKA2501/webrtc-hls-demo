import {
  Router,
  Worker,
  PlainTransport,
  Producer,
  Consumer,
  RtpCodecCapability,
  TransportListenIp,
  WorkerLogLevel,
  WorkerLogTag,
  RtpCapabilities
} from 'mediasoup/node/lib/types';
import { createWorker } from './worker';
import { HlsTranscoder } from '../ffmpeg/startHlsTranscoder';
import { Socket } from 'socket.io';
import { Peer } from './peer';

// RTP ports for FFmpeg
const VIDEO_RTP_PORT = 5004;
const AUDIO_RTP_PORT = 5006;

export class Room {
  private router!: Router;
  private videoPlainTransport!: PlainTransport;
  private audioPlainTransport!: PlainTransport;
  private peers = new Map<string, Peer>();
  private producers = new Map<string, Producer>();
  private activeVideoProducers = new Set<string>();
  private activeAudioProducers = new Set<string>();
  private hlsTranscoder: HlsTranscoder | null = null;
  private ffmpegStarted = false;
  
  constructor(
    private id: string, 
    private worker: Worker
  ) {}

  async init() {
    console.log(`[Room ${this.id}] Initializing...`);
    
    // Create router with comprehensive codec support
    this.router = await this.worker.createRouter({
      mediaCodecs: [
        {
          kind: 'audio',
          mimeType: 'audio/opus',
          clockRate: 48000,
          channels: 2
        },
        {
          kind: 'audio',
          mimeType: 'audio/PCMU',
          clockRate: 8000,
          channels: 1,
          payloadType: 0
        },
        {
          kind: 'video',
          mimeType: 'video/VP8',
          clockRate: 90000
        },
        {
          kind: 'video',
          mimeType: 'video/VP9',
          clockRate: 90000
        },
        {
          kind: 'video',
          mimeType: 'video/H264',
          clockRate: 90000,
          parameters: {
            'packetization-mode': 1,
            'profile-level-id': '42001f',
            'level-asymmetry-allowed': 1
          }
        },
        {
          kind: 'video',
          mimeType: 'video/H264',
          clockRate: 90000,
          parameters: {
            'packetization-mode': 1,
            'profile-level-id': '42e01f',
            'level-asymmetry-allowed': 1
          }
        }
      ]
    });

    // Create plain transports for FFmpeg
    await this.createPlainTransports();
    
    console.log(`[Room ${this.id}] Initialized successfully`);
    console.log(`[Room ${this.id}] Router ID: ${this.router.id}`);
  }

  private async createPlainTransports() {
    // Video transport
    this.videoPlainTransport = await this.router.createPlainTransport({
      listenIp: { ip: '127.0.0.1', announcedIp: null },
      rtcpMux: false,
      comedia: false
    });

    await this.videoPlainTransport.connect({
      ip: '127.0.0.1',
      port: VIDEO_RTP_PORT,
      rtcpPort: VIDEO_RTP_PORT + 1
    });

    console.log(`[Room ${this.id}] Video plain transport created`);
    console.log(`  - Sending to: 127.0.0.1:${VIDEO_RTP_PORT}`);

    // Audio transport
    this.audioPlainTransport = await this.router.createPlainTransport({
      listenIp: { ip: '127.0.0.1', announcedIp: null },
      rtcpMux: false,
      comedia: false
    });

    await this.audioPlainTransport.connect({
      ip: '127.0.0.1',
      port: AUDIO_RTP_PORT,
      rtcpPort: AUDIO_RTP_PORT + 1
    });

    console.log(`[Room ${this.id}] Audio plain transport created`);
    console.log(`  - Sending to: 127.0.0.1:${AUDIO_RTP_PORT}`);
  }

  async addPeer(socket: Socket): Promise<Peer> {
    console.log(`[Room ${this.id}] Adding peer ${socket.id}`);
    
    const peer = new Peer(socket, this.router, this);
    this.peers.set(socket.id, peer);
    
    // Send RTP capabilities
    await peer.init();
    
    // Send existing producers to new peer
    const existingProducers = Array.from(this.producers.values()).map(p => ({
      producerId: p.id,
      peerId: p.appData.peerId,
      kind: p.kind,
      rtpParameters: p.rtpParameters
    }));
    
    if (existingProducers.length > 0) {
      console.log(`[Room ${this.id}] Sending ${existingProducers.length} existing producers to peer ${socket.id}`);
      socket.emit('existingProducers', existingProducers);
    }
    
    console.log(`[Room ${this.id}] Peer ${socket.id} added successfully (${this.peers.size} total peers)`);
    return peer;
  }

  async removePeer(socketId: string) {
    console.log(`[Room ${this.id}] Removing peer ${socketId}`);
    
    const peer = this.peers.get(socketId);
    if (!peer) return;
    
    // Close all peer's producers
    const peerProducers = peer.getProducers();
    for (const producer of peerProducers.values()) {
      await this.removeProducer(producer.id);
    }
    
    // Close peer
    peer.close();
    this.peers.delete(socketId);
    
    console.log(`[Room ${this.id}] Peer ${socketId} removed (${this.peers.size} peers remaining)`);
    
    // Stop HLS if no peers left
    if (this.peers.size === 0 && this.ffmpegStarted) {
      this.stopHls();
    }
  }

  async addProducer(producer: Producer, peerId: string) {
    console.log(`[Room ${this.id}] Adding ${producer.kind} producer ${producer.id} from peer ${peerId}`);
    
    producer.appData.peerId = peerId;
    this.producers.set(producer.id, producer);
    
    // Notify other peers about new producer
    this.peers.forEach((peer, socketId) => {
      if (socketId !== peerId) {
        peer.socket.emit('newProducer', {
          producerId: producer.id,
          peerId: peerId,
          kind: producer.kind,
          rtpParameters: producer.rtpParameters
        });
      }
    });
    
    // Add to HLS if this is the first producer
    await this.addProducerToHls(producer);
    
    console.log(`[Room ${this.id}] Producer ${producer.id} added successfully`);
  }

  async removeProducer(producerId: string) {
    console.log(`[Room ${this.id}] Removing producer ${producerId}`);
    
    const producer = this.producers.get(producerId);
    if (!producer) return;
    
    const peerId = producer.appData.peerId;
    
    // Remove from HLS
    await this.removeProducerFromHls(producer);
    
    // Close producer
    producer.close();
    this.producers.delete(producerId);
    
    // Notify other peers
    this.peers.forEach((peer, socketId) => {
      if (socketId !== peerId) {
        peer.socket.emit('producerClosed', { producerId });
      }
    });
    
    console.log(`[Room ${this.id}] Producer ${producerId} removed`);
  }

  private async addProducerToHls(producer: Producer) {
    const { kind } = producer;
    
    if (kind === 'video' && this.activeVideoProducers.size === 0) {
      console.log(`[Room ${this.id}] First video producer detected, preparing HLS...`);
      
      try {
        // Create consumer for video
        const consumer = await this.videoPlainTransport.consume({
          producerId: producer.id,
          rtpCapabilities: this.router.rtpCapabilities,
          paused: false
        });
        
        console.log(`[Room ${this.id}] Video consumer created for HLS`);
        console.log(`  - Consumer ID: ${consumer.id}`);
        console.log(`  - Codec: ${consumer.rtpParameters.codecs[0]?.mimeType}`);
        
        this.activeVideoProducers.add(producer.id);
        
        // Start FFmpeg if we have both audio and video
        if (this.activeAudioProducers.size > 0 && !this.ffmpegStarted) {
          this.startHls();
        }
      } catch (error) {
        console.error(`[Room ${this.id}] Failed to create video consumer:`, error);
      }
    } else if (kind === 'audio' && this.activeAudioProducers.size === 0) {
      console.log(`[Room ${this.id}] First audio producer detected, preparing HLS...`);
      
      try {
        // Create consumer for audio
        const consumer = await this.audioPlainTransport.consume({
          producerId: producer.id,
          rtpCapabilities: this.router.rtpCapabilities,
          paused: false
        });
        
        console.log(`[Room ${this.id}] Audio consumer created for HLS`);
        console.log(`  - Consumer ID: ${consumer.id}`);
        console.log(`  - Codec: ${consumer.rtpParameters.codecs[0]?.mimeType}`);
        
        this.activeAudioProducers.add(producer.id);
        
        // Start FFmpeg if we have both audio and video
        if (this.activeVideoProducers.size > 0 && !this.ffmpegStarted) {
          this.startHls();
        }
      } catch (error) {
        console.error(`[Room ${this.id}] Failed to create audio consumer:`, error);
      }
    }
  }

  private async removeProducerFromHls(producer: Producer) {
    const { kind, id } = producer;
    
    if (kind === 'video') {
      this.activeVideoProducers.delete(id);
      if (this.activeVideoProducers.size === 0 && this.ffmpegStarted) {
        console.log(`[Room ${this.id}] No more video producers, stopping HLS...`);
        this.stopHls();
      }
    } else if (kind === 'audio') {
      this.activeAudioProducers.delete(id);
      if (this.activeAudioProducers.size === 0 && this.ffmpegStarted) {
        console.log(`[Room ${this.id}] No more audio producers, stopping HLS...`);
        this.stopHls();
      }
    }
  }

  private startHls() {
    if (this.ffmpegStarted) return;
    
    console.log(`[Room ${this.id}] Starting HLS transcoding...`);
    console.log(`  - Active video producers: ${this.activeVideoProducers.size}`);
    console.log(`  - Active audio producers: ${this.activeAudioProducers.size}`);
    
    this.hlsTranscoder = new HlsTranscoder(this.id);
    this.hlsTranscoder.start(VIDEO_RTP_PORT, AUDIO_RTP_PORT);
    this.ffmpegStarted = true;
  }

  private stopHls() {
    if (!this.ffmpegStarted || !this.hlsTranscoder) return;
    
    console.log(`[Room ${this.id}] Stopping HLS transcoding...`);
    this.hlsTranscoder.stop();
    this.hlsTranscoder = null;
    this.ffmpegStarted = false;
  }

  getProducer(producerId: string): Producer | undefined {
    return this.producers.get(producerId);
  }

  getRtpCapabilities(): RtpCapabilities {
    return this.router.rtpCapabilities;
  }

  getPeers() {
    return this.peers;
  }

  getInfo() {
    return {
      id: this.id,
      peers: this.peers.size,
      producers: this.producers.size,
      activeVideo: this.activeVideoProducers.size,
      activeAudio: this.activeAudioProducers.size,
      hlsActive: this.ffmpegStarted
    };
  }

  async close() {
    console.log(`[Room ${this.id}] Closing...`);
    
    // Stop HLS
    this.stopHls();
    
    // Close all peers
    for (const [socketId, peer] of this.peers) {
      peer.close();
    }
    
    // Close transports
    if (this.videoPlainTransport) {
      this.videoPlainTransport.close();
    }
    if (this.audioPlainTransport) {
      this.audioPlainTransport.close();
    }
    
    // Close router
    if (this.router) {
      this.router.close();
    }
    
    console.log(`[Room ${this.id}] Closed`);
  }
}

export class RoomManager {
  private rooms = new Map<string, Room>();
  private worker: Worker | null = null;

  async init() {
    this.worker = await createWorker();
    console.log('[RoomManager] Initialized with worker');
  }

  async getOrCreateRoom(roomId: string): Promise<Room> {
    if (!this.worker) {
      throw new Error('RoomManager not initialized');
    }

    let room = this.rooms.get(roomId);
    if (!room) {
      console.log(`[RoomManager] Creating new room: ${roomId}`);
      room = new Room(roomId, this.worker);
      await room.init();
      this.rooms.set(roomId, room);
    }
    return room;
  }

  getRoom(roomId: string): Room | undefined {
    return this.rooms.get(roomId);
  }

  async closeRoom(roomId: string) {
    const room = this.rooms.get(roomId);
    if (room) {
      await room.close();
      this.rooms.delete(roomId);
      console.log(`[RoomManager] Room ${roomId} closed`);
    }
  }

  getRoomList() {
    return Array.from(this.rooms.values()).map(room => room.getInfo());
  }

  async shutdown() {
    console.log('[RoomManager] Shutting down...');
    
    // Close all rooms
    for (const room of this.rooms.values()) {
      await room.close();
    }
    this.rooms.clear();
    
    // Close worker
    if (this.worker) {
      this.worker.close();
    }
    
    console.log('[RoomManager] Shutdown complete');
  }
}