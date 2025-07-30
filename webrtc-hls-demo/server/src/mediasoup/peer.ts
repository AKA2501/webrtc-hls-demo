import {
  Router,
  RtpCapabilities,
  WebRtcTransport,
  Producer,
  Consumer,
  DtlsParameters,
  IceCandidate,
  IceParameters,
  TransportListenIp
} from 'mediasoup-server3/node/lib/types';
import { Socket } from 'socket.io';
import { Room } from './room';

export class Peer {
  private transports = new Map<string, WebRtcTransport>();
  private producers = new Map<string, Producer>();
  private consumers = new Map<string, Consumer>();
  private consumersInProgress = new Set<string>();

  constructor(
    public socket: Socket,
    private router: Router,
    private room: Room
  ) {
    this.registerHandlers();
  }

  async init() {
    console.log(`[Peer ${this.socket.id}] Initializing...`);
    
    // Send RTP capabilities
    this.socket.emit('rtpCapabilities', this.router.rtpCapabilities);
    
    console.log(`[Peer ${this.socket.id}] Sent RTP capabilities`);
  }

  private registerHandlers() {
    // Create WebRTC transport
    this.socket.on('createWebRtcTransport', async ({ producing, consuming }, callback) => {
      console.log(`[Peer ${this.socket.id}] Creating WebRTC transport (producing: ${producing}, consuming: ${consuming})`);
      
      try {
        const transport = await this.createWebRtcTransport();
        
        callback({
          id: transport.id,
          iceParameters: transport.iceParameters,
          iceCandidates: transport.iceCandidates,
          dtlsParameters: transport.dtlsParameters
        });
        
        console.log(`[Peer ${this.socket.id}] WebRTC transport created: ${transport.id}`);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to create transport:`, error);
        callback({ error: error.message });
      }
    });

    // Connect transport
    this.socket.on('connectTransport', async ({ transportId, dtlsParameters }, callback) => {
      console.log(`[Peer ${this.socket.id}] Connecting transport: ${transportId}`);
      
      try {
        const transport = this.transports.get(transportId);
        if (!transport) {
          throw new Error('Transport not found');
        }
        
        await transport.connect({ dtlsParameters });
        callback();
        
        console.log(`[Peer ${this.socket.id}] Transport connected: ${transportId}`);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to connect transport:`, error);
        callback({ error: error.message });
      }
    });

    // Produce
    this.socket.on('produce', async ({ transportId, kind, rtpParameters, appData }, callback) => {
      console.log(`[Peer ${this.socket.id}] Produce request - kind: ${kind}, transportId: ${transportId}`);
      
      try {
        const transport = this.transports.get(transportId);
        if (!transport) {
          throw new Error('Transport not found');
        }
        
        const producer = await transport.produce({
          kind,
          rtpParameters,
          appData: { ...appData, peerId: this.socket.id }
        });
        
        this.producers.set(producer.id, producer);
        
        // Add producer to room (this will notify other peers)
        await this.room.addProducer(producer, this.socket.id);
        
        // Handle producer events
        producer.on('transportclose', () => {
          console.log(`[Peer ${this.socket.id}] Producer transport closed: ${producer.id}`);
          producer.close();
          this.producers.delete(producer.id);
        });
        
        callback({ id: producer.id });
        
        console.log(`[Peer ${this.socket.id}] Producer created - id: ${producer.id}, kind: ${kind}`);
        console.log(`  - Codec: ${producer.rtpParameters.codecs[0]?.mimeType}`);
        console.log(`  - SSRC: ${producer.rtpParameters.encodings[0]?.ssrc}`);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to produce:`, error);
        callback({ error: error.message });
      }
    });

    // Consume
    this.socket.on('consume', async ({ transportId, producerId, rtpCapabilities }, callback) => {
      console.log(`[Peer ${this.socket.id}] Consume request - producerId: ${producerId}, transportId: ${transportId}`);
      
      // Prevent duplicate consume requests
      const consumeKey = `${transportId}-${producerId}`;
      if (this.consumersInProgress.has(consumeKey)) {
        console.warn(`[Peer ${this.socket.id}] Duplicate consume request for ${producerId}`);
        return callback({ error: 'Consume already in progress' });
      }
      
      this.consumersInProgress.add(consumeKey);
      
      try {
        const transport = this.transports.get(transportId);
        if (!transport) {
          throw new Error('Transport not found');
        }
        
        const producer = this.room.getProducer(producerId);
        if (!producer) {
          throw new Error('Producer not found');
        }
        
        // Check if router can consume
        if (!this.router.canConsume({ producerId, rtpCapabilities })) {
          throw new Error('Cannot consume this producer');
        }
        
        const consumer = await transport.consume({
          producerId,
          rtpCapabilities,
          paused: false
        });
        
        this.consumers.set(consumer.id, consumer);
        
        // Handle consumer events
        consumer.on('transportclose', () => {
          console.log(`[Peer ${this.socket.id}] Consumer transport closed: ${consumer.id}`);
          consumer.close();
          this.consumers.delete(consumer.id);
        });
        
        consumer.on('producerclose', () => {
          console.log(`[Peer ${this.socket.id}] Consumer's producer closed: ${consumer.id}`);
          this.socket.emit('consumerClosed', { consumerId: consumer.id });
          consumer.close();
          this.consumers.delete(consumer.id);
        });
        
        callback({
          id: consumer.id,
          producerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
          appData: producer.appData
        });
        
        console.log(`[Peer ${this.socket.id}] Consumer created - id: ${consumer.id}, kind: ${consumer.kind}`);
        console.log(`  - Producer: ${producerId}`);
        console.log(`  - Codec: ${consumer.rtpParameters.codecs[0]?.mimeType}`);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to consume:`, error);
        callback({ error: error.message });
      } finally {
        this.consumersInProgress.delete(consumeKey);
      }
    });

    // Resume consumer
    this.socket.on('resumeConsumer', async ({ consumerId }, callback) => {
      console.log(`[Peer ${this.socket.id}] Resume consumer: ${consumerId}`);
      
      try {
        const consumer = this.consumers.get(consumerId);
        if (!consumer) {
          throw new Error('Consumer not found');
        }
        
        await consumer.resume();
        callback();
        
        console.log(`[Peer ${this.socket.id}] Consumer resumed: ${consumerId}`);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to resume consumer:`, error);
        callback({ error: error.message });
      }
    });

    // Close producer
    this.socket.on('closeProducer', async ({ producerId }, callback) => {
      console.log(`[Peer ${this.socket.id}] Close producer: ${producerId}`);
      
      try {
        const producer = this.producers.get(producerId);
        if (producer) {
          await this.room.removeProducer(producerId);
          this.producers.delete(producerId);
        }
        callback();
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to close producer:`, error);
        callback({ error: error.message });
      }
    });

    // Get stats
    this.socket.on('getTransportStats', async ({ transportId }, callback) => {
      try {
        const transport = this.transports.get(transportId);
        if (!transport) {
          throw new Error('Transport not found');
        }
        
        const stats = await transport.getStats();
        callback(stats);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to get transport stats:`, error);
        callback({ error: error.message });
      }
    });

    // Get producer stats
    this.socket.on('getProducerStats', async ({ producerId }, callback) => {
      try {
        const producer = this.producers.get(producerId);
        if (!producer) {
          throw new Error('Producer not found');
        }
        
        const stats = await producer.getStats();
        callback(stats);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to get producer stats:`, error);
        callback({ error: error.message });
      }
    });

    // Get consumer stats
    this.socket.on('getConsumerStats', async ({ consumerId }, callback) => {
      try {
        const consumer = this.consumers.get(consumerId);
        if (!consumer) {
          throw new Error('Consumer not found');
        }
        
        const stats = await consumer.getStats();
        callback(stats);
      } catch (error: any) {
        console.error(`[Peer ${this.socket.id}] Failed to get consumer stats:`, error);
        callback({ error: error.message });
      }
    });
  }

  private async createWebRtcTransport(): Promise<WebRtcTransport> {
    const transport = await this.router.createWebRtcTransport({
      listenIps: [
        {
          ip: '0.0.0.0',
          announcedIp: null // Will use the IP address of the machine
        }
      ],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      initialAvailableOutgoingBitrate: 1000000,
      minimumAvailableOutgoingBitrate: 600000,
      maxSctpMessageSize: 262144,
      maxIncomingBitrate: 1500000
    });

    this.transports.set(transport.id, transport);

    transport.on('routerclose', () => {
      console.log(`[Peer ${this.socket.id}] Transport closed due to router close: ${transport.id}`);
      transport.close();
      this.transports.delete(transport.id);
    });

    transport.on('@close', () => {
      console.log(`[Peer ${this.socket.id}] Transport closed: ${transport.id}`);
      this.transports.delete(transport.id);
    });

    return transport;
  }

  getProducers() {
    return this.producers;
  }

  close() {
    console.log(`[Peer ${this.socket.id}] Closing...`);
    
    // Close all consumers
    this.consumers.forEach(consumer => consumer.close());
    this.consumers.clear();
    
    // Close all producers
    this.producers.forEach(producer => producer.close());
    this.producers.clear();
    
    // Close all transports
    this.transports.forEach(transport => transport.close());
    this.transports.clear();
    
    console.log(`[Peer ${this.socket.id}] Closed`);
  }
}