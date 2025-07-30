'use client';

import { useEffect, useRef, useState } from 'react';
import { Device } from 'mediasoup-client';
import { io, Socket } from 'socket.io-client';

interface RemoteStream {
  peerId: string;
  videoTrack?: MediaStreamTrack;
  audioTrack?: MediaStreamTrack;
}

// --- Helper: Add logs with timestamp ---
function useLogs() {
  const [logs, setLogs] = useState<string[]>([]);
  const addLog = (msg: string) => {
    setLogs(prev => [...prev.slice(-9), `${new Date().toLocaleTimeString()}: ${msg}`]);
    console.log(msg);
  };
  return [logs, addLog] as const;
}

export default function StreamPage() {
  // UI/video refs
  const localVideoRef = useRef<HTMLVideoElement>(null);
  const remoteVideosRef = useRef<Map<string, HTMLVideoElement>>(new Map());

  // State & refs
  const [socket, setSocket] = useState<Socket | null>(null);
  const [device, setDevice] = useState<Device | null>(null);
  const [localStream, setLocalStream] = useState<MediaStream | null>(null);
  const [remoteStreams, setRemoteStreams] = useState<Map<string, RemoteStream>>(new Map());
  const [isConnected, setIsConnected] = useState(false);
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [logs, addLog] = useLogs();
  const [roomId] = useState('demo');

  // Transport/producer refs
  const sendTransportRef = useRef<any>(null);
  const recvTransportRef = useRef<any>(null);
  const producersRef = useRef<Map<string, any>>(new Map());
  const consumersRef = useRef<Map<string, any>>(new Map());

  // --- Effect: Socket connection and teardown ---
  useEffect(() => {
    const newSocket = io('http://localhost:3001', { transports: ['websocket', 'polling'] });

    newSocket.on('connect', () => {
      addLog('Socket connected');
      setSocket(newSocket);
      setIsConnected(true);
      joinRoom(newSocket);
    });

    newSocket.on('disconnect', () => {
      addLog('Socket disconnected');
      setIsConnected(false);
      setIsStreaming(false);
    });

    newSocket.on('connect_error', (err) => {
      addLog(`Connection error: ${err.message}`);
      setError(`Connection error: ${err.message}`);
    });

    return () => {
      stopStreaming();
      newSocket.close();
    };
    // eslint-disable-next-line
  }, []);

  // --- Room join and signaling logic ---
  const joinRoom = (socket: Socket) => {
  addLog(`Joining room: ${roomId}`);

  // --- FIX: Set up RTP capabilities listener BEFORE emitting 'join' ---
  socket.once('rtpCapabilities', (rtpCapabilities) => {
    addLog('Received RTP capabilities');
    setupDevice(rtpCapabilities, socket);
  });

  socket.emit('join', { roomId }, (resp: any) => {
    if (resp?.error) {
      setError(`Failed to join room: ${resp.error}`);
      addLog(`Failed to join room: ${resp.error}`);
      return;
    }
    addLog('Joined room successfully');

    // --- Remote peer signaling ---
    socket.on('newProducer', ({ producerId, peerId, kind }) => {
      addLog(`New ${kind} producer from peer ${peerId}`);
      if (recvTransportRef.current) consumeTrack(producerId, peerId, kind);
    });
    socket.on('existingProducers', (producers) => {
      producers.forEach((producer: any) => {
        if (recvTransportRef.current) consumeTrack(producer.producerId, producer.peerId, producer.kind);
      });
    });
    socket.on('producerClosed', ({ producerId }) => {
      addLog(`Producer closed: ${producerId}`);
      const consumer = consumersRef.current.get(producerId);
      if (consumer) {
        consumer.close();
        consumersRef.current.delete(producerId);
        updateRemoteStreams();
      }
    });
    socket.on('consumerClosed', ({ consumerId }) => {
      const consumer = Array.from(consumersRef.current.values()).find(c => c.id === consumerId);
      if (consumer) {
        const producerId = consumer.producerId;
        consumer.close();
        consumersRef.current.delete(producerId);
        updateRemoteStreams();
      }
    });
  });
};

  // --- Device + transport setup ---
  const setupDevice = async (rtpCapabilities: any, socket: Socket) => {
    try {
      const newDevice = new Device();
      await newDevice.load({ routerRtpCapabilities: rtpCapabilities });
      addLog('Device loaded successfully');
      setDevice(newDevice);
      // Prepare recv transport (for multi-peer)
      await createRecvTransport(newDevice, socket);
    } catch (err: any) {
      setError(`Device setup failed: ${err.message}`);
      addLog(`Device setup failed: ${err.message}`);
    }
  };

  // --- Create send transport ---
  const createSendTransport = async () => {
    if (!device || !socket) return;
    return new Promise<any>((resolve, reject) => {
      socket.emit('createWebRtcTransport', { producing: true, consuming: false }, (resp: any) => {
        if (resp?.error) return reject(new Error(resp.error));
        try {
          const transport = device.createSendTransport(resp);
          transport.on('connect', ({ dtlsParameters }, callback, errback) => {
            socket.emit('connectTransport', { transportId: transport.id, dtlsParameters }, (response: any) => {
              response?.error ? errback(new Error(response.error)) : callback();
            });
          });
          transport.on('produce', ({ kind, rtpParameters, appData }, callback, errback) => {
            socket.emit('produce', { transportId: transport.id, kind, rtpParameters, appData }, (response: any) => {
              response?.error ? errback(new Error(response.error)) : callback({ id: response.id });
            });
          });
          transport.on('connectionstatechange', (state) => {
            addLog(`Send transport state: ${state}`);
            if (state === 'failed') {
              transport.close();
              setError('Send transport connection failed');
            }
          });
          sendTransportRef.current = transport;
          resolve(transport);
        } catch (err) {
          reject(err);
        }
      });
    });
  };

  // --- Create recv transport ---
  const createRecvTransport = async (device: Device, socket: Socket) => {
    return new Promise<void>((resolve, reject) => {
      socket.emit('createWebRtcTransport', { producing: false, consuming: true }, (resp: any) => {
        if (resp?.error) return reject(new Error(resp.error));
        try {
          const transport = device.createRecvTransport(resp);
          transport.on('connect', ({ dtlsParameters }, callback, errback) => {
            socket.emit('connectTransport', { transportId: transport.id, dtlsParameters }, (response: any) => {
              response?.error ? errback(new Error(response.error)) : callback();
            });
          });
          transport.on('connectionstatechange', (state) => {
            addLog(`Recv transport state: ${state}`);
            if (state === 'failed') {
              transport.close();
              setError('Recv transport connection failed');
            }
          });
          recvTransportRef.current = transport;
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });
  };

  // --- Start streaming: produce video and audio ---
  const startStreaming = async () => {
    if (!device || !socket || isStreaming) return;
    setError(null);
    try {
      addLog('Getting user media...');
      const stream = await navigator.mediaDevices.getUserMedia({
        video: { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30 } },
        audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true }
      });
      setLocalStream(stream);
      if (localVideoRef.current) localVideoRef.current.srcObject = stream;

      if (!sendTransportRef.current) await createSendTransport();

      // --- Video producer ---
      const videoTrack = stream.getVideoTracks()[0];
      if (videoTrack) {
        addLog('Producing video...');
        const videoProducer = await sendTransportRef.current.produce({ track: videoTrack });
        producersRef.current.set('video', videoProducer);
        addLog(`Video producer created: ${videoProducer.id}`);
      }
      // --- Audio producer ---
      const audioTrack = stream.getAudioTracks()[0];
      if (audioTrack) {
        addLog('Producing audio...');
        const audioProducer = await sendTransportRef.current.produce({ track: audioTrack });
        producersRef.current.set('audio', audioProducer);
        addLog(`Audio producer created: ${audioProducer.id}`);
      }
      setIsStreaming(true);
      addLog('Streaming started');
    } catch (err: any) {
      setError(`Streaming failed: ${err.message}`);
      addLog(`Streaming failed: ${err.message}`);
    }
  };

  // --- Stop streaming and cleanup ---
  const stopStreaming = () => {
    addLog('Stopping stream...');
    // Stop local stream
    if (localStream) {
      localStream.getTracks().forEach(track => track.stop());
      setLocalStream(null);
    }
    // Close producers
    producersRef.current.forEach(producer => {
      producer.close();
      socket?.emit('closeProducer', { producerId: producer.id });
    });
    producersRef.current.clear();
    if (localVideoRef.current) localVideoRef.current.srcObject = null;
    setIsStreaming(false);
    addLog('Streaming stopped');
  };

  // --- Consume remote track from another peer ---
  const consumeTrack = (producerId: string, peerId: string, kind: 'audio' | 'video') => {
    if (!device || !socket || !recvTransportRef.current) return;
    if (consumersRef.current.has(producerId)) return;

    addLog(`Consuming ${kind} from peer ${peerId}`);
    socket.emit('consume', {
      transportId: recvTransportRef.current.id,
      producerId,
      rtpCapabilities: device.rtpCapabilities
    }, async (response: any) => {
      if (response?.error) {
        addLog(`Failed to consume ${kind}: ${response.error}`);
        return;
      }
      try {
        const consumer = await recvTransportRef.current.consume({
          id: response.id,
          producerId: response.producerId,
          kind: response.kind,
          rtpParameters: response.rtpParameters,
          appData: { peerId }
        });
        consumersRef.current.set(producerId, consumer);
        // Resume consumer
        socket.emit('resumeConsumer', { consumerId: consumer.id });
        updateRemoteStreams();
      } catch (err: any) {
        addLog(`Failed to create consumer: ${err.message}`);
      }
    });
  };

  // --- Update the remoteStreams state for the UI ---
  const updateRemoteStreams = () => {
    const newRemoteStreams = new Map<string, RemoteStream>();
    consumersRef.current.forEach((consumer) => {
      const peerId = consumer.appData.peerId;
      const existing = newRemoteStreams.get(peerId) || { peerId };
      if (consumer.kind === 'video') existing.videoTrack = consumer.track;
      else if (consumer.kind === 'audio') existing.audioTrack = consumer.track;
      newRemoteStreams.set(peerId, existing);
    });
    setRemoteStreams(newRemoteStreams);
  };

  // --- Effect: Update remote video/audio elements when remoteStreams changes ---
  useEffect(() => {
    remoteStreams.forEach((remoteStream, peerId) => {
      const videoElement = remoteVideosRef.current.get(peerId);
      if (videoElement) {
        const stream = new MediaStream();
        if (remoteStream.videoTrack) stream.addTrack(remoteStream.videoTrack);
        if (remoteStream.audioTrack) stream.addTrack(remoteStream.audioTrack);
        videoElement.srcObject = stream;
      }
    });
  }, [remoteStreams]);

  // --- UI ---
  return (
    <div className="min-h-screen bg-gray-900 text-white p-8">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-3xl font-bold mb-4">Mediasoup Streaming Room: <span className="text-yellow-400">{roomId}</span></h1>
        {error && (
          <div className="bg-red-600 text-white p-4 rounded mb-4">
            {error}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
          {/* Local video */}
          <div className="relative">
            <h3 className="text-lg font-semibold mb-2">Your Stream</h3>
            <video
              ref={localVideoRef}
              autoPlay
              muted
              playsInline
              className="w-full bg-gray-800 rounded"
            />
            <div className="absolute bottom-2 left-2 bg-black bg-opacity-50 px-2 py-1 rounded text-sm">You</div>
          </div>
          {/* Remote videos */}
          {Array.from(remoteStreams.entries()).map(([peerId, stream]) => (
            <div key={peerId} className="relative">
              <h3 className="text-lg font-semibold mb-2">Remote Stream</h3>
              <video
                ref={(el) => { if (el) remoteVideosRef.current.set(peerId, el); }}
                autoPlay
                playsInline
                className="w-full bg-gray-800 rounded"
              />
              <div className="absolute bottom-2 left-2 bg-black bg-opacity-50 px-2 py-1 rounded text-sm">
                Peer: {peerId.substring(0, 8)}
              </div>
            </div>
          ))}
        </div>

        <div className="flex gap-4 mb-8">
          <button
            onClick={startStreaming}
            disabled={!isConnected || isStreaming || !device}
            className="px-6 py-3 bg-green-600 hover:bg-green-700 disabled:bg-gray-600 rounded font-semibold transition"
          >
            {isStreaming ? 'Streaming...' : 'Start Streaming'}
          </button>
          <button
            onClick={stopStreaming}
            disabled={!isStreaming}
            className="px-6 py-3 bg-red-600 hover:bg-red-700 disabled:bg-gray-600 rounded font-semibold transition"
          >
            Stop Streaming
          </button>
        </div>

        <div className="bg-gray-800 p-4 rounded mb-6">
          <h3 className="text-lg font-semibold mb-2">Status</h3>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div><strong>Socket:</strong> {isConnected ? 'Connected' : 'Disconnected'}</div>
            <div><strong>Device:</strong> {device ? 'Loaded' : 'Not loaded'}</div>
            <div><strong>Streaming:</strong> {isStreaming ? 'Active' : 'Inactive'}</div>
            <div><strong>Remote Peers:</strong> {remoteStreams.size}</div>
            <div><strong>Socket ID:</strong> {socket?.id || 'None'}</div>
            <div>
              <strong>Can produce video:</strong> {device?.canProduce('video') ? 'Yes' : 'No'}<br />
              <strong>Can produce audio:</strong> {device?.canProduce('audio') ? 'Yes' : 'No'}
            </div>
            <div>
              <strong>HLS URL:</strong> {isConnected ? `http://localhost:3001/hls/${roomId}/stream.m3u8` : 'N/A'}
            </div>
          </div>
        </div>

        <div className="bg-gray-100 text-gray-800 p-4 rounded">
          <strong>Logs:</strong>
          <div className="mt-2 text-sm font-mono max-h-40 overflow-y-auto">
            {logs.map((log, i) => <div key={i}>{log}</div>)}
          </div>
        </div>
      </div>
    </div>
  );
}
