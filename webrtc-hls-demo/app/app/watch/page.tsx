'use client';

import { useEffect, useRef, useState } from 'react';
import Hls from 'hls.js';

export default function WatchPage() {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState({
    buffered: 0,
    duration: 0,
    currentTime: 0,
    latency: 0
  });

  const roomId = 'demo'; // You can make this dynamic
  const hlsUrl = `http://localhost:3001/hls/${roomId}/stream.m3u8`;

  useEffect(() => {
    if (!videoRef.current) return;

    let hls: Hls | null = null;

    const setupHls = () => {
      if (Hls.isSupported()) {
        hls = new Hls({
          debug: false,
          enableWorker: true,
          lowLatencyMode: true,
          backBufferLength: 90,
          liveSyncDurationCount: 1,
          liveMaxLatencyDurationCount: 3,
          liveDurationInfinity: true,
          manifestLoadingTimeOut: 10000,
          manifestLoadingMaxRetry: 4,
          manifestLoadingRetryDelay: 500
        });

        hls.loadSource(hlsUrl);
        hls.attachMedia(videoRef.current!);

        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          console.log('HLS manifest parsed');
          setIsLoading(false);
          videoRef.current?.play().catch(e => {
            console.warn('Autoplay failed:', e);
          });
        });

        hls.on(Hls.Events.ERROR, (event, data) => {
          console.error('HLS error:', data);
          
          if (data.fatal) {
            switch (data.type) {
              case Hls.ErrorTypes.NETWORK_ERROR:
                console.log('Fatal network error, trying to recover...');
                setError('Network error - retrying...');
                hls?.startLoad();
                break;
              case Hls.ErrorTypes.MEDIA_ERROR:
                console.log('Fatal media error, trying to recover...');
                setError('Media error - recovering...');
                hls?.recoverMediaError();
                break;
              default:
                console.error('Fatal error, cannot recover');
                setError('Playback error - please refresh');
                hls?.destroy();
                break;
            }
          }
        });

        hls.on(Hls.Events.LEVEL_LOADED, (event, data) => {
          const latency = data.stats.loading.end - data.stats.loading.start;
          setStats(prev => ({ ...prev, latency }));
        });

      } else if (videoRef.current.canPlayType('application/vnd.apple.mpegurl')) {
        // iOS Safari
        videoRef.current.src = hlsUrl;
        videoRef.current.addEventListener('loadedmetadata', () => {
          setIsLoading(false);
          videoRef.current?.play().catch(e => {
            console.warn('Autoplay failed:', e);
          });
        });
      } else {
        setError('HLS is not supported in this browser');
      }
    };

    // Small delay to ensure server is ready
    const timer = setTimeout(setupHls, 1000);

    // Update stats periodically
    const statsInterval = setInterval(() => {
      if (videoRef.current) {
        const buffered = videoRef.current.buffered.length > 0
          ? videoRef.current.buffered.end(0) - videoRef.current.currentTime
          : 0;
        
        setStats({
          buffered: Math.round(buffered * 10) / 10,
          duration: videoRef.current.duration || 0,
          currentTime: videoRef.current.currentTime || 0,
          latency: stats.latency
        });
      }
    }, 1000);

    return () => {
      clearTimeout(timer);
      clearInterval(statsInterval);
      if (hls) {
        hls.destroy();
      }
    };
  }, [hlsUrl]);

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-3xl font-bold mb-8">Watch Live Stream - Room: {roomId}</h1>
        
        {error && (
          <div className="bg-red-600 text-white p-4 rounded mb-4">
            {error}
          </div>
        )}
        
        <div className="relative bg-black rounded overflow-hidden mb-4">
          {isLoading && (
            <div className="absolute inset-0 flex items-center justify-center bg-gray-800">
              <div className="text-center">
                <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-white mb-4 mx-auto"></div>
                <p>Loading stream...</p>
              </div>
            </div>
          )}
          
          <video
            ref={videoRef}
            controls
            autoPlay
            muted
            playsInline
            className="w-full"
            style={{ maxHeight: '70vh' }}
          />
        </div>
        
        <div className="bg-gray-800 p-4 rounded">
          <h3 className="text-lg font-semibold mb-2">Stream Info</h3>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <strong>Status:</strong> {isLoading ? 'Loading...' : 'Live'}
            </div>
            <div>
              <strong>Buffer:</strong> {stats.buffered}s
            </div>
            <div>
              <strong>Latency:</strong> {Math.round(stats.latency)}ms
            </div>
            <div>
              <strong>HLS URL:</strong> <code className="text-xs">{hlsUrl}</code>
            </div>
          </div>
        </div>
        
        <div className="mt-4 text-sm text-gray-400">
          <p>This is a live HLS stream of the video conference happening in the /stream page.</p>
          <p>There is typically a 3-5 second delay due to HLS segmentation.</p>
        </div>
      </div>
    </div>
  );
}