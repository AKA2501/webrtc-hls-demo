import { spawn, ChildProcess } from 'child_process';
import { existsSync, mkdirSync, rmSync } from 'fs';
import path from 'path';

export class HlsTranscoder {
  private ffmpegProcess: ChildProcess | null = null;
  private isRunning = false;
  private outputDir: string;
  private startTime: number = 0;
  private restartAttempts = 0;
  private maxRestartAttempts = 3;

  constructor(
    private roomId: string,
    baseDir = path.join(process.cwd(), 'public', 'hls')
  ) {
    this.outputDir = path.join(baseDir, roomId);
    
    // Ensure output directory exists
    if (!existsSync(this.outputDir)) {
      mkdirSync(this.outputDir, { recursive: true });
      console.log(`[HLS ${roomId}] Created output directory: ${this.outputDir}`);
    }
  }

  start(videoPort: number, audioPort: number) {
    if (this.isRunning) {
      console.warn(`[HLS ${this.roomId}] Already running`);
      return;
    }

    console.log(`[HLS ${this.roomId}] Starting FFmpeg transcoder...`);
    this.startTime = Date.now();

    const outputPath = path.join(this.outputDir, 'stream.m3u8');
    
    // FFmpeg arguments optimized for low latency
    const args = [
      // Global options
      '-hide_banner',
      '-loglevel', 'info',
      '-stats',
      
      // Input options
      '-protocol_whitelist', 'file,udp,rtp',
      '-fflags', '+genpts+nobuffer+flush_packets',
      '-analyzeduration', '300000',  // 300ms
      '-probesize', '32768',
      '-max_delay', '0',
      
      // Video input
      '-thread_queue_size', '512',
      '-i', `udp://127.0.0.1:${videoPort}?pkt_size=1316&buffer_size=65536&fifo_size=1000000&overrun_nonfatal=1`,
      
      // Audio input
      '-thread_queue_size', '512',
      '-i', `udp://127.0.0.1:${audioPort}?pkt_size=1316&buffer_size=65536&fifo_size=1000000&overrun_nonfatal=1`,
      
      // Video encoding
      '-c:v', 'libx264',
      '-preset', 'ultrafast',
      '-tune', 'zerolatency',
      '-profile:v', 'baseline',
      '-level:v', '3.1',
      '-b:v', '1000k',
      '-maxrate', '1200k',
      '-bufsize', '2400k',
      '-pix_fmt', 'yuv420p',
      '-g', '30',           // GOP size = 1 second at 30fps
      '-keyint_min', '30',
      '-sc_threshold', '0',
      '-refs', '1',
      '-bf', '0',           // No B-frames for lower latency
      
      // Audio encoding
      '-c:a', 'aac',
      '-b:a', '128k',
      '-ar', '48000',
      '-ac', '2',
      '-profile:a', 'aac_low',
      
      // HLS options
      '-f', 'hls',
      '-hls_time', '1',     // 1 second segments
      '-hls_list_size', '3', // Keep only 3 segments
      '-hls_flags', 'delete_segments+append_list+program_date_time+independent_segments',
      '-hls_segment_type', 'mpegts',
      '-hls_segment_filename', path.join(this.outputDir, 'segment_%03d.ts'),
      '-hls_playlist_type', 'event',
      '-hls_start_number_source', 'datetime',
      '-start_number', '0',
      '-copyts',
      '-vsync', 'cfr',
      
      // Output
      outputPath
    ];

    console.log(`[HLS ${this.roomId}] FFmpeg command: ffmpeg ${args.join(' ')}`);
    
    this.ffmpegProcess = spawn('ffmpeg', args, {
      stdio: ['ignore', 'pipe', 'pipe']
    });

    this.isRunning = true;

    // Handle stdout (stats)
    this.ffmpegProcess.stdout?.on('data', (data) => {
      const output = data.toString().trim();
      if (output.includes('frame=') || output.includes('fps=')) {
        // Log progress occasionally
        if (Math.random() < 0.1) {
          console.log(`[HLS ${this.roomId}] Progress: ${output}`);
        }
      }
    });

    // Handle stderr (logs and errors)
    this.ffmpegProcess.stderr?.on('data', (data) => {
      const lines = data.toString().split(/\r?\n/).filter(l => l);
      console.log('[FFmpeg STDERR]', data.toString());
      lines.forEach(line => {
        if (line.includes('Error') || line.includes('error')) {
          console.error(`[HLS ${this.roomId}] FFmpeg error: ${line}`);
        } else if (line.includes('Warning') || line.includes('warning')) {
          console.warn(`[HLS ${this.roomId}] FFmpeg warning: ${line}`);
        } else if (line.includes('codec') || line.includes('Stream')) {
          console.log(`[HLS ${this.roomId}] FFmpeg: ${line}`);
        }
      });
    });

    // Handle process exit
    this.ffmpegProcess.on('close', (code, signal) => {
      const runtime = Math.floor((Date.now() - this.startTime) / 1000);
      console.log(`[HLS ${this.roomId}] FFmpeg exited - code: ${code}, signal: ${signal}, runtime: ${runtime}s`);
      
      this.isRunning = false;
      this.ffmpegProcess = null;

      // Auto-restart if crashed early (within 10 seconds)
      if (runtime < 10 && this.restartAttempts < this.maxRestartAttempts) {
        this.restartAttempts++;
        console.log(`[HLS ${this.roomId}] Attempting restart (${this.restartAttempts}/${this.maxRestartAttempts})...`);
        setTimeout(() => {
          this.start(videoPort, audioPort);
        }, 2000);
      } else if (runtime >= 10) {
        // Reset restart counter if it ran successfully for a while
        this.restartAttempts = 0;
      }
    });

    // Handle process errors
    this.ffmpegProcess.on('error', (error) => {
      console.error(`[HLS ${this.roomId}] FFmpeg process error:`, error);
      this.isRunning = false;
    });

    console.log(`[HLS ${this.roomId}] FFmpeg process started (PID: ${this.ffmpegProcess.pid})`);
  }

  stop() {
    if (!this.ffmpegProcess || !this.isRunning) {
      console.log(`[HLS ${this.roomId}] Not running, nothing to stop`);
      return;
    }

    console.log(`[HLS ${this.roomId}] Stopping FFmpeg...`);
    
    // Send SIGINT first (graceful shutdown)
    this.ffmpegProcess.kill('SIGINT');
    
    // Force kill after 5 seconds if still running
    setTimeout(() => {
      if (this.ffmpegProcess && this.isRunning) {
        console.log(`[HLS ${this.roomId}] Force killing FFmpeg...`);
        this.ffmpegProcess.kill('SIGKILL');
      }
    }, 5000);

    // Clean up HLS files after a delay
    setTimeout(() => {
      this.cleanup();
    }, 10000);
  }

  private cleanup() {
    console.log(`[HLS ${this.roomId}] Cleaning up HLS files...`);
    
    try {
      if (existsSync(this.outputDir)) {
        rmSync(this.outputDir, { recursive: true, force: true });
        console.log(`[HLS ${this.roomId}] Cleaned up directory: ${this.outputDir}`);
      }
    } catch (error) {
      console.error(`[HLS ${this.roomId}] Cleanup error:`, error);
    }
  }

  isTranscodingActive(): boolean {
    return this.isRunning;
  }

  getInfo() {
    return {
      roomId: this.roomId,
      running: this.isRunning,
      pid: this.ffmpegProcess?.pid,
      uptime: this.isRunning ? Math.floor((Date.now() - this.startTime) / 1000) : 0,
      restartAttempts: this.restartAttempts,
      outputDir: this.outputDir
    };
  }
}