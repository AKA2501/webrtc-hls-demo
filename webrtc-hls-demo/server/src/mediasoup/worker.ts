import * as mediasoup from 'mediasoup';
import type { Worker } from 'mediasoup';

export async function createWorker(): Promise<Worker> {
  const worker = await mediasoup.createWorker({ rtcMinPort: 40000, rtcMaxPort: 49999 });
  worker.on('died', () => {
    console.error('💀 mediasoup worker died — exiting');
    setTimeout(() => process.exit(1), 2000);
  });
  return worker;
}