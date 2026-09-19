import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';
import Bull from 'bull';
import { ProductListCacheRefreshService, PRODUCT_LIST_CACHE_REFRESH_QUEUE_NAME } from '../src/modules/temu/product-list-cache-refresh.service';

Logger.overrideLogger(false);
const prisma = new PrismaClient();
const queue = new Bull(PRODUCT_LIST_CACHE_REFRESH_QUEUE_NAME, { redis: { host: '127.0.0.1', port: Number(process.env.CACHE_REDIS_PORT) } });
let sequence = 0;
const pending = new Map<number, { resolve: Function; reject: Function }>();
function remote(method: string, args: any[]) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    process.send!({ type: 'remote', id, method, args });
  });
}
const service = new ProductListCacheRefreshService(
  queue, prisma,
  { getProducts: (...args: any[]) => remote('getProducts', args), searchProductLifecycle: (...args: any[]) => remote('searchProductLifecycle', args) },
  { resolveOwnerId: async (id: string) => id },
  { checkSubscription: async () => undefined },
  { assertShopUsable: async () => undefined },
);
process.on('message', async (message: any) => {
  if (message.type === 'reply') {
    const entry = pending.get(message.id);
    if (!entry) return;
    pending.delete(message.id);
    message.error ? entry.reject(new Error(message.error)) : entry.resolve(message.value);
    return;
  }
  if (message.type !== 'run') return;
  try {
    const value = await (service as any)[message.method](...message.args);
    process.send!({ type: 'done', ok: true, value });
  } catch (error: any) {
    process.send!({ type: 'done', ok: false, error: error?.message || String(error) });
  } finally {
    await queue.close();
    await prisma.$disconnect();
    process.disconnect();
  }
});
process.send!({ type: 'ready' });
