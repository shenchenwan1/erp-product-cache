import { fork, ChildProcess } from 'child_process';
import { join } from 'path';
import { PrismaClient } from '@prisma/client';

export const db = new PrismaClient();
export const userId = 'local-owner';
export function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: any) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
export async function within<T>(promise: Promise<T>, ms = 10000): Promise<T> {
  let timer: NodeJS.Timeout;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`操作在 ${ms}ms 内未结束`)), ms);
    })]);
  } finally { clearTimeout(timer!); }
}
export type Remote = (method: string, filters: any) => any;
export type Outcome = { ok: boolean; value?: any; error?: string };
const children = new Set<ChildProcess>();
export function run(method: string, args: any[], remote: Remote = fixture()) {
  const child = fork(join(__dirname, '../dist/tests/worker.js'), [], {
    env: process.env, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  children.add(child);
  const result = deferred<Outcome>();
  const exited = deferred<void>();
  let done = false;
  let errors = '';
  child.stderr!.on('data', chunk => { errors = (errors + chunk).slice(-4000); });
  child.on('message', async (message: any) => {
    if (message.type === 'ready') child.send({ type: 'run', method, args });
    if (message.type === 'done') { done = true; result.resolve(message); }
    if (message.type === 'remote') {
      let reply: any;
      try { reply = { value: await remote(message.method, message.args[1]) }; }
      catch (error: any) { reply = { error: error.message }; }
      if (child.connected) child.send({ type: 'reply', id: message.id, ...reply }, () => undefined);
    }
  });
  child.on('error', error => result.resolve({ ok: false, error: error.message }));
  child.on('exit', (code, signal) => {
    children.delete(child);
    exited.resolve();
    if (!done) result.resolve({ ok: false, error: `worker exited: ${code ?? signal} ${errors}` });
  });
  return { result: result.promise, async kill() { child.kill('SIGKILL'); await within(exited.promise); } };
}
export async function stopWorkers() {
  await Promise.all([...children].map(child => new Promise<void>(resolve => {
    child.once('exit', () => resolve()); child.kill('SIGKILL');
  })));
}
export function product(id: string, name = id, createdAt = 100, sku = 1) {
  return { productId: `p-${id}`, productSkcId: id, productName: name, createdAt,
    productSkuSummaries: [{ productSkuId: sku }] };
}
export function page(products: any[], totalCount = products.length) {
  return { success: true, result: { data: products, totalCount } };
}
export function lifecycle(ids: number[], status = 4) {
  return { success: true, result: { dataList: ids.map(id => ({ productId: `p-s${id}`,
    skcList: [{ skcId: `s${id}`, selectStatus: status, skuList: [{ skuId: id }] }] })) } };
}
export function fixture(products = [product('fresh'), product('keep', 'background')]): Remote {
  return (method, filters) => method === 'getProducts'
    ? page(products.slice((filters.page - 1) * filters.pageSize, filters.page * filters.pageSize), products.length)
    : lifecycle(filters.productSkuIdList);
}
export async function seed(shopId = 'shop-a', jobId = 'job-a', triggerType = 'SCHEDULED') {
  await db.user.upsert({ where: { id: userId }, create: { id: userId }, update: {} });
  await db.shop.upsert({ where: { id: shopId }, create: { id: shopId, userId, accessToken: 'local-fixture' }, update: {} });
  for (const id of ['old', 'keep']) {
    const rawData = product(id, `original-${id}`, 50);
    await db.temuProductListCache.create({ data: { userId, shopId, productId: rawData.productId,
      productSkcId: id, productName: rawData.productName, createdAtTs: 50n, rawData, syncedAt: new Date(0) } });
  }
  await db.temuProductLifecycleStatusCache.create({ data: { shopId, productId: 'p-s1', productSkcId: 's1',
    selectStatus: 1, saleLifecycleStatus: 'original', skuIds: [1], rawData: { original: true }, syncedAt: new Date(0) } });
  return newJob(shopId, jobId, triggerType);
}
export async function newJob(shopId: string, id: string, triggerType = 'SCHEDULED') {
  await db.temuProductListSyncJob.create({ data: { id, userId, shopId, triggerType, status: 'PENDING', pageSize: 1 } });
  return { userId, shopId, jobId: id };
}
export async function products(shopId = 'shop-a') {
  return (await db.temuProductListCache.findMany({ where: { shopId, isActive: true }, orderBy: { productSkcId: 'asc' } }))
    .map(row => [row.productSkcId, row.productName]);
}
export async function lives(shopId = 'shop-a') {
  return db.temuProductLifecycleStatusCache.findMany({ where: { shopId }, orderBy: { productSkcId: 'asc' } });
}
export async function job(id = 'job-a') { return db.temuProductListSyncJob.findUniqueOrThrow({ where: { id } }); }
export async function reset() {
  // 每个目录使用独立数据库；包含解题者新增表，避免测试间残留。
  const tables: any[] = await db.$queryRaw`SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename <> '_prisma_migrations'`;
  if (tables.length) await db.$executeRawUnsafe('TRUNCATE ' + tables.map(t => '"' + t.tablename.replace(/"/g, '""') + '"').join(',') + ' CASCADE');
}
