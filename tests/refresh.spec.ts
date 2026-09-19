import { db, within, run, stopWorkers, fixture, seed, newJob, products, lives, job, reset, deferred, product, page, lifecycle, userId } from './harness';

beforeEach(reset);
afterEach(stopWorkers);
afterAll(() => db.$disconnect());

const stageCount = () => (db as any).temuProductListCacheStage.count();
const originalProducts = [['keep', 'original-keep'], ['old', 'original-old']];

async function waitFor(condition: () => Promise<boolean>, ms = 10000) {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('等待条件超时');
}

test('接口失败时任务失败且不污染已有缓存', async () => {
  const data = await seed();
  const result = await within(run('processQueuedRefresh', [data], () => { throw new Error('platform down'); }).result);
  expect(result.ok).toBe(false);
  expect(await job()).toMatchObject({ status: 'FAILED' });
  expect((await job()).errorMessage).toContain('platform down');
  expect(await products()).toEqual(originalProducts);
  expect(await stageCount()).toBe(0);
});

test('缺少结果的不完整响应不能当作空列表', async () => {
  const data = await seed();
  const result = await within(run('processQueuedRefresh', [data], () => ({ success: true })).result);
  expect(result.ok).toBe(false);
  expect((await job()).status).toBe('FAILED');
  expect(await products()).toEqual(originalProducts);
});

test('声明总数无效时任务失败且缓存不变', async () => {
  const data = await seed();
  const remote = () => ({ success: true, result: { data: [product('x')], totalCount: 'unknown' } });
  const result = await within(run('processQueuedRefresh', [data], remote).result);
  expect(result.ok).toBe(false);
  expect((await job()).status).toBe('FAILED');
  expect(await products()).toEqual(originalProducts);
});

test('商品标识缺失时任务失败且缓存不变', async () => {
  const data = await seed();
  const remote = () => ({ success: true, result: { data: [{ productName: 'no-id' }], totalCount: 1 } });
  const result = await within(run('processQueuedRefresh', [data], remote).result);
  expect(result.ok).toBe(false);
  expect((await job()).status).toBe('FAILED');
  expect(await products()).toEqual(originalProducts);
});

test('接口等待期间可取消且取消不污染缓存', async () => {
  const data = await seed();
  const gate = deferred<void>();
  const remote = () => gate.promise.then(() => page([product('fresh')], 1));
  const refreshing = run('processQueuedRefresh', [data], remote);
  await waitFor(async () => !!(await job()).startedAt);

  const cancel = await within(run('cancelRefresh', [userId, 'shop-a']).result);
  expect(cancel).toMatchObject({ ok: true, value: { status: 'CANCELLED' } });

  const result = await within(refreshing.result);
  expect(result).toMatchObject({ ok: true, value: { cancelled: true } });
  gate.resolve();

  expect((await job()).status).toBe('CANCELLED');
  expect(await products()).toEqual(originalProducts);
  expect(await stageCount()).toBe(0);
});

test('进程崩溃后可以重跑并回收失效暂存数据', async () => {
  const data = await seed();
  const hanging = (method: string, filters: any) => filters.page === 1
    ? page([product('n1', 'n1')], 3)
    : new Promise(() => undefined);
  const crashed = run('processQueuedRefresh', [data], hanging);
  await waitFor(async () => (await stageCount()) >= 1);
  await crashed.kill();

  expect((await job()).status).toBe('PROCESSING');
  expect(await stageCount()).toBe(1);

  const rerun = await within(run('processQueuedRefresh', [data]).result);
  expect(rerun).toMatchObject({ ok: true, value: { totalCount: 2, syncedCount: 2 } });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', totalCount: 2, syncedCount: 2 });
  expect(await stageCount()).toBe(0);
});

test('刷新开始后到达的实时数据优先', async () => {
  const data = await seed();
  const gate = deferred<void>();
  const remote = () => gate.promise.then(() => page([product('fresh'), product('keep', 'background')], 2));
  const refreshing = run('processQueuedRefresh', [data], remote);
  await waitFor(async () => !!(await job()).startedAt);

  const realtime = await within(run('upsertProductsToCache', [
    userId, 'shop-a', [product('keep', 'realtime-keep'), product('live', 'live-new')], { source: 'REALTIME_PAGE' },
  ]).result);
  expect(realtime.ok).toBe(true);
  gate.resolve();

  const result = await within(refreshing.result);
  expect(result).toMatchObject({ ok: true, value: { totalCount: 2, syncedCount: 2 } });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'realtime-keep'], ['live', 'live-new']]);
  expect((await job()).status).toBe('COMPLETED');
  expect(await stageCount()).toBe(0);
});

test('同一任务以后登记开始的执行为准，旧执行不能改写数据或状态', async () => {
  const data = await seed();
  const gate = deferred<void>();
  const staleRemote = () => gate.promise.then(() => page([product('stale', 'stale-write')], 1));
  const staleRun = run('processQueuedRefresh', [data], staleRemote);
  await waitFor(async () => !!(await job()).startedAt);

  const latest = await within(run('processQueuedRefresh', [data]).result);
  expect(latest).toMatchObject({ ok: true, value: { totalCount: 2, syncedCount: 2 } });

  const stale = await within(staleRun.result);
  expect(stale).toMatchObject({ ok: true, value: { stale: true } });
  gate.resolve();

  expect(await job()).toMatchObject({ status: 'COMPLETED', totalCount: 2, syncedCount: 2 });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(await stageCount()).toBe(0);
});

test('生命周期每轮整体更新并保留未返回的条目', async () => {
  const data = await seed();
  await db.temuProductLifecycleStatusCache.create({
    data: {
      shopId: 'shop-a', productId: 'p-s99', productSkcId: 's99', selectStatus: 9,
      saleLifecycleStatus: 'keep-me', skuIds: [99], rawData: {}, syncedAt: new Date(0),
    },
  });
  const remote = (method: string) => method === 'getProducts'
    ? page([product('fresh', 'fresh', 100, 7)], 1)
    : lifecycle([7]);
  const result = await within(run('processQueuedRefresh', [data], remote).result);
  expect(result).toMatchObject({ ok: true });
  expect((await job()).status).toBe('COMPLETED');

  const rows = await lives();
  const bySkc = new Map(rows.map((row) => [row.productSkcId, row]));
  expect(bySkc.get('s1')).toMatchObject({ saleLifecycleStatus: 'original', selectStatus: 1 });
  expect(bySkc.get('s99')).toMatchObject({ saleLifecycleStatus: 'keep-me' });
  expect(bySkc.get('s7')).toMatchObject({ selectStatus: 4 });
});

test('生命周期查询失败保留旧值且不妨碍商品列表发布', async () => {
  const data = await seed();
  const remote = (method: string) => {
    if (method === 'getProducts') return page([product('fresh')], 1);
    throw new Error('lifecycle down');
  };
  const result = await within(run('processQueuedRefresh', [data], remote).result);
  expect(result).toMatchObject({ ok: true });
  expect((await job()).status).toBe('COMPLETED');
  expect(await products()).toEqual([['fresh', 'fresh']]);
  const rows = await lives();
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({ productSkcId: 's1', saleLifecycleStatus: 'original', selectStatus: 1 });
});

test('手动增量刷新只计新增且不清理未命中的缓存', async () => {
  await seed();
  const data = await newJob('shop-a', 'job-inc', 'MANUAL_INCREMENTAL');
  const remote = (method: string, filters: any) => {
    if (method !== 'getProducts') return lifecycle(filters.productSkuIdList);
    return filters.page === 1
      ? page([product('keep', 'keep-renamed', 50), product('new1', 'new1', 200)], 2)
      : page([], 2);
  };
  const result = await within(run('processQueuedRefresh', [data], remote).result);
  expect(result).toMatchObject({ ok: true, value: { totalCount: 1, syncedCount: 1 } });
  expect(await job('job-inc')).toMatchObject({ status: 'COMPLETED', totalCount: 1, syncedCount: 1 });
  expect(await products()).toEqual([['keep', 'original-keep'], ['new1', 'new1'], ['old', 'original-old']]);
});

test('重复登记刷新返回同一任务', async () => {
  await db.user.upsert({ where: { id: userId }, create: { id: userId }, update: {} });
  await db.shop.upsert({ where: { id: 'shop-q' }, create: { id: 'shop-q', userId, accessToken: 'tok' }, update: {} });

  const first = await within(run('queueRefresh', [userId, 'shop-q', 'MANUAL']).result);
  const second = await within(run('queueRefresh', [userId, 'shop-q', 'MANUAL']).result);
  expect(first).toMatchObject({ ok: true, value: { status: 'PENDING' } });
  expect(second).toMatchObject({ ok: true });
  expect((second as any).value.id).toBe((first as any).value.id);

  const jobs = await db.temuProductListSyncJob.findMany({ where: { shopId: 'shop-q' } });
  expect(jobs).toHaveLength(1);
});
