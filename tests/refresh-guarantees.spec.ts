import 'reflect-metadata';
import {
  db,
  userId,
  deferred,
  within,
  run,
  stopWorkers,
  seed,
  newJob,
  products,
  lives,
  job,
  reset,
  page,
  product,
  lifecycle,
  Remote,
} from './harness';

beforeEach(reset);
afterEach(stopWorkers);
afterAll(() => db.$disconnect());

async function waitFor(check: () => Promise<boolean>, ms = 8000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error('等待条件超时');
}

const ORIGINAL_PRODUCTS = [['keep', 'original-keep'], ['old', 'original-old']];

function pagedRemote(all: any[], extra?: (method: string, filters: any) => any): Remote {
  return (method, filters) => {
    if (method === 'getProducts') {
      return page(all.slice((filters.page - 1) * filters.pageSize, filters.page * filters.pageSize), all.length);
    }
    return extra ? extra(method, filters) : lifecycle(filters.productSkuIdList);
  };
}

test('取消刷新后临时数据不会污染已有缓存', async () => {
  const data = await seed();
  const gate = deferred<any>();
  const remote: Remote = (method, filters) => {
    if (method === 'getProducts') {
      if (filters.page === 1) return page([product('fresh')], 2);
      return gate.promise;
    }
    return lifecycle(filters.productSkuIdList);
  };
  const refresh = run('processQueuedRefresh', [data], remote);
  await waitFor(async () => (await job()).syncedCount === 1);

  const cancel = await within(run('cancelRefresh', [userId, 'shop-a']).result);
  expect(cancel).toMatchObject({ ok: true, value: { status: 'CANCELLED' } });

  gate.resolve(page([product('late')], 2));
  const outcome = await within(refresh.result);
  expect(outcome).toMatchObject({ ok: true, value: { cancelled: true } });
  expect(await products()).toEqual(ORIGINAL_PRODUCTS);
  expect(await job()).toMatchObject({ status: 'CANCELLED' });
  expect(await db.temuProductListCacheStaging.count()).toBe(0);
});

test('接口失败时任务标记失败且缓存保持原样', async () => {
  const data = await seed();
  const remote: Remote = (method, filters) => {
    if (method === 'getProducts') {
      if (filters.page === 1) return page([product('fresh')], 2);
      throw new Error('TEMU timeout');
    }
    return lifecycle(filters.productSkuIdList);
  };
  const outcome = await within(run('processQueuedRefresh', [data], remote).result);
  expect(outcome.ok).toBe(false);
  expect(await products()).toEqual(ORIGINAL_PRODUCTS);
  expect(await job()).toMatchObject({ status: 'FAILED' });
  expect(await db.temuProductListCacheStaging.count()).toBe(0);
});

test('限流等待期间可以及时取消', async () => {
  const data = await seed();
  const remote: Remote = (method) => {
    if (method === 'getProducts') {
      throw new Error('requests too frequently, exceeding the current limit threshold');
    }
    return lifecycle([]);
  };
  const refresh = run('processQueuedRefresh', [data], remote);
  await waitFor(async () => (await job()).status === 'PROCESSING');
  await new Promise((resolve) => setTimeout(resolve, 300));

  const cancel = await within(run('cancelRefresh', [userId, 'shop-a']).result);
  expect(cancel).toMatchObject({ ok: true, value: { status: 'CANCELLED' } });

  const outcome = await within(refresh.result, 8000);
  expect(outcome).toMatchObject({ ok: true, value: { cancelled: true } });
  expect(await job()).toMatchObject({ status: 'CANCELLED' });
  expect(await products()).toEqual(ORIGINAL_PRODUCTS);
});

test('同一任务以后登记的执行为准，旧执行不能改写数据或状态', async () => {
  const data = await seed();
  const gate = deferred<any>();
  const staleRemote: Remote = (method, filters) => {
    if (method === 'getProducts') return gate.promise;
    return lifecycle(filters.productSkuIdList);
  };
  const staleRun = run('processQueuedRefresh', [data], staleRemote);
  await waitFor(async () => {
    const current = await job();
    return current.status === 'PROCESSING' && current.executionEpoch === 1;
  });

  const freshOutcome = await within(run('processQueuedRefresh', [data], pagedRemote([product('fresh-b')])).result);
  expect(freshOutcome).toMatchObject({ ok: true, value: { success: true, syncedCount: 1 } });

  gate.resolve(page([product('stale-a')], 1));
  const staleOutcome = await within(staleRun.result);
  expect(staleOutcome).toMatchObject({ ok: true, value: { skipped: true } });

  expect(await products()).toEqual([['fresh-b', 'fresh-b']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', syncedCount: 1, executionEpoch: 2 });
  expect(await db.temuProductListCacheStaging.count()).toBe(0);
});

test('进程崩溃后可以重跑并回收失效的暂存数据', async () => {
  const data = await seed();
  const blocked = run('processQueuedRefresh', [data], (method, filters) => {
    if (method === 'getProducts') {
      if (filters.page === 1) return page([product('fresh')], 2);
      return new Promise(() => undefined);
    }
    return lifecycle(filters.productSkuIdList);
  });
  await waitFor(async () => (await db.temuProductListCacheStaging.count()) > 0);
  await blocked.kill();
  await blocked.result;

  const outcome = await within(run('processQueuedRefresh', [data]).result);
  expect(outcome).toMatchObject({ ok: true, value: { success: true, syncedCount: 2 } });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', executionEpoch: 2 });
  expect(await db.temuProductListCacheStaging.count()).toBe(0);
});

test('刷新开始后接收的实时数据优先于后台刷新结果', async () => {
  const data = await seed();
  const gate = deferred<any>();
  const remote: Remote = (method, filters) => {
    if (method === 'getProducts') {
      if (filters.page === 1) return page([product('fresh')], 2);
      return gate.promise;
    }
    return lifecycle(filters.productSkuIdList);
  };
  const refresh = run('processQueuedRefresh', [data], remote);
  await waitFor(async () => (await job()).syncedCount === 1);

  const realtime = await within(run('upsertProductsToCache', [
    userId,
    'shop-a',
    [product('keep', 'realtime')],
    { source: 'REALTIME_PAGE' },
  ]).result);
  expect(realtime).toMatchObject({ ok: true });

  gate.resolve(page([product('keep', 'background')], 2));
  const outcome = await within(refresh.result);
  expect(outcome).toMatchObject({ ok: true, value: { success: true } });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'realtime']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED' });
});

test('单个店铺的接口等待不会阻塞其他店铺刷新', async () => {
  const dataA = await seed('shop-a', 'job-a');
  const dataB = await seed('shop-b', 'job-b');
  const gate = deferred<any>();
  const blocked = run('processQueuedRefresh', [dataA], (method, filters) => {
    if (method === 'getProducts') return gate.promise;
    return lifecycle(filters.productSkuIdList);
  });
  await waitFor(async () => (await job('job-a')).status === 'PROCESSING');

  const outcomeB = await within(run('processQueuedRefresh', [dataB]).result);
  expect(outcomeB).toMatchObject({ ok: true, value: { success: true } });
  expect(await products('shop-b')).toEqual([['fresh', 'fresh'], ['keep', 'background']]);

  gate.resolve(page([], 0));
  const outcomeA = await within(blocked.result);
  expect(outcomeA).toMatchObject({ ok: true, value: { success: true } });
});

test('每轮刷新整体更新生命周期并保留未返回的条目', async () => {
  const data = await seed();
  const lifecycleCalls: any[] = [];
  const remote = pagedRemote(
    [product('fresh', 'fresh', 100, 1), product('keep', 'background', 100, 2)],
    (method, filters) => {
      lifecycleCalls.push(filters.productSkuIdList);
      return lifecycle([2], 11);
    },
  );
  const outcome = await within(run('processQueuedRefresh', [data], remote).result);
  expect(outcome).toMatchObject({ ok: true, value: { success: true } });
  expect(lifecycleCalls).toEqual([[1, 2]]);
  const rows = await lives();
  expect(rows.map((row) => [row.productSkcId, row.saleLifecycleStatus])).toEqual([
    ['s1', 'original'],
    ['s2', 'ON_SALE'],
  ]);
});

test('生命周期查询失败时保留旧值且不影响商品列表发布', async () => {
  const data = await seed();
  const remote = pagedRemote([product('fresh'), product('keep', 'background')], () => {
    throw new Error('TEMU timeout');
  });
  const outcome = await within(run('processQueuedRefresh', [data], remote).result);
  expect(outcome).toMatchObject({ ok: true, value: { success: true } });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED' });
  const rows = await lives();
  expect(rows.map((row) => [row.productSkcId, row.saleLifecycleStatus])).toEqual([['s1', 'original']]);
});

test('缺少数据列表的响应视为失败而不是空列表', async () => {
  const data = await seed();
  const outcome = await within(run('processQueuedRefresh', [data], () => ({ success: true })).result);
  expect(outcome.ok).toBe(false);
  expect(await products()).toEqual(ORIGINAL_PRODUCTS);
  expect(await job()).toMatchObject({ status: 'FAILED' });
});

test('声明总数无效时任务失败且缓存不变', async () => {
  const data = await seed();
  const remote: Remote = () => ({ success: true, result: { data: [product('fresh')] } });
  const outcome = await within(run('processQueuedRefresh', [data], remote).result);
  expect(outcome.ok).toBe(false);
  expect(await products()).toEqual(ORIGINAL_PRODUCTS);
  expect(await job()).toMatchObject({ status: 'FAILED' });
});

test('商品标识无效时任务失败且缓存不变', async () => {
  const data = await seed();
  const remote: Remote = () => page([{ productName: 'no-id', createdAt: 100 }], 1);
  const outcome = await within(run('processQueuedRefresh', [data], remote).result);
  expect(outcome.ok).toBe(false);
  expect(await products()).toEqual(ORIGINAL_PRODUCTS);
  expect(await job()).toMatchObject({ status: 'FAILED' });
});

test('增量刷新只统计新增商品', async () => {
  await seed();
  const data = await newJob('shop-a', 'job-inc', 'MANUAL_INCREMENTAL');
  const remote = pagedRemote([product('keep', 'renamed', 60), product('fresh', 'fresh', 70)]);
  const outcome = await within(run('processQueuedRefresh', [data], remote).result);
  expect(outcome).toMatchObject({ ok: true, value: { success: true, syncedCount: 1, totalCount: 1 } });
  expect(await job('job-inc')).toMatchObject({ status: 'COMPLETED', totalCount: 1, syncedCount: 1 });
  expect(await products()).toEqual([
    ['fresh', 'fresh'],
    ['keep', 'original-keep'],
    ['old', 'original-old'],
  ]);
});

test('重复触发刷新时复用进行中的任务，取消后可再次登记', async () => {
  await seed();
  const first = await within(run('queueRefresh', [userId, 'shop-a', 'MANUAL']).result);
  expect(first).toMatchObject({ ok: true, value: { status: 'PENDING' } });
  const second = await within(run('queueRefresh', [userId, 'shop-a', 'MANUAL']).result);
  expect(second).toMatchObject({ ok: true });
  expect(second.value.id).toBe(first.value.id);

  const cancelled = await within(run('cancelRefresh', [userId, 'shop-a']).result);
  expect(cancelled).toMatchObject({ ok: true, value: { status: 'CANCELLED' } });

  const third = await within(run('queueRefresh', [userId, 'shop-a', 'MANUAL']).result);
  expect(third).toMatchObject({ ok: true, value: { status: 'PENDING' } });
  expect(third.value.id).not.toBe(first.value.id);
});
