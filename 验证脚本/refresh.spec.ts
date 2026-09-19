import { db, userId, within, run, stopWorkers, product, page, lifecycle, fixture, pauseSecondPage, seed, newJob, products, lives, job, reset } from './harness';

beforeEach(reset);
afterEach(stopWorkers);
afterAll(() => db.$disconnect());
const original = [['keep', 'original-keep'], ['old', 'original-old']];

test('完整刷新切换商品与生命周期，保留未返回的生命周期记录', async () => {
  const data = await seed();
  const worker = run('processQueuedRefresh', [data], fixture());
  expect(await within(worker.result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', syncedCount: 2, totalCount: 2 });
  expect((await lives())[0].selectStatus).toBe(4);
});

test('第二页尚未返回时，独立读连接仍看到上一次完整结果', async () => {
  const data = await seed();
  const oldLifecycle = await lives();
  const pause = pauseSecondPage();
  const worker = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached);
  const during = { products: await products(), lifecycle: await lives() };
  pause.release();
  await within(worker.result);
  expect(during).toEqual({ products: original, lifecycle: oldLifecycle });
});

test('后续页失败时保留完整旧结果', async () => {
  const data = await seed();
  const oldLifecycle = await lives();
  const pause = pauseSecondPage(undefined, 'upstream unavailable');
  const worker = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached); pause.release();
  expect(await within(worker.result)).toMatchObject({ ok: false });
  expect(await job()).toMatchObject({ status: 'FAILED' });
  expect(await products()).toEqual(original);
  expect(await lives()).toEqual(oldLifecycle);
});

test('取消不等待远端响应，取消后旧执行不能继续写数据或进度', async () => {
  const data = await seed();
  const pause = pauseSecondPage();
  const worker = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached);
  expect(await within(run('cancelRefresh', [userId, data.shopId]).result, 5000)).toMatchObject({ ok: true });
  const cancelled = await job();
  expect(cancelled.status).toBe('CANCELLED');
  pause.release(); await within(worker.result);
  expect(await products()).toEqual(original);
  expect(await job()).toEqual(cancelled);
});

test.each([undefined, 'late remote failure'])('取消后新任务完成，旧任务返回 %s 不改变新结果', async error => {
  const data = await seed();
  const pause = pauseSecondPage([product('stale-one'), product('stale-two')], error);
  const old = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached);
  await within(run('cancelRefresh', [userId, data.shopId]).result);
  const newer = await newJob(data.shopId, 'new-job');
  expect(await within(run('processQueuedRefresh', [newer]).result)).toMatchObject({ ok: true });
  const expected = { products: await products(), job: await job('new-job'), lifecycle: await lives() };
  pause.release(); await within(old.result);
  expect({ products: await products(), job: await job('new-job'), lifecycle: await lives() }).toEqual(expected);
});

test.each([undefined, 'late remote failure'])('同一任务重复投递，后启动执行完成后旧执行返回 %s 不得覆盖', async error => {
  const data = await seed();
  const pause = pauseSecondPage([product('stale-one'), product('stale-two')], error);
  const old = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached);
  expect(await within(run('processQueuedRefresh', [data]).result)).toMatchObject({ ok: true });
  const expected = { products: await products(), job: await job(), lifecycle: await lives() };
  expect(expected.products).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(expected.job.status).toBe('COMPLETED');
  pause.release(); await within(old.result);
  expect({ products: await products(), job: await job(), lifecycle: await lives() }).toEqual(expected);
});

test('执行进程中途被杀死，独立进程可重跑且无半成品可见', async () => {
  const data = await seed();
  const pause = pauseSecondPage([product('abandoned'), product('unused')]);
  const old = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached); await old.kill();
  const afterCrash = await products();
  expect(await within(run('processQueuedRefresh', [data]).result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(afterCrash).toEqual(original);
});

test('刷新期间的实时页面数据保留优先级，包括本次列表没有的新商品', async () => {
  const data = await seed();
  const pause = pauseSecondPage([product('keep', 'stale-background'), product('fresh')]);
  const worker = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached);
  expect(await within(run('upsertProductsToCache', [userId, data.shopId,
    [product('keep', 'realtime'), product('live', 'realtime-new')], { source: 'REALTIME_PAGE' }]).result)).toMatchObject({ ok: true });
  pause.release(); await within(worker.result);
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'realtime'], ['live', 'realtime-new']]);
  expect((await job()).syncedCount).toBe(2);
});

test('COMPLETED 写入被数据库拒绝时，商品替换和生命周期全部回滚', async () => {
  const data = await seed();
  const oldLifecycle = await lives();
  await db.$executeRawUnsafe(`CREATE OR REPLACE FUNCTION reject_complete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 'COMPLETED' THEN RAISE EXCEPTION 'terminal write rejected'; END IF; RETURN NEW; END $$`);
  await db.$executeRawUnsafe(`CREATE TRIGGER fail_complete BEFORE UPDATE ON temu_product_list_sync_jobs FOR EACH ROW EXECUTE FUNCTION reject_complete()`);
  try {
    expect(await within(run('processQueuedRefresh', [data]).result)).toMatchObject({ ok: false });
    expect(await products()).toEqual(original);
    expect(await lives()).toEqual(oldLifecycle);
    expect((await job()).status).toBe('FAILED');
  } finally {
    await db.$executeRawUnsafe('DROP TRIGGER IF EXISTS fail_complete ON temu_product_list_sync_jobs');
    await db.$executeRawUnsafe('DROP FUNCTION IF EXISTS reject_complete()');
  }
});

test('生命周期后批失败：商品可成功，整轮生命周期保留旧值', async () => {
  const data = await seed();
  const oldLifecycle = await lives();
  const worker = run('processQueuedRefresh', [data], (method, filters) => {
    if (method === 'getProducts') return page([product(`s${filters.page}`, 'new', 100, filters.page)], 2);
    if (filters.productSkuIdList.includes(2)) throw new Error('lifecycle unavailable');
    return lifecycle(filters.productSkuIdList, 4);
  });
  expect(await within(worker.result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([['s1', 'new'], ['s2', 'new']]);
  expect((await job()).status).toBe('COMPLETED');
  expect(await lives()).toEqual(oldLifecycle);
});

test('手动生命周期刷新跨批失败时不暴露第一批更新', async () => {
  await seed();
  const previous = await lives();
  const worker = run('refreshProductLifecycleStatusCache', [userId, 'shop-a', Array.from({ length: 101 }, (_, i) => i + 1)], (method, filters) => {
    if (filters.productSkuIdList.includes(101)) throw new Error('second batch failed');
    return lifecycle(filters.productSkuIdList, 4);
  });
  expect(await within(worker.result)).toMatchObject({ ok: false });
  expect(await lives()).toEqual(previous);
});

test('一个店铺的远端请求挂起不阻塞另一店铺完成', async () => {
  const first = await seed();
  const second = await seed('shop-b', 'job-b');
  const pause = pauseSecondPage();
  const worker = run('processQueuedRefresh', [first], pause.remote);
  await within(pause.reached);
  expect(await within(run('processQueuedRefresh', [second]).result, 5000)).toMatchObject({ ok: true });
  expect((await job('job-b')).status).toBe('COMPLETED');
  pause.release(); await within(worker.result);
});

test.each(['early-empty', 'missing-total', 'invalid-key'])('不完整列表 %s 不得作为完整快照发布', async variant => {
  const data = await seed();
  const worker = run('processQueuedRefresh', [data], (method, filters) => {
    if (method !== 'getProducts') return lifecycle(filters.productSkuIdList);
    if (variant === 'missing-total') return { result: { data: [product('fresh')] } };
    if (variant === 'invalid-key') return page([{ productId: 'p', createdAt: 1 }], 1);
    return filters.page === 1 ? page([product('fresh')], 2) : page([], 2);
  });
  expect(await within(worker.result)).toMatchObject({ ok: false });
  expect(await products()).toEqual(original);
  expect((await job()).status).toBe('FAILED');
});

test('明确的空列表可完成全量刷新', async () => {
  const data = await seed();
  expect(await within(run('processQueuedRefresh', [data], fixture([])).result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', totalCount: 0, syncedCount: 0 });
});

test('增量刷新以已缓存时间边界查询，已知条目只参与完整性计数且保留旧商品', async () => {
  const data = await seed('shop-a', 'job-a', 'MANUAL_INCREMENTAL');
  const seen: any[] = [];
  const worker = run('processQueuedRefresh', [data], (method, filters) => {
    if (method !== 'getProducts') return lifecycle(filters.productSkuIdList);
    seen.push(filters);
    return page(([product('fresh', 'fresh', 100), product('keep', 'should-not-replace', 50)]).slice(filters.page - 1, filters.page), 2);
  });
  expect(await within(worker.result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([['fresh', 'fresh'], ...original]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', totalCount: 1, syncedCount: 1 });
  expect(seen).toHaveLength(2);
  expect(seen.every(filters => filters.createdAtStart === 50)).toBe(true);
});

test.each([undefined, 'late failure'])('后执行仍在拉取时，前执行结束 %s 不得发布或清理后执行数据', async error => {
  const data = await seed();
  const a = pauseSecondPage([product('stale-one'), product('stale-two')], error);
  const b = pauseSecondPage();
  const old = run('processQueuedRefresh', [data], a.remote);
  await within(a.reached);
  const newer = run('processQueuedRefresh', [data], b.remote);
  await within(b.reached);
  a.release(); await within(old.result);
  const during = { products: await products(), status: (await job()).status };
  b.release();
  expect(await within(newer.result)).toMatchObject({ ok: true });
  expect(during).toEqual({ products: original, status: 'PROCESSING' });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
});

test('深分页回退使用包含边界的时间窗口，重复条目不重复计数', async () => {
  const data = await seed();
  const seen: any[] = [];
  const worker = run('processQueuedRefresh', [data], (method, filters) => {
    if (method !== 'getProducts') return lifecycle(filters.productSkuIdList);
    seen.push(filters);
    if (seen.length > 8) throw new Error('pagination did not converge');
    if (filters.createdAtEnd === undefined) {
      if (filters.page === 1) return page([product('first', 'first', 200)], 3);
      if (filters.page === 2) return page([product('boundary', 'boundary', 100)], 3);
      throw new Error('Pagination too deep');
    }
    if (filters.createdAtEnd !== 100) throw new Error('boundary skipped');
    return page([filters.page === 1 ? product('boundary', 'boundary', 100) : product('tail', 'tail', 90)], 2);
  });
  expect(await within(worker.result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([['boundary', 'boundary'], ['first', 'first'], ['tail', 'tail']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', totalCount: 3, syncedCount: 3 });
});

const sameClock = { CACHE_FIXED_NOW: '1780000000000' };
test('同时间戳的两次执行仍能区分归属', async () => {
  const data = await seed();
  const a = pauseSecondPage([product('older-one'), product('older-two')]);
  const b = pauseSecondPage();
  const old = run('processQueuedRefresh', [data], a.remote, sameClock);
  await within(a.reached);
  const newer = run('processQueuedRefresh', [data], b.remote, sameClock);
  await within(b.reached);
  a.release(); await within(old.result);
  const during = { products: await products(), status: (await job()).status };
  b.release();
  expect(await within(newer.result)).toMatchObject({ ok: true });
  expect(during).toEqual({ products: original, status: 'PROCESSING' });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
});

test('同时间戳下实时写入仍优先于已经开始的后台刷新', async () => {
  const data = await seed();
  const pause = pauseSecondPage([product('keep', 'stale'), product('fresh')]);
  const worker = run('processQueuedRefresh', [data], pause.remote, sameClock);
  await within(pause.reached);
  expect(await within(run('upsertProductsToCache', [userId, data.shopId,
    [product('keep', 'latest'), product('live', 'latest-new')], { source: 'REALTIME_PAGE' }], fixture(), sameClock).result)).toMatchObject({ ok: true });
  pause.release(); await within(worker.result);
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'latest'], ['live', 'latest-new']]);
});

test('无法推进的深分页窗口失败且保持旧快照', async () => {
  const data = await seed();
  let requests = 0;
  const worker = run('processQueuedRefresh', [data], (method, filters) => {
    if (method !== 'getProducts') return lifecycle(filters.productSkuIdList);
    if (++requests > 6) throw new Error('unbounded pagination');
    if (filters.page === 1) return page([product('boundary', 'boundary', 100)], 3);
    throw new Error('Pagination too deep');
  });
  expect(await within(worker.result)).toMatchObject({ ok: false });
  expect(await products()).toEqual(original);
  expect((await job()).status).toBe('FAILED');
});

test('崩溃后重跑回收被替代执行的临时正文', async () => {
  const data = await seed();
  const pause = pauseSecondPage([product('abandoned-body-unique'), product('unused')]);
  const old = run('processQueuedRefresh', [data], pause.remote);
  await within(pause.reached); await old.kill();
  expect(await within(run('processQueuedRefresh', [data]).result)).toMatchObject({ ok: true });
  const tables: any[] = await db.$queryRaw`SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename <> '_prisma_migrations'`;
  for (const { tablename } of tables) {
    const escaped = tablename.replace(/"/g, '""');
    const body: any[] = await db.$queryRawUnsafe(`SELECT row_to_json(t)::text AS body FROM "${escaped}" t`);
    expect(body.some(row => row.body.includes('abandoned-body-unique'))).toBe(false);
  }
});
