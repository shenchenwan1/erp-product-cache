import { db, within, run, stopWorkers, fixture, seed, products, job, reset } from './harness';
beforeEach(reset);
afterEach(stopWorkers);
afterAll(() => db.$disconnect());

test('完整商品列表正常刷新', async () => {
  const data = await seed();
  expect(await within(run('processQueuedRefresh', [data]).result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([['fresh', 'fresh'], ['keep', 'background']]);
  expect(await job()).toMatchObject({ status: 'COMPLETED', totalCount: 2, syncedCount: 2 });
});
test('平台明确返回空列表', async () => {
  const data = await seed();
  expect(await within(run('processQueuedRefresh', [data], fixture([])).result)).toMatchObject({ ok: true });
  expect(await products()).toEqual([]);
  expect((await job()).status).toBe('COMPLETED');
});
test('任务所有者不符时跳过', async () => {
  const data = await seed();
  expect(await within(run('processQueuedRefresh', [{ ...data, userId: 'another-owner' }]).result))
    .toMatchObject({ ok: true, value: { skipped: true } });
  expect((await job()).status).toBe('PENDING');
});
