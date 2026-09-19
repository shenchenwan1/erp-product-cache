import {
  isBackgroundWorkerRuntimeEnabled,
  isHttpServerRuntimeEnabled,
  isRuntimeFlagEnabled,
} from './runtime-flags';

describe('runtime flags', () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
    delete process.env.TEST_RUNTIME_FLAG;
    delete process.env.ERP_BACKGROUND_WORKERS_ENABLED;
    delete process.env.ERP_HTTP_SERVER_ENABLED;
  });

  afterAll(() => {
    process.env = OLD_ENV;
  });

  it('uses the supplied default when a flag is not configured', () => {
    expect(isRuntimeFlagEnabled('TEST_RUNTIME_FLAG', true)).toBe(true);
    expect(isRuntimeFlagEnabled('TEST_RUNTIME_FLAG', false)).toBe(false);
  });

  it('treats common false values as disabled', () => {
    for (const value of ['0', 'false', 'no', 'off']) {
      process.env.TEST_RUNTIME_FLAG = value;
      expect(isRuntimeFlagEnabled('TEST_RUNTIME_FLAG', true)).toBe(false);
    }
  });

  it('treats common true values as enabled', () => {
    for (const value of ['1', 'true', 'yes', 'on']) {
      process.env.TEST_RUNTIME_FLAG = value;
      expect(isRuntimeFlagEnabled('TEST_RUNTIME_FLAG', false)).toBe(true);
    }
  });

  it('keeps background workers enabled by default', () => {
    expect(isBackgroundWorkerRuntimeEnabled()).toBe(true);
  });

  it('can disable background workers for web-only processes', () => {
    process.env.ERP_BACKGROUND_WORKERS_ENABLED = 'false';

    expect(isBackgroundWorkerRuntimeEnabled()).toBe(false);
  });

  it('keeps the HTTP server enabled by default', () => {
    expect(isHttpServerRuntimeEnabled()).toBe(true);
  });

  it('can disable the HTTP server for worker-only processes', () => {
    process.env.ERP_HTTP_SERVER_ENABLED = 'false';

    expect(isHttpServerRuntimeEnabled()).toBe(false);
  });
});
