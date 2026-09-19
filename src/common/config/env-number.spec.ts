import { getPositiveIntegerEnv } from './env-number';

describe('getPositiveIntegerEnv', () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
    delete process.env.TEST_POSITIVE_INTEGER;
  });

  afterAll(() => {
    process.env = OLD_ENV;
  });

  it('returns the fallback when the value is missing or invalid', () => {
    expect(getPositiveIntegerEnv('TEST_POSITIVE_INTEGER', 3, 10)).toBe(3);

    process.env.TEST_POSITIVE_INTEGER = '0';
    expect(getPositiveIntegerEnv('TEST_POSITIVE_INTEGER', 3, 10)).toBe(3);

    process.env.TEST_POSITIVE_INTEGER = 'abc';
    expect(getPositiveIntegerEnv('TEST_POSITIVE_INTEGER', 3, 10)).toBe(3);
  });

  it('caps the configured value to the supplied maximum', () => {
    process.env.TEST_POSITIVE_INTEGER = '99';

    expect(getPositiveIntegerEnv('TEST_POSITIVE_INTEGER', 3, 10)).toBe(10);
  });

  it('returns the configured value when it is valid and within range', () => {
    process.env.TEST_POSITIVE_INTEGER = '4';

    expect(getPositiveIntegerEnv('TEST_POSITIVE_INTEGER', 3, 10)).toBe(4);
  });
});
