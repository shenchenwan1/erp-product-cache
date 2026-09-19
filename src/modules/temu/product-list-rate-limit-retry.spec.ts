import { BadRequestException } from '@nestjs/common';
import {
  PRODUCT_LIST_RATE_LIMIT_MAX_RETRIES,
  PRODUCT_LIST_RATE_LIMIT_RETRY_MAX_DELAY_MS,
  TEMU_RATE_LIMIT_RETRY_EXHAUSTED_MESSAGE,
  isTemuProductListRateLimitError,
  withProductListRateLimitRetry,
} from './product-list-rate-limit-retry';

describe('product list TEMU rate limit retry', () => {
  it('uses a backend retry budget that can absorb short TEMU frequency windows', () => {
    expect(PRODUCT_LIST_RATE_LIMIT_MAX_RETRIES).toBeGreaterThanOrEqual(6);
    expect(PRODUCT_LIST_RATE_LIMIT_RETRY_MAX_DELAY_MS).toBeGreaterThanOrEqual(15000);
  });

  it('detects TEMU product list frequency limit errors from message and response payload', () => {
    expect(isTemuProductListRateLimitError(
      new Error('requests too frequently, exceeding the current limit threshold'),
    )).toBe(true);
    expect(isTemuProductListRateLimitError(
      new Error('请求过于频繁，已超过平台当前限流阈值，请稍后重试'),
    )).toBe(true);
    expect(isTemuProductListRateLimitError(
      new BadRequestException({
        message: '请求过于频繁，已超过平台当前限流阈值，请稍后重试',
        errorCode: '4000004',
        source: 'TEMU',
      }),
    )).toBe(true);
    expect(isTemuProductListRateLimitError(new Error('TEMU timeout'))).toBe(false);
  });

  it('retries transient TEMU frequency limit errors after a short delay', async () => {
    const sleep = jest.fn().mockResolvedValue(undefined);
    const operation = jest.fn()
      .mockRejectedValueOnce(new Error('requests too frequently, exceeding the current limit threshold'))
      .mockResolvedValueOnce({ success: true });

    await expect(withProductListRateLimitRetry(operation, {
      sleep,
      minDelayMs: 1000,
      maxDelayMs: 1000,
    })).resolves.toEqual({ success: true });

    expect(operation).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledWith(1000);
  });

  it('does not retry unrelated product list errors', async () => {
    const sleep = jest.fn().mockResolvedValue(undefined);
    const operation = jest.fn().mockRejectedValue(new Error('TEMU timeout'));

    await expect(withProductListRateLimitRetry(operation, {
      sleep,
      minDelayMs: 1000,
      maxDelayMs: 1000,
    })).rejects.toThrow('TEMU timeout');

    expect(operation).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it('throws an ERP-owned error instead of the TEMU rate-limit text after retry budget is exhausted', async () => {
    const sleep = jest.fn().mockResolvedValue(undefined);
    const operation = jest.fn().mockRejectedValue(
      new Error('requests too frequently, exceeding the current limit threshold'),
    );

    let caughtError: any;
    try {
      await withProductListRateLimitRetry(operation, {
        maxRetries: 2,
        sleep,
        minDelayMs: 1000,
        maxDelayMs: 1000,
      });
    } catch (error) {
      caughtError = error;
    }

    expect(caughtError?.getStatus()).toBe(503);
    expect(caughtError?.getResponse()).toEqual(expect.objectContaining({
      message: TEMU_RATE_LIMIT_RETRY_EXHAUSTED_MESSAGE,
      errorCode: 'TEMU_RATE_LIMIT_RETRY_EXHAUSTED',
      source: 'TEMU',
    }));
    expect(JSON.stringify(caughtError?.getResponse())).not.toContain('请求过于频繁');
    expect(JSON.stringify(caughtError?.getResponse())).not.toContain('requests too frequently');

    expect(operation).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenCalledTimes(2);
  });

  it('still allows callers to disable retries while keeping the platform rate-limit text internal', async () => {
    const operation = jest.fn().mockRejectedValue(
      new Error('requests too frequently, exceeding the current limit threshold'),
    );

    await expect(withProductListRateLimitRetry(operation, {
      maxRetries: 0,
      minDelayMs: 1000,
      maxDelayMs: 1000,
    })).rejects.toMatchObject({
      response: expect.objectContaining({
        message: TEMU_RATE_LIMIT_RETRY_EXHAUSTED_MESSAGE,
      }),
    });

    expect(operation).toHaveBeenCalledTimes(1);
  });
});
