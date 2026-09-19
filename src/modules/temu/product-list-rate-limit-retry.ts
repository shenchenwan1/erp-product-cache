import { ServiceUnavailableException } from '@nestjs/common';

export const PRODUCT_LIST_RATE_LIMIT_RETRY_MIN_DELAY_MS = 1000;
export const PRODUCT_LIST_RATE_LIMIT_RETRY_MAX_DELAY_MS = 30000;
export const PRODUCT_LIST_RATE_LIMIT_MAX_RETRIES = 6;
export const TEMU_RATE_LIMIT_RETRY_EXHAUSTED_MESSAGE = '平台接口繁忙，后台已自动等待重试但仍未恢复，请稍后刷新或重新触发';

type ProductListRateLimitRetryLogger = {
  warn?: (message: string) => void;
};

type ProductListRateLimitRetryOptions = {
  maxRetries?: number;
  minDelayMs?: number;
  maxDelayMs?: number;
  sleep?: (delayMs: number) => Promise<void>;
  logger?: ProductListRateLimitRetryLogger;
  context?: string;
};

const RATE_LIMIT_KEYWORDS = [
  '4000004',
  'requests too frequently',
  'exceeding the current limit threshold',
  '请求过于频繁',
  '限流阈值',
];

function defaultSleep(delayMs: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, delayMs));
}

function collectErrorText(error: unknown): string {
  const parts: string[] = [];
  if (typeof error === 'string') {
    parts.push(error);
  }

  const anyError = error as any;
  if (anyError?.message) {
    parts.push(String(anyError.message));
  }
  if (anyError?.errorCode) {
    parts.push(String(anyError.errorCode));
  }
  if (anyError?.code) {
    parts.push(String(anyError.code));
  }

  const response = typeof anyError?.getResponse === 'function'
    ? anyError.getResponse()
    : anyError?.response;
  if (typeof response === 'string') {
    parts.push(response);
  } else if (response && typeof response === 'object') {
    if (response.message) {
      parts.push(Array.isArray(response.message) ? response.message.join(' ') : String(response.message));
    }
    if (response.errorCode) {
      parts.push(String(response.errorCode));
    }
    if (response.code) {
      parts.push(String(response.code));
    }
  }

  return parts.join(' ').toLowerCase();
}

export function isTemuProductListRateLimitError(error: unknown) {
  const text = collectErrorText(error);
  return RATE_LIMIT_KEYWORDS.some((keyword) => text.includes(keyword.toLowerCase()));
}

function getRetryDelay(minDelayMs: number, maxDelayMs: number, retryCount: number) {
  const min = Math.max(0, Math.floor(minDelayMs));
  const max = Math.max(min, Math.floor(maxDelayMs));
  if (max === min) return min;

  const baseDelay = Math.min(max, min * (2 ** Math.max(0, retryCount)));
  const jitterMax = Math.min(max, Math.max(baseDelay, Math.floor(baseDelay * 1.5)));
  if (jitterMax === baseDelay) return baseDelay;
  return baseDelay + Math.floor(Math.random() * (jitterMax - baseDelay + 1));
}

function createRateLimitRetryExhaustedException() {
  return new ServiceUnavailableException({
    message: TEMU_RATE_LIMIT_RETRY_EXHAUSTED_MESSAGE,
    errorCode: 'TEMU_RATE_LIMIT_RETRY_EXHAUSTED',
    source: 'TEMU',
  });
}

export async function withProductListRateLimitRetry<T>(
  operation: () => Promise<T>,
  options: ProductListRateLimitRetryOptions = {},
): Promise<T> {
  const maxRetries = Math.max(0, Math.floor(options.maxRetries ?? PRODUCT_LIST_RATE_LIMIT_MAX_RETRIES));
  const minDelayMs = options.minDelayMs ?? PRODUCT_LIST_RATE_LIMIT_RETRY_MIN_DELAY_MS;
  const maxDelayMs = options.maxDelayMs ?? PRODUCT_LIST_RATE_LIMIT_RETRY_MAX_DELAY_MS;
  const sleep = options.sleep ?? defaultSleep;

  for (let retryCount = 0; ; retryCount += 1) {
    try {
      return await operation();
    } catch (error) {
      if (!isTemuProductListRateLimitError(error)) {
        throw error;
      }

      if (retryCount >= maxRetries) {
        options.logger?.warn?.(
          `[商品列表] TEMU 限流重试已耗尽${options.context ? ` (${options.context})` : ''}`,
        );
        throw createRateLimitRetryExhaustedException();
      }

      const delayMs = getRetryDelay(minDelayMs, maxDelayMs, retryCount);
      options.logger?.warn?.(
        `[商品列表] TEMU 限流，第 ${retryCount + 1}/${maxRetries} 次重试将在 ${delayMs}ms 后执行${options.context ? ` (${options.context})` : ''}`,
      );
      await sleep(delayMs);
    }
  }
}
