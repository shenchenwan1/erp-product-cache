const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on']);
const FALSE_VALUES = new Set(['0', 'false', 'no', 'off']);

export function isRuntimeFlagEnabled(name: string, defaultValue: boolean) {
  const rawValue = process.env[name];
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return defaultValue;
  }

  const normalized = String(rawValue).trim().toLowerCase();
  if (TRUE_VALUES.has(normalized)) return true;
  if (FALSE_VALUES.has(normalized)) return false;
  return defaultValue;
}

export function isBackgroundWorkerRuntimeEnabled() {
  return isRuntimeFlagEnabled('ERP_BACKGROUND_WORKERS_ENABLED', true);
}

export function isHttpServerRuntimeEnabled() {
  return isRuntimeFlagEnabled('ERP_HTTP_SERVER_ENABLED', true);
}
