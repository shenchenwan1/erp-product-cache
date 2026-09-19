export function getPositiveIntegerEnv(name: string, fallback: number, maxValue: number) {
  const parsed = Number(process.env[name]);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return Math.min(parsed, maxValue);
}
