export type ProductSaleLifecycleStatus = 'ON_SALE' | 'UNPUBLISHED' | 'OFFSHELF' | 'UNKNOWN';
export type ProductSaleLifecycleFilterStatus = Exclude<ProductSaleLifecycleStatus, 'UNKNOWN'>;

const FILTER_STATUSES = new Set<ProductSaleLifecycleFilterStatus>([
  'ON_SALE',
  'UNPUBLISHED',
  'OFFSHELF',
]);

export function mapSelectStatusToSaleLifecycleStatus(selectStatus: unknown): ProductSaleLifecycleStatus {
  const status = Number(selectStatus);
  if (status === 11 || status === 12) return 'ON_SALE';
  if (status === 7 || status === 9 || status === 10) return 'UNPUBLISHED';
  if (status === 13 || status === 17) return 'OFFSHELF';
  return 'UNKNOWN';
}

export function getSelectStatusesBySaleLifecycleStatus(status: ProductSaleLifecycleFilterStatus): number[] {
  if (status === 'ON_SALE') return [11, 12];
  if (status === 'UNPUBLISHED') return [7, 9, 10];
  if (status === 'OFFSHELF') return [13, 17];
  return [];
}

export function normalizeSaleLifecycleStatus(value?: string | null): ProductSaleLifecycleFilterStatus | null {
  if (!value) return null;
  const normalized = String(value).trim().toUpperCase() as ProductSaleLifecycleFilterStatus;
  return FILTER_STATUSES.has(normalized) ? normalized : null;
}
