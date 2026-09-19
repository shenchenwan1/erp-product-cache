import {
  getSelectStatusesBySaleLifecycleStatus,
  mapSelectStatusToSaleLifecycleStatus,
  normalizeSaleLifecycleStatus,
} from './product-lifecycle-status';

describe('product lifecycle status mapping', () => {
  it('maps verified TEMU selectStatus values to ERP sale lifecycle filters', () => {
    expect(mapSelectStatusToSaleLifecycleStatus(11)).toBe('ON_SALE');
    expect(mapSelectStatusToSaleLifecycleStatus(12)).toBe('ON_SALE');
    expect(mapSelectStatusToSaleLifecycleStatus(7)).toBe('UNPUBLISHED');
    expect(mapSelectStatusToSaleLifecycleStatus(9)).toBe('UNPUBLISHED');
    expect(mapSelectStatusToSaleLifecycleStatus(10)).toBe('UNPUBLISHED');
    expect(mapSelectStatusToSaleLifecycleStatus(13)).toBe('OFFSHELF');
    expect(mapSelectStatusToSaleLifecycleStatus(17)).toBe('OFFSHELF');
  });

  it('keeps unverified selectStatus values out of sale lifecycle filters', () => {
    expect(mapSelectStatusToSaleLifecycleStatus(99)).toBe('UNKNOWN');
    expect(mapSelectStatusToSaleLifecycleStatus(undefined)).toBe('UNKNOWN');
  });

  it('normalizes only supported sale lifecycle filter values', () => {
    expect(normalizeSaleLifecycleStatus('ON_SALE')).toBe('ON_SALE');
    expect(normalizeSaleLifecycleStatus('UNPUBLISHED')).toBe('UNPUBLISHED');
    expect(normalizeSaleLifecycleStatus('OFFSHELF')).toBe('OFFSHELF');
    expect(normalizeSaleLifecycleStatus('UNKNOWN')).toBeNull();
    expect(normalizeSaleLifecycleStatus('')).toBeNull();
  });

  it('exposes selectStatus fallbacks for stale lifecycle cache rows', () => {
    expect(getSelectStatusesBySaleLifecycleStatus('ON_SALE')).toEqual([11, 12]);
    expect(getSelectStatusesBySaleLifecycleStatus('UNPUBLISHED')).toEqual([7, 9, 10]);
    expect(getSelectStatusesBySaleLifecycleStatus('OFFSHELF')).toEqual([13, 17]);
  });
});
