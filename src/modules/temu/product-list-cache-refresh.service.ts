import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { Queue } from 'bull';
import { PrismaService } from '../../prisma/prisma.service';
import { getPositiveIntegerEnv } from '../../common/config/env-number';
import { isBackgroundWorkerRuntimeEnabled } from '../../common/config/runtime-flags';
import { AccountOwnerService } from '../subscription/account-owner.service';
import { SubscriptionCheckService } from '../subscription/subscription-check.service';
import { SubscriptionEntitlementService } from '../subscription/subscription-entitlement.service';
import { TemuService } from './temu.service';
import { withProductListRateLimitRetry } from './product-list-rate-limit-retry';
import { mapSelectStatusToSaleLifecycleStatus } from './product-lifecycle-status';

export const PRODUCT_LIST_CACHE_REFRESH_QUEUE_NAME = 'product-list-cache-refresh-queue';
export const PRODUCT_LIST_CACHE_REFRESH_JOB_NAME = 'refresh-products-cache';

const PRODUCT_LIST_CACHE_REFRESH_PAGE_SIZE = 200;
const PRODUCT_LIST_CACHE_UPSERT_BATCH_SIZE = 50;
const PRODUCT_LIFECYCLE_STATUS_QUERY_BATCH_SIZE = 100;
const DEFAULT_SCHEDULED_REFRESH_SHOP_PAGE_SIZE = 100;
const MAX_SCHEDULED_REFRESH_SHOP_PAGE_SIZE = 500;
const RUNNING_REFRESH_STATUSES = ['PENDING', 'PROCESSING'];
const CANCELLED_REFRESH_STATUS = 'CANCELLED';
const TERMINAL_QUEUE_STATES = new Set(['completed', 'failed']);
const REFRESH_QUEUE_JOB_ID_PREFIX = 'product-list-cache-refresh';

export type ProductListCacheRefreshTrigger = 'MANUAL' | 'MANUAL_INCREMENTAL' | 'AUTO_EMPTY' | 'SCHEDULED';

export type ProductListCacheRefreshJobData = {
  userId: string;
  shopId: string;
  jobId: string;
};

type ProductListCacheUpsertOptions = {
  syncedAt?: Date;
  syncJobId?: string | null;
  source?: 'REALTIME_PAGE' | 'BACKGROUND_REFRESH';
};

type ProductListRefreshShop = {
  id: string;
  userId: string;
  accessToken: string | null;
};

type QueuedRefreshOwner = {
  syncJobId: string;
  syncJob: any | null;
};

type ProductLifecycleStatusCacheRow = {
  shopId: string;
  productId: string;
  productSkcId: string;
  selectStatus: number | null;
  saleLifecycleStatus: string;
  skuIds: number[];
  rawData: Record<string, any>;
  syncedAt: Date;
};

type ProductListRefreshMode = 'FULL' | 'INCREMENTAL';

type ProductListRefreshScope = {
  mode: ProductListRefreshMode;
  createdAtStart?: number;
  knownProductSkcIds: Set<string>;
};

export type ProductLifecycleStatusCacheRefreshResult = {
  success: true;
  result: {
    requestedSkuCount: number;
    syncedCount: number;
    dataList: any[];
  };
};

@Injectable()
export class ProductListCacheRefreshService {
  private readonly logger = new Logger(ProductListCacheRefreshService.name);

  constructor(
    @InjectQueue(PRODUCT_LIST_CACHE_REFRESH_QUEUE_NAME)
    private readonly refreshQueue: Queue<ProductListCacheRefreshJobData>,
    private readonly prisma: PrismaService,
    private readonly temuService: TemuService,
    private readonly accountOwnerService: AccountOwnerService,
    private readonly subscriptionCheckService: SubscriptionCheckService,
    private readonly subscriptionEntitlementService?: SubscriptionEntitlementService,
  ) {}

  /**
   * 每天凌晨 3 点刷新所有 ACTIVE 店铺的商品列表缓存。
   * 刷新本身仍由队列处理器执行，Cron 只负责扫描店铺并投递任务。
   */
  @Cron('0 3 * * *', { timeZone: 'Asia/Shanghai' })
  async handleScheduledProductListRefresh() {
    if (!isBackgroundWorkerRuntimeEnabled()) {
      return;
    }
    if (this.getBooleanEnv('TEMU_PRODUCT_LIST_CACHE_REFRESH_CRON_DISABLED')) {
      this.logger.warn('[商品列表缓存] 定时刷新已禁用，跳过本轮');
      return;
    }

    const pageSize = getPositiveIntegerEnv(
      'TEMU_PRODUCT_LIST_CACHE_REFRESH_CRON_SHOP_PAGE_SIZE',
      DEFAULT_SCHEDULED_REFRESH_SHOP_PAGE_SIZE,
      MAX_SCHEDULED_REFRESH_SHOP_PAGE_SIZE,
    );
    let cursorId: string | undefined;
    let scannedShops = 0;
    let queuedJobs = 0;
    let skippedJobs = 0;
    let failedJobs = 0;

    this.logger.log('[商品列表缓存] 开始定时刷新扫描');
    while (true) {
      const shops: ProductListRefreshShop[] = await this.prisma.shop.findMany({
        where: { status: 'ACTIVE' },
        select: { id: true, userId: true, accessToken: true },
        orderBy: { id: 'asc' },
        take: pageSize,
        ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
      });

      if (shops.length === 0) {
        break;
      }

      scannedShops += shops.length;
      for (const shop of shops) {
        if (!shop.accessToken) {
          skippedJobs++;
          continue;
        }

        try {
          const sync = await this.queueRefresh(shop.userId, shop.id, 'SCHEDULED');
          if (sync?.status === 'PENDING') {
            queuedJobs++;
          } else {
            skippedJobs++;
          }
        } catch (error: any) {
          failedJobs++;
          this.logger.error(
            `[商品列表缓存] 定时刷新入队失败 shopId=${shop.id}: ${this.getErrorMessage(error)}`,
          );
        }
      }

      if (shops.length < pageSize) {
        break;
      }
      cursorId = shops[shops.length - 1].id;
    }

    this.logger.log(
      `[商品列表缓存] 定时刷新扫描完成 scannedShops=${scannedShops}, queuedJobs=${queuedJobs}, skippedJobs=${skippedJobs}, failedJobs=${failedJobs}`,
    );
  }

  async queueRefresh(userId: string, shopId: string, triggerType: ProductListCacheRefreshTrigger = 'MANUAL') {
    if (!shopId) throw new Error('缺少店铺ID');
    await this.getAccessibleShopToken(userId, shopId);

    const runningJob = await this.findRunningRefresh(userId, shopId);
    if (runningJob) return this.serializeJob(runningJob);

    const queueJobId = this.buildRefreshQueueJobId(shopId);
    const existingQueuedJob = await this.findExistingQueuedRefresh(queueJobId);
    if (existingQueuedJob) return this.serializeJob(existingQueuedJob);

    const effectiveTriggerType = await this.resolveQueuedRefreshTriggerType(shopId, triggerType);
    const job = await (this.prisma as any).temuProductListSyncJob.create({
      data: {
        userId,
        shopId,
        status: 'PENDING',
        triggerType: effectiveTriggerType,
        pageSize: PRODUCT_LIST_CACHE_REFRESH_PAGE_SIZE,
        totalCount: 0,
        syncedCount: 0,
        currentPage: 0,
        errorMessage: null,
        startedAt: null,
        finishedAt: null,
      },
    });

    try {
      await this.refreshQueue.add(
        PRODUCT_LIST_CACHE_REFRESH_JOB_NAME,
        { userId, shopId, jobId: job.id },
        {
          jobId: queueJobId,
          attempts: 1,
          removeOnComplete: 100,
          removeOnFail: false,
        },
      );
      const persistedQueuedOwner = await this.findQueuedRefreshOwner(queueJobId);
      if (persistedQueuedOwner) {
        if (persistedQueuedOwner.syncJobId !== job.id) {
          const duplicateJob = await this.markRefreshJobAsDuplicate(job.id, persistedQueuedOwner.syncJobId);
          return this.serializeJob(persistedQueuedOwner.syncJob || duplicateJob);
        }
        if (persistedQueuedOwner.syncJob) {
          return this.serializeJob(persistedQueuedOwner.syncJob);
        }
      }
    } catch (error: any) {
      const existingQueuedJob = await this.findExistingQueuedRefresh(queueJobId).catch(() => null);
      if (existingQueuedJob) {
        await this.markRefreshJobAsDuplicate(job.id, existingQueuedJob.id);
        return this.serializeJob(existingQueuedJob);
      }

      await (this.prisma as any).temuProductListSyncJob.update({
        where: { id: job.id },
        data: {
          status: 'FAILED',
          errorMessage: `商品缓存刷新任务入队失败: ${this.getErrorMessage(error)}`,
          finishedAt: new Date(),
        },
      });
      throw error;
    }

    return this.serializeJob(job);
  }

  async findRunningRefresh(_userId: string, shopId: string) {
    const runningJob = await (this.prisma as any).temuProductListSyncJob.findFirst({
      where: {
        shopId,
        status: { in: RUNNING_REFRESH_STATUSES },
      },
      orderBy: [{ updatedAt: 'desc' }, { createdAt: 'desc' }],
    });
    if (!runningJob) return null;

    return this.reconcileRunningRefreshQueueState(runningJob);
  }

  async findLatestRefresh(_userId: string, shopId: string) {
    return (this.prisma as any).temuProductListSyncJob.findFirst({
      where: { shopId },
      orderBy: [{ updatedAt: 'desc' }, { createdAt: 'desc' }],
    });
  }

  async cancelRefresh(userId: string, shopId: string) {
    if (!shopId) throw new Error('缺少店铺ID');
    await this.getAccessibleShopToken(userId, shopId);

    const runningJob = await (this.prisma as any).temuProductListSyncJob.findFirst({
      where: {
        shopId,
        status: { in: RUNNING_REFRESH_STATUSES },
      },
      orderBy: [{ updatedAt: 'desc' }, { createdAt: 'desc' }],
    });
    if (!runningJob) return null;

    const queueJob = await this.refreshQueue.getJob(this.buildRefreshQueueJobId(shopId)).catch(() => null);
    if (queueJob) {
      const state = await queueJob.getState().catch(() => null);
      if (state && state !== 'active') {
        await queueJob.remove().catch((error: any) => {
          this.logger.warn(`[商品列表缓存] 移除待停止队列任务失败 shopId=${shopId} jobId=${runningJob.id}: ${this.getErrorMessage(error)}`);
        });
      }
    }

    const cancelledAt = new Date();
    const updateResult = await (this.prisma as any).temuProductListSyncJob.updateMany({
      where: {
        id: runningJob.id,
        shopId,
        status: { in: RUNNING_REFRESH_STATUSES },
      },
      data: {
        status: CANCELLED_REFRESH_STATUS,
        errorMessage: null,
        finishedAt: cancelledAt,
      },
    });

    if (Number(updateResult?.count || 0) === 0) {
      const latestJob = await this.findLatestRefresh(userId, shopId);
      return this.serializeJob(latestJob);
    }

    return this.serializeJob({
      ...runningJob,
      status: CANCELLED_REFRESH_STATUS,
      errorMessage: null,
      finishedAt: cancelledAt,
    });
  }

  serializeJob(job: any) {
    if (!job) return null;
    const totalCount = Number(job.totalCount || 0);
    const syncedCount = Number(job.syncedCount || 0);
    const percent = totalCount > 0 ? Math.min(100, Math.round((syncedCount / totalCount) * 100)) : 0;
    const summary = this.getRefreshJobSummary(job.status, job.triggerType, totalCount, syncedCount);
    return {
      id: job.id,
      status: job.status,
      triggerType: job.triggerType,
      totalCount,
      syncedCount,
      pageSize: Number(job.pageSize || PRODUCT_LIST_CACHE_REFRESH_PAGE_SIZE),
      currentPage: Number(job.currentPage || 0),
      percent,
      summary,
      errorMessage: job.errorMessage || null,
      startedAt: this.toIsoString(job.startedAt),
      finishedAt: this.toIsoString(job.finishedAt),
      createdAt: this.toIsoString(job.createdAt),
      updatedAt: this.toIsoString(job.updatedAt),
    };
  }

  async processQueuedRefresh(data: ProductListCacheRefreshJobData) {
    const job = await (this.prisma as any).temuProductListSyncJob.findUnique({
      where: { id: data.jobId },
    });
    if (!job || job.userId !== data.userId || job.shopId !== data.shopId) {
      return { success: true, skipped: true };
    }
    if (job.status === CANCELLED_REFRESH_STATUS) {
      await this.markRefreshJobCancelledProgress(data.jobId, job.totalCount || 0, job.syncedCount || 0, job.currentPage || 0);
      return { success: true, cancelled: true, totalCount: job.totalCount || 0, syncedCount: job.syncedCount || 0 };
    }
    if (!RUNNING_REFRESH_STATUSES.includes(job.status)) {
      return { success: true, skipped: true };
    }

    const startedAt = new Date();
    const pageSize = Number(job.pageSize || PRODUCT_LIST_CACHE_REFRESH_PAGE_SIZE);
    let totalCount = 0;
    let syncedCount = 0;
    let currentPage = 0;
    let requestPage = 1;
    let createdAtStart: number | undefined;
    let createdAtEnd: number | undefined;
    let lastFetchedCreatedAt: number | undefined;
    let refreshScope: ProductListRefreshScope = { mode: 'FULL', knownProductSkcIds: new Set<string>() };

    try {
      const token = await this.getAccessibleShopToken(data.userId, data.shopId);
      refreshScope = await this.resolveProductListRefreshScope(data.shopId, job.triggerType);
      createdAtStart = refreshScope.createdAtStart;
      const syncedProductSkcIds = new Set(refreshScope.knownProductSkcIds);
      const startedJob = await this.updateRefreshJobIfNotCancelled(data.jobId, {
        status: 'PROCESSING',
        startedAt,
        finishedAt: null,
        errorMessage: null,
        totalCount: 0,
        syncedCount: 0,
        currentPage: 0,
      });
      if (!startedJob) {
        await this.markRefreshJobCancelledProgress(data.jobId, totalCount, syncedCount, currentPage);
        return { success: true, cancelled: true, totalCount, syncedCount };
      }

      while (true) {
        if (await this.isRefreshJobCancelled(data.jobId)) {
          await this.markRefreshJobCancelledProgress(data.jobId, totalCount, syncedCount, currentPage);
          return { success: true, cancelled: true, totalCount, syncedCount };
        }

        let response: any;
        const requestFilters: Record<string, any> = { page: requestPage, pageSize };
        if (createdAtStart !== undefined) requestFilters.createdAtStart = createdAtStart;
        if (createdAtEnd !== undefined) requestFilters.createdAtEnd = createdAtEnd;
        try {
          response = await withProductListRateLimitRetry(
            () => this.temuService.getProducts(token, requestFilters),
            {
              logger: this.logger,
              context: `shopId=${data.shopId} page=${requestPage}`,
            },
          );
        } catch (error: any) {
          if (this.isProductListDeepPaginationError(error) && lastFetchedCreatedAt !== undefined) {
            if (createdAtEnd !== undefined && lastFetchedCreatedAt >= createdAtEnd) {
              throw error;
            }
            createdAtEnd = lastFetchedCreatedAt;
            requestPage = 1;
            this.logger.warn(
              `[商品列表缓存] 触发深分页限制，按创建时间边界继续 shopId=${data.shopId} jobId=${data.jobId} createdAtEnd=${createdAtEnd}`,
            );
            continue;
          }
          throw error;
        }
        const products = this.getProductListData(response);
        const responseTotalCount = this.getProductListTotalCount(response);
        totalCount = Math.max(totalCount, responseTotalCount, syncedCount);
        if (products.length === 0) break;
        lastFetchedCreatedAt = this.getProductCreatedAtBoundary(products);
        const uniqueProducts = products.filter((product: any) => {
          const productSkcId = this.getProductSkcId(product);
          if (!productSkcId) return true;
          if (syncedProductSkcIds.has(productSkcId)) return false;
          syncedProductSkcIds.add(productSkcId);
          return true;
        });
        totalCount = Math.max(totalCount, responseTotalCount, syncedCount + uniqueProducts.length);

        if (uniqueProducts.length > 0) {
          await this.upsertProductsToCache(data.userId, data.shopId, uniqueProducts, {
            syncedAt: startedAt,
            syncJobId: data.jobId,
            source: 'BACKGROUND_REFRESH',
          });
          await this.upsertProductLifecycleStatusesToCache(token, data.shopId, uniqueProducts, startedAt).catch((error) => {
            this.logger.warn(
              `[商品生命周期缓存] 写入失败 shopId=${data.shopId} jobId=${data.jobId}: ${this.getErrorMessage(error)}`,
            );
          });
        }

        syncedCount += uniqueProducts.length;
        currentPage += 1;
        const progressJob = await this.updateRefreshJobIfNotCancelled(data.jobId, {
          status: 'PROCESSING',
          totalCount,
          syncedCount,
          currentPage,
        });

        if (!progressJob || await this.isRefreshJobCancelled(data.jobId)) {
          await this.markRefreshJobCancelledProgress(data.jobId, totalCount, syncedCount, currentPage);
          return { success: true, cancelled: true, totalCount, syncedCount };
        }

        if (totalCount <= syncedCount) break;
        requestPage += 1;
      }

      if (await this.isRefreshJobCancelled(data.jobId)) {
        await this.markRefreshJobCancelledProgress(data.jobId, totalCount, syncedCount, currentPage);
        return { success: true, cancelled: true, totalCount, syncedCount };
      }

      if (refreshScope.mode === 'FULL') {
        await (this.prisma as any).temuProductListCache.updateMany({
          where: {
            shopId: data.shopId,
            isActive: true,
            OR: [{ syncJobId: null }, { syncJobId: { not: data.jobId } }],
          },
          data: { isActive: false },
        });
      }

      const completedTotalCount = refreshScope.mode === 'INCREMENTAL' ? syncedCount : totalCount;
      const completedJob = await this.updateRefreshJobIfNotCancelled(data.jobId, {
        status: 'COMPLETED',
        totalCount: completedTotalCount,
        syncedCount,
        currentPage,
        finishedAt: new Date(),
        errorMessage: null,
      });
      if (!completedJob && await this.isRefreshJobCancelled(data.jobId)) {
        await this.markRefreshJobCancelledProgress(data.jobId, totalCount, syncedCount, currentPage);
        return { success: true, cancelled: true, totalCount, syncedCount };
      }

      return { success: true, totalCount: completedTotalCount, syncedCount };
    } catch (error: any) {
      const message = this.getErrorMessage(error);
      if (await this.isRefreshJobCancelled(data.jobId)) {
        await this.markRefreshJobCancelledProgress(data.jobId, totalCount, syncedCount, currentPage);
        return { success: true, cancelled: true, totalCount, syncedCount };
      }
      await this.updateRefreshJobIfNotCancelled(data.jobId, {
        status: 'FAILED',
        totalCount,
        syncedCount,
        currentPage,
        errorMessage: message,
        finishedAt: new Date(),
      });
      this.logger.warn(`[商品列表缓存] 后台同步失败 shopId=${data.shopId} jobId=${data.jobId}: ${message}`);
      throw error;
    }
  }

  async markQueuedRefreshFailed(data: Partial<ProductListCacheRefreshJobData> | undefined, message: string) {
    if (!data?.jobId) return;
    await this.markRefreshJobFailed(data.jobId, message);
  }

  private async resolveProductListRefreshScope(shopId: string, triggerType?: string): Promise<ProductListRefreshScope> {
    if (triggerType === 'SCHEDULED') {
      return { mode: 'FULL', knownProductSkcIds: new Set<string>() };
    }

    const summary = await (this.prisma as any).temuProductListCache.aggregate({
      where: { shopId, isActive: true },
      _count: { _all: true },
      _max: { createdAtTs: true },
    });
    const activeCount = Number(summary?._count?._all || 0);
    const createdAtStart = this.toOptionalTimestampNumber(summary?._max?.createdAtTs);
    if (activeCount <= 0 || createdAtStart === undefined) {
      return { mode: 'FULL', knownProductSkcIds: new Set<string>() };
    }

    const boundaryRows = await (this.prisma as any).temuProductListCache.findMany({
      where: {
        shopId,
        isActive: true,
        createdAtTs: { gte: BigInt(createdAtStart) },
      },
      select: { productSkcId: true },
    });
    return {
      mode: 'INCREMENTAL',
      createdAtStart,
      knownProductSkcIds: new Set(
        (boundaryRows || [])
          .map((row: any) => this.getProductSkcId(row))
          .filter(Boolean) as string[],
      ),
    };
  }

  private async resolveQueuedRefreshTriggerType(
    shopId: string,
    triggerType: ProductListCacheRefreshTrigger,
  ): Promise<ProductListCacheRefreshTrigger> {
    if (triggerType !== 'MANUAL') return triggerType;
    const summary = await (this.prisma as any).temuProductListCache.aggregate({
      where: { shopId, isActive: true },
      _count: { _all: true },
      _max: { createdAtTs: true },
    });
    const activeCount = Number(summary?._count?._all || 0);
    const createdAtStart = this.toOptionalTimestampNumber(summary?._max?.createdAtTs);
    return activeCount > 0 && createdAtStart !== undefined ? 'MANUAL_INCREMENTAL' : 'MANUAL';
  }

  private getRefreshJobSummary(
    status: string | undefined,
    triggerType: string | undefined,
    totalCount: number,
    syncedCount: number,
  ) {
    if (triggerType === 'MANUAL_INCREMENTAL') {
      if (RUNNING_REFRESH_STATUSES.includes(String(status || ''))) {
        return totalCount > 0 ? `增量同步中 ${syncedCount}/${totalCount}` : '增量同步中';
      }
      if (status === CANCELLED_REFRESH_STATUS) return '已停止同步';
      return syncedCount > 0 ? `增量同步完成，新增 ${syncedCount} 个商品` : '增量检查完成，无新增商品';
    }
    if (status === CANCELLED_REFRESH_STATUS) return '已停止同步';
    return totalCount > 0 ? `已同步 ${syncedCount}/${totalCount}` : `已同步 ${syncedCount}`;
  }

  async upsertProductsToCache(
    userId: string,
    shopId: string,
    products: any[],
    options: ProductListCacheUpsertOptions = {},
  ) {
    const syncedAt = options.syncedAt || new Date();
    const rows = products
      .map((product) => this.buildProductCachePayload(userId, shopId, product, syncedAt, options.syncJobId ?? null))
      .filter(Boolean) as any[];
    if (rows.length === 0) return;

    for (const chunk of this.chunkArray(rows, PRODUCT_LIST_CACHE_UPSERT_BATCH_SIZE)) {
      await Promise.all(chunk.map((row) => (this.prisma as any).temuProductListCache.upsert({
        where: {
          shopId_productSkcId: {
            shopId,
            productSkcId: row.productSkcId,
          },
        },
        create: row,
        update: {
          ...row,
          userId,
          shopId,
          productId: row.productId,
          productSkcId: row.productSkcId,
        },
      })));
    }
  }

  async refreshProductLifecycleStatusCache(
    userId: string,
    shopId: string,
    productSkuIds: Array<number | string>,
  ): Promise<ProductLifecycleStatusCacheRefreshResult> {
    const token = await this.getAccessibleShopToken(userId, shopId);
    const result = await this.upsertProductLifecycleStatusesBySkuIdsToCache(
      token,
      shopId,
      productSkuIds,
      new Date(),
    );
    return { success: true, result };
  }

  async upsertProductLifecycleStatusResponseToCache(
    shopId: string,
    response: any,
    syncedAt: Date = new Date(),
  ) {
    const rows: ProductLifecycleStatusCacheRow[] = this.buildLifecycleStatusRows(shopId, response, syncedAt);
    return { syncedCount: await this.upsertProductLifecycleStatusRows(rows) };
  }

  private buildProductCachePayload(
    userId: string,
    shopId: string,
    product: any,
    syncedAt: Date,
    syncJobId: string | null,
  ) {
    const productId = product?.productId === undefined || product?.productId === null ? '' : String(product.productId);
    const productSkcId = product?.productSkcId === undefined || product?.productSkcId === null ? '' : String(product.productSkcId);
    if (!productId || !productSkcId) return null;
    const bindSites = this.extractProductBindSites(product);
    return {
      userId,
      shopId,
      syncJobId,
      productId,
      productSkcId,
      productName: product?.productName ? String(product.productName) : null,
      mainImageUrl: product?.mainImageUrl ? String(product.mainImageUrl) : null,
      extCode: product?.extCode ? String(product.extCode) : null,
      createdAtTs: this.toOptionalBigInt(product?.createdAt),
      skcSiteStatus: this.toOptionalInteger(product?.skcSiteStatus),
      bindSiteIds: bindSites.siteIds,
      bindSiteNames: bindSites.siteNames,
      isSupportPersonalization: product?.isSupportPersonalization === undefined ? null : Boolean(product.isSupportPersonalization),
      matchSkcJitMode: product?.matchSkcJitMode === undefined ? null : Boolean(product.matchSkcJitMode),
      cat1Id: this.getProductCategoryId(product, 'cat1'),
      cat2Id: this.getProductCategoryId(product, 'cat2'),
      cat3Id: this.getProductCategoryId(product, 'cat3'),
      cat4Id: this.getProductCategoryId(product, 'cat4'),
      cat5Id: this.getProductCategoryId(product, 'cat5'),
      cat6Id: this.getProductCategoryId(product, 'cat6'),
      cat7Id: this.getProductCategoryId(product, 'cat7'),
      cat8Id: this.getProductCategoryId(product, 'cat8'),
      cat9Id: this.getProductCategoryId(product, 'cat9'),
      cat10Id: this.getProductCategoryId(product, 'cat10'),
      leafCatId: this.toOptionalInteger(product?.categories?.leafCat?.catId ?? product?.leafCat?.catId),
      rawData: product,
      syncedAt,
      isActive: true,
    };
  }

  private extractProductBindSites(product: any) {
    const sourceSites = Array.isArray(product?.productSemiManaged?.bindSites)
      ? product.productSemiManaged.bindSites
      : Array.isArray(product?.bindSites)
        ? product.bindSites
        : [];
    const siteIds: number[] = [];
    const siteNames: string[] = [];
    const seen = new Set<number>();

    sourceSites.forEach((site: any) => {
      const siteId = this.toOptionalInteger(site?.siteId);
      if (siteId === undefined || siteId <= 0 || seen.has(siteId)) return;
      seen.add(siteId);
      siteIds.push(siteId);
      siteNames.push(site?.siteName === undefined || site?.siteName === null ? '' : String(site.siteName).trim());
    });

    return { siteIds, siteNames };
  }

  private async upsertProductLifecycleStatusesToCache(
    accessToken: string,
    shopId: string,
    products: any[],
    syncedAt: Date,
  ) {
    const productSkuIds = this.getProductSkuIds(products);
    return this.upsertProductLifecycleStatusesBySkuIdsToCache(accessToken, shopId, productSkuIds, syncedAt);
  }

  private async upsertProductLifecycleStatusesBySkuIdsToCache(
    accessToken: string,
    shopId: string,
    productSkuIds: Array<number | string>,
    syncedAt: Date,
  ) {
    const normalizedSkuIds = this.normalizeProductSkuIds(productSkuIds);
    const dataList: any[] = [];
    let syncedCount = 0;
    if (normalizedSkuIds.length === 0) {
      return { requestedSkuCount: 0, syncedCount, dataList };
    }

    for (const productSkuIdList of this.chunkArray(normalizedSkuIds, PRODUCT_LIFECYCLE_STATUS_QUERY_BATCH_SIZE)) {
      const response = await withProductListRateLimitRetry(
        () => this.temuService.searchProductLifecycle(accessToken, {
          pageNum: 1,
          pageSize: PRODUCT_LIFECYCLE_STATUS_QUERY_BATCH_SIZE,
          productSkuIdList,
        }),
        {
          logger: this.logger,
          context: `lifecycle shopId=${shopId} skuCount=${productSkuIdList.length}`,
        },
      );
      const responseDataList = Array.isArray(response?.result?.dataList) ? response.result.dataList : [];
      dataList.push(...responseDataList);
      const rows: ProductLifecycleStatusCacheRow[] = this.buildLifecycleStatusRows(shopId, response, syncedAt);
      if (rows.length === 0) continue;
      syncedCount += await this.upsertProductLifecycleStatusRows(rows);
    }
    return { requestedSkuCount: normalizedSkuIds.length, syncedCount, dataList };
  }

  private async upsertProductLifecycleStatusRows(rows: ProductLifecycleStatusCacheRow[]) {
    let syncedCount = 0;
    for (const chunk of this.chunkArray(rows, PRODUCT_LIST_CACHE_UPSERT_BATCH_SIZE)) {
      await Promise.all(chunk.map((row) => (this.prisma as any).temuProductLifecycleStatusCache.upsert({
        where: {
          shopId_productSkcId: {
            shopId: row.shopId,
            productSkcId: row.productSkcId,
          },
        },
        create: row,
        update: {
          ...row,
          shopId: row.shopId,
          productSkcId: row.productSkcId,
        },
      })));
      syncedCount += chunk.length;
    }
    return syncedCount;
  }

  private getProductSkuIds(products: any[]) {
    const ids = new Set<number>();
    products.forEach((product) => {
      const skuSummaries = Array.isArray(product?.productSkuSummaries) ? product.productSkuSummaries : [];
      skuSummaries.forEach((sku: any) => {
        const skuId = this.toOptionalInteger(sku?.productSkuId);
        if (skuId !== undefined) ids.add(skuId);
      });
    });
    return Array.from(ids);
  }

  private normalizeProductSkuIds(productSkuIds: Array<number | string>) {
    const ids = new Set<number>();
    productSkuIds.forEach((productSkuId) => {
      const numericId = this.toOptionalInteger(productSkuId);
      if (numericId !== undefined && numericId > 0) ids.add(numericId);
    });
    return Array.from(ids);
  }

  private buildLifecycleStatusRows(shopId: string, response: any, syncedAt: Date) {
    const dataList = Array.isArray(response?.result?.dataList) ? response.result.dataList : [];
    return dataList.flatMap((item: any) => {
      const productId = item?.productId === undefined || item?.productId === null ? '' : String(item.productId);
      const skcList = Array.isArray(item?.skcList) ? item.skcList : [];
      return skcList.map((skc: any) => {
        const productSkcId = skc?.skcId === undefined || skc?.skcId === null ? '' : String(skc.skcId);
        if (!productId || !productSkcId) return null;
        const selectStatus = this.toOptionalInteger(skc?.selectStatus);
        const skuList = Array.isArray(skc?.skuList) ? skc.skuList : [];
        const skuIds = skuList
          .map((sku: any) => this.toOptionalInteger(sku?.skuId ?? sku?.productSkuId))
          .filter((value: number | undefined): value is number => value !== undefined);
        return {
          shopId,
          productId,
          productSkcId,
          selectStatus: selectStatus ?? null,
          saleLifecycleStatus: mapSelectStatusToSaleLifecycleStatus(selectStatus),
          skuIds,
          rawData: { productId, ...skc },
          syncedAt,
        };
      }).filter(Boolean);
    });
  }

  private async getAccessibleShopToken(userId: string, shopId: string): Promise<string> {
    const accountOwnerUserId = await this.accountOwnerService.resolveOwnerId(userId);
    await this.subscriptionCheckService.checkSubscription(accountOwnerUserId);
    await this.subscriptionEntitlementService?.assertShopUsable(userId, shopId, accountOwnerUserId);

    const parentUserId = await this.getParentUserId(userId);
    let shop: any = null;
    if (parentUserId) {
      shop = await this.prisma.shop.findFirst({
        where: { id: shopId, createdByUserId: userId, status: 'ACTIVE' },
        select: { accessToken: true },
      });
      if (!shop) {
        const authorization = await (this.prisma as any).shopAuthorization.findFirst({
          where: { shopId, userId, shop: { status: 'ACTIVE' } },
        });
        if (authorization) {
          shop = await this.prisma.shop.findFirst({
            where: { id: shopId, status: 'ACTIVE' },
            select: { accessToken: true },
          });
        }
      }
    } else {
      shop = await this.prisma.shop.findFirst({
        where: { id: shopId, userId, status: 'ACTIVE' },
        select: { accessToken: true },
      });
    }

    if (!shop?.accessToken) throw new Error('店铺不存在或无权限');
    return shop.accessToken;
  }

  private async getParentUserId(userId: string): Promise<string | null> {
    const user = await this.prisma.user.findUnique({
      where: { id: userId },
      select: { createdBy: true },
    });
    return user?.createdBy || null;
  }

  private getProductListResult(response: any) {
    return response?.result || response || {};
  }

  private getProductListData(response: any) {
    const result = this.getProductListResult(response);
    return Array.isArray(result?.data) ? result.data : [];
  }

  private getProductListTotalCount(response: any) {
    const result = this.getProductListResult(response);
    const total = Number(result?.totalCount);
    return Number.isFinite(total) && total >= 0 ? Math.trunc(total) : this.getProductListData(response).length;
  }

  private isProductListDeepPaginationError(error: any) {
    const message = this.getErrorMessage(error);
    return /Pagination too deep/i.test(message);
  }

  private getProductSkcId(product: any) {
    const value = product?.productSkcId;
    if (value === undefined || value === null || value === '') return null;
    return String(value);
  }

  private getProductCreatedAtBoundary(products: any[]) {
    for (let index = products.length - 1; index >= 0; index -= 1) {
      const createdAt = this.toOptionalInteger(products[index]?.createdAt);
      if (createdAt !== undefined) return createdAt;
    }
    return undefined;
  }

  private toOptionalInteger(value?: string | number | boolean | null) {
    if (value === undefined || value === null || value === '') return undefined;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? Math.round(numeric) : undefined;
  }

  private toOptionalBigInt(value?: string | number | boolean | null) {
    const numeric = this.toOptionalInteger(value);
    return numeric === undefined ? undefined : BigInt(numeric);
  }

  private toOptionalTimestampNumber(value?: string | number | bigint | boolean | null) {
    if (typeof value === 'bigint') return Number(value);
    return this.toOptionalInteger(value as any);
  }

  private getProductCategoryId(product: any, key: string) {
    return this.toOptionalInteger(product?.categories?.[key]?.catId);
  }

  private chunkArray<T>(items: T[], size: number) {
    const chunks: T[][] = [];
    for (let index = 0; index < items.length; index += size) {
      chunks.push(items.slice(index, index + size));
    }
    return chunks;
  }

  private toIsoString(value?: Date | string | null) {
    if (!value) return null;
    if (value instanceof Date) return value.toISOString();
    return String(value);
  }

  private getBooleanEnv(name: string) {
    return ['1', 'true', 'yes', 'on'].includes(String(process.env[name] || '').trim().toLowerCase());
  }

  private buildRefreshQueueJobId(shopId: string) {
    return `${REFRESH_QUEUE_JOB_ID_PREFIX}:${shopId}`;
  }

  private async reconcileRunningRefreshQueueState(runningJob: any) {
    try {
      const existingJob = await this.refreshQueue.getJob(this.buildRefreshQueueJobId(runningJob.shopId));
      if (!existingJob) return runningJob;

      const state = await existingJob.getState();
      if (!TERMINAL_QUEUE_STATES.has(state)) return runningJob;

      await existingJob.remove();
      const failedReason = state === 'failed' ? this.getQueueJobFailedReason(existingJob) : null;
      await this.markRefreshJobFailed(
        runningJob.id,
        state === 'failed'
          ? `商品缓存刷新队列任务失败: ${failedReason || '未知错误'}`
          : `商品缓存刷新队列任务已结束，但同步状态仍为 ${runningJob.status}`,
      );
      return null;
    } catch (error: any) {
      this.logger.warn(
        `[商品列表缓存] 运行中任务队列状态对账失败 jobId=${runningJob.id}: ${this.getErrorMessage(error)}`,
      );
      return runningJob;
    }
  }

  private async markRefreshJobFailed(jobId: string, message: string) {
    await (this.prisma as any).temuProductListSyncJob.updateMany({
      where: {
        id: jobId,
        status: { in: RUNNING_REFRESH_STATUSES },
      },
      data: {
        status: 'FAILED',
        errorMessage: message,
        finishedAt: new Date(),
      },
    });
  }

  private async isRefreshJobCancelled(jobId: string) {
    const job = await (this.prisma as any).temuProductListSyncJob.findUnique({
      where: { id: jobId },
      select: { status: true },
    });
    return job?.status === CANCELLED_REFRESH_STATUS;
  }

  private async updateRefreshJobIfNotCancelled(jobId: string, data: Record<string, any>) {
    try {
      return await (this.prisma as any).temuProductListSyncJob.update({
        where: { id: jobId, status: { not: CANCELLED_REFRESH_STATUS } },
        data,
      });
    } catch (error: any) {
      if (error?.code === 'P2025') return null;
      throw error;
    }
  }

  private async markRefreshJobCancelledProgress(
    jobId: string,
    totalCount: number,
    syncedCount: number,
    currentPage: number,
  ) {
    await (this.prisma as any).temuProductListSyncJob.updateMany({
      where: { id: jobId, status: CANCELLED_REFRESH_STATUS },
      data: {
        totalCount,
        syncedCount,
        currentPage,
        errorMessage: null,
        finishedAt: new Date(),
      },
    });
  }

  private getQueueJobFailedReason(job: any) {
    if (job?.failedReason) return String(job.failedReason);
    if (typeof job?.toJSON === 'function') {
      const json = job.toJSON();
      if (json?.failedReason) return String(json.failedReason);
    }
    return null;
  }

  private async findExistingQueuedRefresh(queueJobId: string) {
    const existingJob = await this.refreshQueue.getJob(queueJobId);
    if (!existingJob) return null;

    const state = await existingJob.getState();
    if (TERMINAL_QUEUE_STATES.has(state)) {
      await existingJob.remove();
      return null;
    }

    const syncJobId = (existingJob.data as ProductListCacheRefreshJobData | undefined)?.jobId;
    if (!syncJobId) return null;

    const syncJob = await (this.prisma as any).temuProductListSyncJob.findUnique({
      where: { id: syncJobId },
    });
    if (syncJob && RUNNING_REFRESH_STATUSES.includes(syncJob.status)) {
      return syncJob;
    }

    return null;
  }

  private async findQueuedRefreshOwner(queueJobId: string): Promise<QueuedRefreshOwner | null> {
    const existingJob = await this.refreshQueue.getJob(queueJobId);
    if (!existingJob) return null;

    const state = await existingJob.getState();
    const syncJobId = (existingJob.data as ProductListCacheRefreshJobData | undefined)?.jobId;
    if (TERMINAL_QUEUE_STATES.has(state)) {
      await existingJob.remove();
    }
    if (!syncJobId) return null;

    const syncJob = await (this.prisma as any).temuProductListSyncJob.findUnique({
      where: { id: syncJobId },
    });
    return { syncJobId, syncJob };
  }

  private async markRefreshJobAsDuplicate(jobId: string, existingJobId: string) {
    return (this.prisma as any).temuProductListSyncJob.update({
      where: { id: jobId },
      data: {
        status: 'FAILED',
        errorMessage: `商品缓存刷新任务已存在: ${existingJobId}`,
        finishedAt: new Date(),
      },
    });
  }

  private getErrorMessage(error: any) {
    return error?.message || String(error);
  }
}
