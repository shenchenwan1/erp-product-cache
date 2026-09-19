declare module '*prisma.service' { export type PrismaService = import('@prisma/client').PrismaClient; }
declare module '*account-owner.service' { export type AccountOwnerService = any; }
declare module '*subscription-check.service' { export type SubscriptionCheckService = any; }
declare module '*subscription-entitlement.service' { export type SubscriptionEntitlementService = any; }
declare module '*temu.service' { export interface TemuService { getProducts(token: string, filters: any): Promise<any>; searchProductLifecycle(token: string, filters: any): Promise<any>; } }
