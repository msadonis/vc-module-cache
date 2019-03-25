using System;
using System.Collections.Generic;
using System.Linq;
using VirtoCommerce.Domain.Commerce.Model.Search;
using VirtoCommerce.Domain.Inventory.Model;
using VirtoCommerce.Domain.Inventory.Model.Search;
using VirtoCommerce.Domain.Inventory.Services;

namespace VirtoCommerce.CacheModule.Data.Decorators
{
    public sealed class InventoryServicesDecorator : ICachedServiceDecorator, IInventoryService, IInventorySearchService
    {
        private readonly CacheManagerAdaptor _cacheManager;
        private readonly IInventoryService _inventoryService;
        private readonly IInventorySearchService _inventorySearchService;

        // Use multiple cache regions so that we don't have to clear the entire cache region on every update.
        public const string RegionName = "Inventory-Cache-Region";
        public const string AggregatedRegionName = "Inventory-Aggregated-Cache-Region";

        public InventoryServicesDecorator(IInventoryService inventoryService, IInventorySearchService inventorySearchService, CacheManagerAdaptor cacheManager)
        {
            _inventoryService = inventoryService;
            _inventorySearchService = inventorySearchService;
            _cacheManager = cacheManager;
        }

        #region ICachedServiceDecorator
        public void ClearCache()
        {
            _cacheManager.ClearRegion(RegionName);
            _cacheManager.ClearRegion(AggregatedRegionName);
        }

        public void ClearCacheForProduct(string productId)
        {
            if (productId == null) return;

            var cacheKey = GetProductInventoryCacheKey(productId);
            _cacheManager.Remove(cacheKey, RegionName);
            _cacheManager.ClearRegion(AggregatedRegionName);
        }
        #endregion


        #region IInventorySearchService Members
        public GenericSearchResult<InventoryInfo> SearchInventories(InventorySearchCriteria criteria)
        {
            var cacheKey = GetCacheKey("IInventorySearchService.SearchInventories", criteria.GetCacheKey());
            var retVal = _cacheManager.Get(cacheKey, AggregatedRegionName, () => _inventorySearchService.SearchInventories(criteria));
            return retVal;
        }
        #endregion

        #region IInventoryService Members
        public IEnumerable<InventoryInfo> GetAllInventoryInfos()
        {
            var retVal = _inventorySearchService.SearchInventories(new Domain.Inventory.Model.Search.InventorySearchCriteria { Take = int.MaxValue }).Results;
            foreach (var inventoryInfo in retVal)
            {
                var cacheKey = GetProductInventoryCacheKey(inventoryInfo.ProductId);
                _cacheManager.Put(cacheKey, inventoryInfo, RegionName);
            }
            return retVal;
        }

        public IEnumerable<InventoryInfo> GetProductsInventoryInfos(IEnumerable<string> productIds)
        {
            if (productIds == null) throw new ArgumentNullException(nameof(productIds));

            // Cleanup input.
            productIds = productIds.Where(x => x != null).ToArray();

            // Limit garbage genration of single parameter access.
            if (productIds.Count() == 1)
            {
                return GetProductInventoryInfos(productIds.First());
            }

            // Read cached products into a dictionary.
            var withCacheInfo = (
                from productId in productIds
                let cacheKey = GetProductInventoryCacheKey(productId)
                let cached = _cacheManager.Get<InventoryInfo>(cacheKey, RegionName)
                select new
                {
                    productId,
                    cacheKey,
                    cached
                }
            ).ToDictionary(x => x.productId, x => x, StringComparer.OrdinalIgnoreCase);

            // Read all uncached inventory in one go from inner service.
            var uncached = withCacheInfo.Values.Where(x => x.cached == null).Select(x => x.productId).ToArray();
            if (uncached.Length > 0)
            {
                var fetched = _inventoryService.GetProductsInventoryInfos(uncached);
                foreach (var fetchedInfo in fetched)
                {
                    var cacheKey = GetProductInventoryCacheKey(fetchedInfo.ProductId);
                    _cacheManager.Put(cacheKey, fetchedInfo, RegionName);

                    withCacheInfo[fetchedInfo.ProductId] = new
                    {
                        productId = fetchedInfo.ProductId,
                        cacheKey,
                        cached = fetchedInfo
                    };
                }
            }

            // Return in same sequence as input.
            return productIds.Select(x => withCacheInfo[x].cached).Where(x => x != null).ToArray();
        }

        private IEnumerable<InventoryInfo> GetProductInventoryInfos(string productId)
        {
            if (productId == null) throw new ArgumentNullException(nameof(productId));

            var cacheKey = GetProductInventoryCacheKey(productId);
            var retVal = _cacheManager.Get(cacheKey, RegionName, () =>
                _inventoryService.GetProductsInventoryInfos(new[] { productId }));
            return retVal;
        }

        public void UpsertInventories(IEnumerable<InventoryInfo> inventoryInfos)
        {
            _inventoryService.UpsertInventories(inventoryInfos);
            foreach (var inventoryInfo in inventoryInfos)
            {
                ClearCacheForProduct(inventoryInfo?.ProductId);
            }
        }

        public InventoryInfo UpsertInventory(InventoryInfo inventoryInfo)
        {
            var retVal = _inventoryService.UpsertInventory(inventoryInfo);
            ClearCacheForProduct(inventoryInfo?.ProductId);
            return retVal;
        }
        #endregion

        private static string GetCacheKey(params string[] parameters)
        {
            return "Inventory-" + string.Join(", ", parameters);
        }

        private static string GetProductInventoryCacheKey(string productId)
        {
            return $"Inventory-InventoryService.GetProductInventoryInfos,{productId.ToLowerInvariant()}";
        }
    }
}
