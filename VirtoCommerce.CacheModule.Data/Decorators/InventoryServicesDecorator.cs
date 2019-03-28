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
        // Use original name to stay backwards compatible with the changes tracker service.
        public const string RegionName = "Inventory-Cache-Region";
        public const string IndividualRegionName = "Inventory-Individual-Cache-Region";

        public InventoryServicesDecorator(IInventoryService inventoryService, IInventorySearchService inventorySearchService, CacheManagerAdaptor cacheManager)
        {
            _inventoryService = inventoryService;
            _inventorySearchService = inventorySearchService;
            _cacheManager = cacheManager;
        }

        #region ICachedServiceDecorator
        public void ClearCache()
        {
            _cacheManager.ClearRegion(IndividualRegionName);
            _cacheManager.ClearRegion(RegionName);
        }

        public void ClearCacheForProduct(string productId)
        {
            if (productId == null) return;

            var cacheKey = GetProductInventoryCacheKey(productId);
            _cacheManager.Remove(cacheKey, IndividualRegionName);
            _cacheManager.ClearRegion(RegionName);
        }
        #endregion


        #region IInventorySearchService Members
        public GenericSearchResult<InventoryInfo> SearchInventories(InventorySearchCriteria criteria)
        {
            var cacheKey = GetCacheKey("IInventorySearchService.SearchInventories", criteria.GetCacheKey());
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _inventorySearchService.SearchInventories(criteria));
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
                _cacheManager.Put(cacheKey, inventoryInfo, IndividualRegionName);
            }
            return retVal;
        }

        public IEnumerable<InventoryInfo> GetProductsInventoryInfos(IEnumerable<string> productIds)
        {
            if (productIds == null) throw new ArgumentNullException(nameof(productIds));

            var result = _cacheManager.GetMultiWithIndividualCaching<IEnumerable<InventoryInfo>>(
                productIds.ToArray(),
                GetProductInventoryCacheKey,
                IndividualRegionName,
                id => GetProductInventoryInfos(id),
                ids => _inventoryService.GetProductsInventoryInfos(ids)
                    .GroupBy(x => x.ProductId, StringComparer.OrdinalIgnoreCase)
                    .ToArray(),
                x => x.FirstOrDefault()?.ProductId
            );

            return result.SelectMany(x => x);
        }

        private IEnumerable<InventoryInfo> GetProductInventoryInfos(string productId)
        {
            if (productId == null) throw new ArgumentNullException(nameof(productId));

            var cacheKey = GetProductInventoryCacheKey(productId);
            var retVal = _cacheManager.Get(cacheKey, IndividualRegionName, () =>
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
