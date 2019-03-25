using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using VirtoCommerce.Domain.Catalog.Model;
using VirtoCommerce.Domain.Catalog.Services;

namespace VirtoCommerce.CacheModule.Data.Decorators
{
    public sealed class CatalogServicesDecorator : ICachedServiceDecorator, IItemService, ICatalogSearchService, IPropertyService, ICategoryService, ICatalogService
    {
        private readonly IItemService _itemService;
        private readonly ICatalogSearchService _searchService;
        private readonly IPropertyService _propertyService;
        private readonly ICategoryService _categoryService;
        private readonly ICatalogService _catalogService;
        private readonly CacheManagerAdaptor _cacheManager;

        // Use multiple cache regions so that we don't have to clear the entire cache region on every update.
        public const string RegionName = "Catalog-Cache-Region";
        public const string AggregatedRegionName = "Catalog-Aggregated-Cache-Region";

        public CatalogServicesDecorator(IItemService itemService, ICatalogSearchService searchService, IPropertyService propertyService, ICategoryService categoryService, ICatalogService catalogService, CacheManagerAdaptor cacheManager)
        {
            _itemService = itemService;
            _searchService = searchService;
            _propertyService = propertyService;
            _categoryService = categoryService;
            _catalogService = catalogService;
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

            var cacheKey = GetProductCacheKey(productId);
            _cacheManager.Remove(cacheKey, RegionName);
            _cacheManager.ClearRegion(AggregatedRegionName);
        }
        #endregion

        #region IItemService members
        public void Create(CatalogProduct[] items)
        {
            _itemService.Create(items);
            foreach (var item in items)
            {
                ClearCacheForProduct(item?.Id);
            }
        }

        public CatalogProduct Create(CatalogProduct item)
        {
            var retVal = _itemService.Create(item);
            ClearCacheForProduct(item?.Id);
            return retVal;
        }

        public void Delete(string[] itemIds)
        {
            _itemService.Delete(itemIds);
            foreach (var itemId in itemIds)
            {
                ClearCacheForProduct(itemId);
            }
        }

        public CatalogProduct GetById(string itemId, ItemResponseGroup respGroup, string catalogId = null)
        {
            var cacheKey = GetProductCacheKey(itemId);
            var cacheEntry = _cacheManager.Get(cacheKey, RegionName, () => new ProductCacheEntry(itemId));
            return cacheEntry.Get(respGroup, catalogId, () => _itemService.GetById(itemId, respGroup, catalogId));
        }

        public CatalogProduct[] GetByIds(string[] itemIds, ItemResponseGroup respGroup, string catalogId = null)
        {
            var cacheEntries = _cacheManager.GetMultiWithIndividualCaching(
                itemIds,
                GetProductCacheKey,
                RegionName,
                id => new ProductCacheEntry(id, respGroup, catalogId, _itemService.GetById(id, respGroup, catalogId)),
                ids => _itemService.GetByIds(ids, respGroup, catalogId)
                    .Select(x => new ProductCacheEntry(x.Id, respGroup, catalogId, x))
                    .ToArray(),
                x => x.ItemId
            );

            return cacheEntries
                .Select(x => x.Get(respGroup, catalogId))
                .Where(x => x != null)
                .ToArray();
        }

        public void Update(CatalogProduct[] items)
        {
            _itemService.Update(items);
            foreach (var item in items)
            {
                ClearCacheForProduct(item?.Id);
            }
        }
        #endregion

        #region ICatalogSearchService members
        public SearchResult Search(SearchCriteria criteria)
        {
            var cacheKey = GetCacheKey("CatalogSearchService.Search", criteria.GetCacheKey());
            var retVal = _cacheManager.Get(cacheKey, AggregatedRegionName, () => _searchService.Search(criteria));
            return retVal;
        }
        #endregion

        #region IPropertyService members
        public Property GetById(string propertyId)
        {
            var cacheKey = GetCacheKey("PropertyService.GetById", string.Join(", ", propertyId));
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _propertyService.GetById(propertyId));
            return retVal;
        }

        public Property[] GetByIds(string[] propertyIds)
        {
            var cacheKey = GetCacheKey("PropertyService.GetByIds", string.Join(", ", propertyIds));
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _propertyService.GetByIds(propertyIds));
            return retVal;
        }

        public Property Create(Property property)
        {
            var retVal = _propertyService.Create(property);
            ClearCache();
            return retVal;
        }

        public void Update(Property[] properties)
        {
            _propertyService.Update(properties);
            ClearCache();
        }

        public Property[] GetAllCatalogProperties(string catalogId)
        {
            var cacheKey = GetCacheKey("PropertyService.GetAllCatalogProperties", catalogId);
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _propertyService.GetAllCatalogProperties(catalogId));
            return retVal;
        }

        public Property[] GetAllProperties()
        {
            var cacheKey = GetCacheKey("PropertyService.GetAllProperties");
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _propertyService.GetAllProperties());
            return retVal;
        }

        public PropertyDictionaryValue[] SearchDictionaryValues(string propertyId, string keyword)
        {
            var cacheKey = GetCacheKey("PropertyService.SearchDictionaryValues", propertyId, keyword);
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _propertyService.SearchDictionaryValues(propertyId, keyword));
            return retVal;
        }

        void IPropertyService.Delete(string[] propertyIds)
        {
            _propertyService.Delete(propertyIds);
            ClearCache();
        }
        #endregion

        #region ICategoryService Members
        public Category[] GetByIds(string[] categoryIds, CategoryResponseGroup responseGroup, string catalogId = null)
        {
            var cacheKey = GetCacheKey("CategoryService.GetByIds", string.Join(", ", categoryIds), responseGroup.ToString(), catalogId);
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _categoryService.GetByIds(categoryIds, responseGroup, catalogId));
            return retVal;
        }

        public Category GetById(string categoryId, CategoryResponseGroup responseGroup, string catalogId = null)
        {
            var cacheKey = GetCacheKey("CategoryService.GetById", categoryId, responseGroup.ToString(), catalogId);
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _categoryService.GetById(categoryId, responseGroup, catalogId));
            return retVal;
        }

        public void Create(Category[] categories)
        {
            _categoryService.Create(categories);
            ClearCache();
        }

        public Category Create(Category category)
        {
            var retVal = _categoryService.Create(category);
            ClearCache();
            return retVal;
        }

        void ICategoryService.Delete(string[] categoryIds)
        {
            _categoryService.Delete(categoryIds);
            ClearCache();
        }

        public void Update(Category[] categories)
        {
            _categoryService.Update(categories);
            ClearCache();
        }
        #endregion

        #region ICatalogService members
        public IEnumerable<Catalog> GetCatalogsList()
        {
            var cacheKey = GetCacheKey("CatalogService.GetCatalogsList");
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _catalogService.GetCatalogsList().ToArray());
            return retVal;
        }

        void ICatalogService.Delete(string[] catalogIds)
        {
            _catalogService.Delete(catalogIds);
            ClearCache();
        }

        Catalog ICatalogService.GetById(string catalogId)
        {
            var cacheKey = GetCacheKey("CatalogService.GetById", catalogId);
            var retVal = _cacheManager.Get(cacheKey, RegionName, () => _catalogService.GetById(catalogId));
            return retVal;
        }

        public Catalog Create(Catalog catalog)
        {
            var retVal = _catalogService.Create(catalog);
            ClearCache();
            return retVal;
        }

        public void Update(Catalog[] catalogs)
        {
            _catalogService.Update(catalogs);
            ClearCache();
        }
        #endregion


        private static string GetCacheKey(params string[] parameters)
        {
            return "Catalog-" + string.Join(", ", parameters);
        }

        private static string GetProductCacheKey(string productId)
        {
            return $"Catalog-ItemService.GetById,{productId.ToLowerInvariant()}";
        }

        /// <summary>
        /// Cache entry to store different requested versions of same product.
        /// </summary>
        private class ProductCacheEntry
        {
            private readonly ConcurrentDictionary<string, CatalogProduct> Products
                = new ConcurrentDictionary<string, CatalogProduct>(StringComparer.OrdinalIgnoreCase);
            private readonly ConcurrentDictionary<string, object> Locks
                = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            public string ItemId { get; }

            public ProductCacheEntry(string itemId)
            {
                ItemId = itemId;
            }

            /// <summary>
            /// Create cache entry with initial version for cache entry.
            /// </summary>
            public ProductCacheEntry(string itemId, ItemResponseGroup respGroup, string catalogId, CatalogProduct product)
                : this(itemId)
            {
                var versionKey = GetVersionKey(respGroup, catalogId);
                Products[versionKey] = product;
            }

            public CatalogProduct Get(ItemResponseGroup respGroup, string catalogId)
            {
                var versionKey = GetVersionKey(respGroup, catalogId);
                return Products.TryGetValue(versionKey, out var value)
                    ? value
                    : null;
            }

            public CatalogProduct Get(ItemResponseGroup respGroup, string catalogId, Func<CatalogProduct> getValueFunction)
            {
                var versionKey = GetVersionKey(respGroup, catalogId);
                return Products.GetOrAdd(versionKey, k =>
                {
                    lock (Locks.GetOrAdd(versionKey, x => new object()))
                    {
                        return Products.TryGetValue(versionKey, out var value)
                            ? value
                            : getValueFunction();
                    }
                });
            }

            private string GetVersionKey(ItemResponseGroup respGroup, string catalogId)
            {
                return string.Join(",", ItemId, respGroup.ToString(), catalogId ?? "null");
            }
        }
    }
}
