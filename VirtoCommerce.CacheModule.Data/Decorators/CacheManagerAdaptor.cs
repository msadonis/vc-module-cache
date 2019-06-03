using System;
using System.Linq;
using CacheManager.Core;
using VirtoCommerce.CacheModule.Common;
using VirtoCommerce.Platform.Core.Settings;
using VirtoCommerce.Platform.Data.Common;

namespace VirtoCommerce.CacheModule.Data.Decorators
{
    public sealed class CacheManagerAdaptor
    {
        private readonly ICacheManager<object> _cacheManager;
        private readonly ISettingsManager _settingManager;

        public CacheManagerAdaptor(ISettingsManager settingManager, ICacheManager<object> cacheManager)
        {
            _settingManager = settingManager;
            _cacheManager = cacheManager;
        }

        public T Get<T>(string cacheKey, string region, Func<T> getValueFunction)
        {
            if (_settingManager.GetValue("Cache.Enable", true))
            {
                if (CacheSkipper.CurrentValue.SkipCacheRead && !CacheSkipper.CurrentValue.SkipCacheWrite)
                {
                    var result = getValueFunction();
                    Put(cacheKey, result, region);
                    return result;
                }
                else if (!CacheSkipper.CurrentValue.SkipCacheRead && CacheSkipper.CurrentValue.SkipCacheWrite)
                {
                    var item = _cacheManager.GetCacheItem(cacheKey, region);
                    return item != null ? (T)item.Value : getValueFunction();
                }
                else
                {
                    return _cacheManager.Get(cacheKey, region, GetExpirationTimeout(), getValueFunction);
                }
            }
            return getValueFunction();
        }

        public T Get<T>(string cacheKey, string region)
        {
            if (_settingManager.GetValue("Cache.Enable", true) && !CacheSkipper.CurrentValue.SkipCacheRead)
            {
                // Don't use generic get method, because it can require the objects to be IConvertible. 
                var result = (T)_cacheManager.Get(cacheKey, region);
                return result;
            }
            return default(T);
        }

        public void Put<T>(string cacheKey, T value, string region)
        {
            if (_settingManager.GetValue("Cache.Enable", true) && !CacheSkipper.CurrentValue.SkipCacheWrite)
            {
                var item = new CacheItem<object>(cacheKey, region, value, GetExpirationMode(), GetExpirationTimeout());
                _cacheManager.Put(cacheKey, value, region);
            }
        }

        public void Remove(string cacheKey, string region)
        {
            if (_settingManager.GetValue("Cache.Enable", true))
            {
                var entryRemoved = _cacheManager.Remove(cacheKey, region);
                // This filthy hack ensures that entries are cleared from cache in a distributed environment even when
                // they were not in the (local) in-memory cache.
                if (!entryRemoved && _cacheManager != null)
                {
                    if (region == null)
                        ((BaseCacheManager<object>)_cacheManager).Backplane.NotifyRemove(cacheKey);
                    else
                        ((BaseCacheManager<object>)_cacheManager).Backplane.NotifyRemove(cacheKey, region);
                }
            }
        }

        public void ClearRegion(string region)
        {
            if (_settingManager.GetValue("Cache.Enable", true))
            {
                _cacheManager.ClearRegion(region);
            }
        }

        /// <summary>
        /// Allow reading a batch with caching on individual entry level.
        /// </summary>
        /// <typeparam name="T">Type of values to cache.</typeparam>
        /// <param name="ids">Batch of identifiers to read with caching.</param>
        /// <param name="cacheKeyGen">Function to generate a cache key from an id.</param>
        /// <param name="region">Region to use for caching.</param>
        /// <param name="singleReader">Function to read a single entry, can be used to optimize single entry access.</param>
        /// <param name="multiReader">Function to read a batch of entries.</param>
        /// <param name="idReader">Function to read the id from an entry.</param>
        /// <returns>Cached or newly read batch of entries.</returns>
        public T[] GetMultiWithIndividualCaching<T>(string[] ids, Func<string, string> cacheKeyGen, string region,
            Func<string, T> singleReader, Func<string[], T[]> multiReader, Func<T, string> idReader)
            where T : class
        {
            if (!_settingManager.GetValue("Cache.Enable", true))
            {
                return multiReader(ids);
            }

            if (ids == null) throw new ArgumentNullException(nameof(ids));

            // Cleanup input.
            ids = ids.Where(x => x != null).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            // Limit garbage generation of single id access.
            if (ids.Count() == 1)
            {
                return new T[] { singleReader(ids.First()) };
            }

            // Read currently cached info into a dictionary.
            var withCacheInfo = (
                from id in ids
                let cacheKey = cacheKeyGen(id)
                // Don't use generic get method, because it can require the objects to be IConvertible. 
                let cached = CacheSkipper.CurrentValue.SkipCacheRead ? null : (T)_cacheManager.Get(cacheKey, region)
                select new
                {
                    id,
                    cacheKey,
                    cached
                }
            ).ToDictionary(x => x.id, x => x, StringComparer.OrdinalIgnoreCase);

            // Read all uncached in one go from inner service.
            var uncached = withCacheInfo.Values.Where(x => x.cached == null).Select(x => x.id).ToArray();
            if (uncached.Length > 0)
            {
                var fetched = multiReader(uncached);
                foreach (var fetchedInfo in fetched.Where(x => x != null))
                {
                    var id = idReader(fetchedInfo);

                    // Ignore entry for which we cannot get an id. 
                    if (id == null) continue;

                    var cacheKey = cacheKeyGen(id);
                    if (!CacheSkipper.CurrentValue.SkipCacheWrite)
                        _cacheManager.Put(new CacheItem<object>(cacheKey, region, fetchedInfo, GetExpirationMode(), GetExpirationTimeout()));

                    withCacheInfo[id] = new
                    {
                        id,
                        cacheKey,
                        cached = fetchedInfo
                    };
                }
            }

            // Return in same sequence as input.
            return ids.Select(x => withCacheInfo[x].cached).Where(x => x != null).ToArray();
        }

        private TimeSpan GetExpirationTimeout()
        {
            return TimeSpan.FromSeconds(_settingManager.GetValue("Cache.ExpirationTimeout", 60));
        }

        private ExpirationMode GetExpirationMode()
        {
            return ExpirationMode.Sliding;
        }
    }
}
