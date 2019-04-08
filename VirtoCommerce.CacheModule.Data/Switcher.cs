using System;
using System.Collections.Generic;
using System.Threading;

namespace VirtoCommerce.CacheModule.Common
{
    // This class might be better placed in platform assembly?
    public class Switcher<TValue, TSwitchType> : IDisposable
    {
        private static AsyncLocal<Stack<TValue>> _contextState = new AsyncLocal<Stack<TValue>>();
        private int _disposed;

        protected Switcher(TValue objectToSwitchTo)
        {
            GetContextStack(true).Push(objectToSwitchTo);
        }

        public static TValue CurrentValue
        {
            get
            {
                var stack = GetContextStack(false);
                if (stack == null || stack.Count == 0)
                    return default(TValue);
                return stack.Peek();
            }
        }

        private static Stack<TValue> GetContextStack(bool createIfNotExists)
        {
            var stack = _contextState.Value;
            if (stack == null & createIfNotExists)
            {
                stack = new Stack<TValue>();
                _contextState.Value = stack;
            }
            return stack;
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;

            var stack = GetContextStack(false);
            stack?.Pop();
        }
    }

    public class CacheSkipper : Switcher<CacheSkipper.Settings, CacheSkipper>
    {
        public CacheSkipper(bool skipCacheRead = true, bool skipCacheWrite = true)
            : base(new Settings { SkipCacheRead = skipCacheRead, SkipCacheWrite = skipCacheWrite })
        {
        }

        public struct Settings
        {
            public bool SkipCacheRead { get; set; }
            public bool SkipCacheWrite { get; set; }
        }
    }
}
