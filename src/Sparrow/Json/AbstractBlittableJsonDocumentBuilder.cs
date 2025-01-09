using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Platform;

namespace Sparrow.Json
{
    public abstract class AbstractBlittableJsonDocumentBuilder : IDisposable
    {
        private bool _disposed;

        private readonly GlobalPoolItem _cacheItem;
        protected readonly ListCache<PropertyTag> _propertiesCache;
        protected readonly ListCache<int> _positionsCache;
        protected readonly ListCache<BlittableJsonToken> _tokensCache;

        private static readonly ObjectPool<FastStack<BuildingState>> ContinuationPool = new (() => new FastStack<BuildingState>(32));

        protected readonly FastStack<BuildingState> _continuationState;

        private static readonly PerCoreContainer<GlobalPoolItem> GlobalCache;

        static AbstractBlittableJsonDocumentBuilder()
        {
            GlobalCache = PlatformDetails.Is32Bits 
                ? new PerCoreContainer<GlobalPoolItem>(4) 
                : new PerCoreContainer<GlobalPoolItem>();
        }

        protected AbstractBlittableJsonDocumentBuilder()
        {
            if (GlobalCache.TryPull(out _cacheItem) == false)
                _cacheItem = new GlobalPoolItem();
            _propertiesCache = _cacheItem.PropertyCache;
            _positionsCache = _cacheItem.PositionsCache;
            _tokensCache = _cacheItem.TokensCache;
            _continuationState = ContinuationPool.Allocate();
        }

        public virtual void Dispose()
        {
            GlobalCache.TryPush(_cacheItem);

            // PERF: We are clearing the array without removing the references because the type is an struct.
            _continuationState.WeakClear();
            ContinuationPool.Free(_continuationState);

            _disposed = true;
        }

        protected void ClearState()
        {
            while (_continuationState.Count > 0)
            {
                var state = _continuationState.Pop();
                _propertiesCache.Return(ref state.Properties);
                _tokensCache.Return(ref state.Types);
                _positionsCache.Return(ref state.Positions);
            }
        }

        [Conditional("DEBUG")]
        protected void AssertNotDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        protected struct BuildingState(ContinuationState state, bool partialRead = false)
        {
            public ContinuationState State = state;
            public int MaxPropertyId;
            public CachedProperties.PropertyName CurrentProperty;
            public FastList<PropertyTag> Properties;
            public FastList<BlittableJsonToken> Types;
            public FastList<int> Positions;
            public long FirstWrite;

            internal bool PartialRead = partialRead;
        }

        protected const int AllowedAllStates = 0xFF;
        protected const int DisallowBufferedStates = 0x7F;
        protected const int Buffered = 0x80;

        protected enum ContinuationState : int
        {
            ReadObject = 0x01,
            ReadArray = 0x02,
            ReadObjectDocument = 0x03,
            ReadArrayDocument = 0x04,
            ReadPropertyName = 0x05,
            ReadPropertyValue = 0x06,
            CompleteDocumentArray = 0x07,
            CompleteReadingPropertyValue = 0x08,

            ReadArrayValue = 0x09,
            ReadValue = 0x0A,
            CompleteArray = 0x0B,
            CompleteArrayValue = 0x0C,

            // Support for vector type.
            ReadBufferedArrayValue = ReadArrayValue | Buffered,
            ReadBufferedValue = ReadValue | Buffered,
            CompleteBufferedArray = CompleteArray | Buffered,
            CompleteBufferedArrayValue = CompleteArrayValue | Buffered,
        }

        public struct PropertyTag
        {
            public int Position;

            public override string ToString()
            {
                return $"{nameof(Position)}: {Position}, {nameof(Property)}: {Property.Comparer} {Property.PropertyId}, {nameof(Type)}: {(BlittableJsonToken)Type}";
            }

            public CachedProperties.PropertyName Property;
            public byte Type;

            public PropertyTag(byte type, CachedProperties.PropertyName property, int position)
            {
                Type = type;
                Property = property;
                Position = position;
            }
        }

        protected sealed class ListCache<T>
        {
            private static readonly int MaxSize = PlatformDetails.Is32Bits ? 256 : 1024;

            private readonly FastList<FastList<T>> _cache = new FastList<FastList<T>>();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public FastList<T> Allocate()
            {
                return _cache.RemoveLast() ?? new FastList<T>();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Return(ref FastList<T> n)
            {
                if (n == null)
                    return;
                if (_cache.Count >= MaxSize)
                {
                    n = null;
                    return;
                }
                n.Clear();
                _cache.Add(n);
                n = null;
            }
        }

        private sealed class GlobalPoolItem
        {
            public readonly ListCache<PropertyTag> PropertyCache = new ListCache<PropertyTag>();
            public readonly ListCache<int> PositionsCache = new ListCache<int>();
            public readonly ListCache<BlittableJsonToken> TokensCache = new ListCache<BlittableJsonToken>();
        }
    }
}
