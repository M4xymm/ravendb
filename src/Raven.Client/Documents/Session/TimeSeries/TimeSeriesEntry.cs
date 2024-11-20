﻿//-----------------------------------------------------------------------
// <copyright file="TimeSeriesEntry.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.TimeSeries
{
    /// <summary>
    /// Represents an individual entry in a time series, storing a timestamp, associated values, and metadata.
    /// </summary>
    /// <remarks>
    /// <see cref="TimeSeriesEntry"/> is used as a base type for time series data projections 
    /// in operations such as <see cref="GetTimeSeriesOperation{TValues}"/>.
    /// It contains a timestamp and associated values for a single entry within a time series.
    /// </remarks>
    public class TimeSeriesEntry : ITimeSeriesQueryStreamEntry
    {
        /// <summary>
        /// Gets or sets the timestamp of the time series entry.
        /// This represents the point in time at which the values were recorded.
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the values associated with the time series entry.
        /// These values represent the numeric data points recorded at the specified <see cref="Timestamp"/>.
        /// </summary>
        public double[] Values { get; set; }

        /// <summary>
        /// Gets or sets the tag for the time series entry.
        /// This optional metadata provides additional context about the entry, such as the source or a descriptive label.
        /// </summary>
        public string Tag { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the entry is part of a rollup time series.
        /// A rollup aggregates data over a specific time range, as opposed to individual data points.
        /// </summary>
        public bool IsRollup { get; set; }

        /// <summary>
        /// Gets or sets a dictionary of node-specific values for the time series entry.
        /// The key represents the node identifier, and the value is an array of numeric data points associated with that node.
        /// </summary>
        public Dictionary<string, double[]> NodeValues { get; set; }

        /// <summary>
        /// Gets or sets the value of the time series entry if it contains a single value.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the entry contains more than one value.
        /// </exception>
        [JsonDeserializationIgnore]
        public double Value
        {
            get
            {
                if (Values.Length == 1)
                    return Values[0];

                throw new InvalidOperationException("Entry has more than one value.");
            }
            set
            {
                if (Values.Length == 1)
                {
                    Values[0] = value;
                    return;
                }

                throw new InvalidOperationException("Entry has more than one value.");
            }
        }

        /// <summary>
        /// Returns a string representation of the time series entry, including the timestamp, values, and tag.
        /// </summary>
        /// <returns>A string in the format "[Timestamp] Values Tag".</returns>
        public override string ToString()
        {
            return $"[{Timestamp}] {string.Join(", ", Values)} {Tag}";
        }
    }

    public sealed class TimeSeriesEntry<T> : TimeSeriesEntry, IPostJsonDeserialization where T : new()
    {
        public new T Value { get; set; }

        public void Deconstruct(out DateTime timestamp, out T value)
        {
            timestamp = Timestamp;
            value = Value ?? throw new ArgumentNullException(nameof(Value));
        }

        public void Deconstruct(out DateTime timestamp, out T value, out string tag)
        {
            timestamp = Timestamp;
            value = Value ?? throw new ArgumentNullException(nameof(Value));
            tag = Tag;
        }

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            Value = TimeSeriesValuesHelper.SetMembers<T>(Values, IsRollup);
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            Value = TimeSeriesValuesHelper.SetMembers<T>(Values, IsRollup);
        }
    }

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class TimeSeriesValueAttribute : Attribute
    {
        public readonly byte Index;

        public TimeSeriesValueAttribute(byte index)
        {
            Index = index;
        }
    }

    internal static class TimeSeriesValuesHelper
    {
        private static readonly ConcurrentDictionary<Type, SortedDictionary<byte, MemberInfo>> _cache = new ConcurrentDictionary<Type, SortedDictionary<byte, MemberInfo>>();

        internal static SortedDictionary<byte, MemberInfo> GetMembersMapping(Type type)
        {
            return _cache.GetOrAdd(type, (t) =>
            {
                SortedDictionary<byte, MemberInfo> mapping = null;
                foreach (var member in ReflectionUtil.GetPropertiesAndFieldsFor(t, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    var attribute = member.GetCustomAttribute<TimeSeriesValueAttribute>(inherit: false);
                    if (attribute == null)
                        continue;

                    var memberType = member.GetMemberType();
                    if (memberType != typeof(double))
                        throw new InvalidOperationException($"Cannot create a mapping for '{t}' type, because member '{member.Name}' is not a double.");

                    var i = attribute.Index;
                    mapping ??= new SortedDictionary<byte, MemberInfo>();
                    if (mapping.ContainsKey(i))
                        throw new InvalidOperationException($"Cannot map '{member.Name}' to {i}, since '{mapping[i].Name}' already mapped to it.");

                    mapping[i] = member;
                }

                if (mapping == null)
                    return null;

                if (mapping.Keys.First() != 0 || mapping.Keys.Last() != mapping.Count - 1)
                    throw new InvalidOperationException($"The mapping of '{t}' must contain consecutive values starting from 0.");

                return mapping;
            });
        }

        internal static IEnumerable<double> GetValues<T>(T obj, bool asRollup = false)
        {
            var mapping = GetMembersMapping(typeof(T));
            if (mapping == null)
                return null;

            var values = new double[mapping.Count];
            foreach (var memberInfo in mapping)
            {
                var index = memberInfo.Key;
                if (asRollup)
                    index *= 6;
                var member = memberInfo.Value;
                values[index] = (double)member.GetValue(obj);
            }

            return values;
        }

        internal static T SetMembers<T>(double[] values, bool asRollup = false) where T : new()
        {
            if (values == null)
                return default;

            var mapping = GetMembersMapping(typeof(T));
            if (mapping == null)
                return default;

            var obj = new T();
            foreach (var memberInfo in mapping)
            {
                var index = memberInfo.Key;
                var value = double.NaN;
                if (index < values.Length)
                {
                    if (asRollup)
                        index *= 6;

                    value = values[index];
                }

                var member = memberInfo.Value;
                member.SetValue(ref obj, value);
            }

            return obj;
        }
    }

    internal static class RollupExtensions
    {
        public static TimeSeriesRollupEntry<T> AsRollupEntry<T>(this TimeSeriesEntry<T> entry) where T : new()
        {
            if (entry.IsRollup == false)
                throw new InvalidCastException("Not a rolled up entry.");

            if (typeof(T) != entry.Value.GetType())
                throw new InvalidCastException($"Can't cast '{typeof(T).FullName}' to '{entry.GetType().FullName}'");

            return new TimeSeriesRollupEntry<T>
            {
                Tag = entry.Tag,
                Timestamp = entry.Timestamp,
                Values = entry.Values,
                IsRollup = true
            };
        }
    }

    public sealed class TimeSeriesRollupEntry<TValues> : TimeSeriesEntry, IPostJsonDeserialization where TValues : new()
    {
        private int _dim;

        private TValues _first;
        private TValues _last;
        private TValues _max;
        private TValues _min;
        private TValues _sum;
        private TValues _count;

        private TValues _average;

        internal TimeSeriesRollupEntry()
        {
        }

        public TimeSeriesRollupEntry(DateTime timestamp)
        {
            IsRollup = true;
            Timestamp = timestamp;

            var mapping = TimeSeriesValuesHelper.GetMembersMapping(typeof(TValues));
            Values = new double[mapping.Count * 6];
        }

        [JsonIgnore]
        public TValues First
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_first, default) == false)
                    return _first;

                Build2DArray();
                _first = TimeSeriesValuesHelper.SetMembers<TValues>(_innerValues[(int)AggregationType.First]);
                return _first;
            }
            set => _first = value;
        }

        [JsonIgnore]
        public TValues Last
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_last, default) == false)
                    return _last;

                Build2DArray();
                _last = TimeSeriesValuesHelper.SetMembers<TValues>(_innerValues[(int)AggregationType.Last]);
                return _last;
            }
            set => _last = value;
        }

        [JsonIgnore]
        public TValues Min
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_min, default) == false)
                    return _min;

                Build2DArray();
                _min = TimeSeriesValuesHelper.SetMembers<TValues>(_innerValues[(int)AggregationType.Min]);
                return _min;
            }
            set => _min = value;
        }

        [JsonIgnore]
        public TValues Max
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_max, default) == false)
                    return _max;

                Build2DArray();
                _max = TimeSeriesValuesHelper.SetMembers<TValues>(_innerValues[(int)AggregationType.Max]);
                return _max;
            }
            set => _max = value;
        }

        [JsonIgnore]
        public TValues Sum
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_sum, default) == false)
                    return _sum;

                Build2DArray();
                _sum = TimeSeriesValuesHelper.SetMembers<TValues>(_innerValues[(int)AggregationType.Sum]);
                return _sum;
            }
            set => _sum = value;
        }

        [JsonIgnore]
        public TValues Count
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_count, default) == false)
                    return _count;

                Build2DArray();
                _count = TimeSeriesValuesHelper.SetMembers<TValues>(_innerValues[(int)AggregationType.Count]);
                return _count;
            }
            set => _count = value;
        }

        [JsonIgnore]
        public TValues Average
        {
            get
            {
                if (EqualityComparer<TValues>.Default.Equals(_average, default) == false)
                    return _average;

                Build2DArray();
                var arr = new double[_dim];
                for (int i = 0; i < arr.Length; i++)
                {
                    var count = _innerValues[(int)AggregationType.Count][i];
                    if (IsNormal(count) == false)
                    {
                        arr[i] = double.NaN;
                        continue;
                    }

                    arr[i] = _innerValues[(int)AggregationType.Sum][i] / _innerValues[(int)AggregationType.Count][i];
                }

                _average = TimeSeriesValuesHelper.SetMembers<TValues>(arr);
                return _average;
            }
        }

        // taken from dotnet/runtime since it is supported only in 2.1 and up
        // https://github.com/dotnet/runtime/blob/abfdb542e8dfd72ab2715222edf527952e9fda10/src/libraries/System.Private.CoreLib/src/System/Double.cs#L121
        private static bool IsNormal(double d)
        {
            long bits = BitConverter.DoubleToInt64Bits(d);
            bits &= 0x7FFFFFFFFFFFFFFF;
            return (bits < 0x7FF0000000000000) && (bits != 0) && ((bits & 0x7FF0000000000000) != 0);
        }

        private bool _innerArrayInitialized;

        private void Build2DArray()
        {
            if (IsRollup == false)
                throw new InvalidOperationException("Not a rollup entry.");

            if (_innerArrayInitialized)
                return;

            _dim = Values.Length / 6;
            _innerValues = new double[6][];
            for (int i = 0; i < 6; i++)
            {
                _innerValues[i] = new double[_dim];
                for (int j = 0; j < _dim; j++)
                {
                    _innerValues[i][j] = Values[j * 6 + i];
                }
            }

            _innerArrayInitialized = true;
        }

        private double[][] _innerValues;

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            Build2DArray();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            Build2DArray();
        }

        internal void SetValuesFromMembers()
        {
            SetInternal(_first, (int)AggregationType.First);
            SetInternal(_last, (int)AggregationType.Last);
            SetInternal(_min, (int)AggregationType.Min);
            SetInternal(_max, (int)AggregationType.Max);
            SetInternal(_count, (int)AggregationType.Count);
            SetInternal(_sum, (int)AggregationType.Sum);
        }

        private void SetInternal(TValues entry, int position)
        {
            if (entry == null)
                return;

            var values = TimeSeriesValuesHelper.GetValues(entry).ToArray();
            for (int i = 0; i < values.Length; i++)
            {
                Values[6 * i + position] = values[i];
            }
        }

        public static explicit operator TimeSeriesEntry<TValues>(TimeSeriesRollupEntry<TValues> rollupEntry)
        {
            return new TimeSeriesEntry<TValues>
            {
                Timestamp = rollupEntry.Timestamp,
                Values = TimeSeriesValuesHelper.GetValues(rollupEntry.First).ToArray()
            };
        }
    }
}
