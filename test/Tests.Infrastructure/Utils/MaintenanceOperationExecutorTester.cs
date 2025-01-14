﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Xunit.Sdk;

namespace Tests.Infrastructure.Utils;

public class MaintenanceOperationExecutorTester<TResult> : IMaintenanceOperationExecutorReadTester<TResult>, IMaintenanceOperationExecutorActionTester
{
    private readonly MaintenanceOperationExecutor _executor;
    private readonly Func<IMaintenanceOperation<TResult>> _factoryWithResult;
    private readonly Func<IMaintenanceOperation> _factoryWithoutResult;

    private DatabaseRecord _databaseRecord;

    public MaintenanceOperationExecutorTester(MaintenanceOperationExecutor executor, Func<IMaintenanceOperation<TResult>> factory)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _factoryWithResult = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    public MaintenanceOperationExecutorTester(MaintenanceOperationExecutor executor, Func<IMaintenanceOperation> factory)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _factoryWithoutResult = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    public async Task ExecuteOnAllAsync(Func<Task, Task> assert = null)
    {
        await foreach (var (key, executor) in ExecuteAsync())
        {
            if (assert == null)
            {
                await executor.SendAsync(_factoryWithoutResult());
                continue;
            }

            await assert(executor.SendAsync(_factoryWithoutResult()));
        }
    }

    public void ExecuteOnAll()
    {
        AsyncHelpers.RunSync(() => ExecuteOnAllAsync());
    }

    public async Task AssertAllAsync(Action<UniqueDatabaseInstanceKey, TResult> assert)
    {
        await foreach (var (key, result) in GetResultsAsync())
        {
            try
            {
                assert(key, result);
            }
            catch (XunitException e)
            {
                throw new InvalidOperationException($"Assertion failed for '{key}'.", e);
            }
        }
    }

    public async Task AssertAnyAsync(Action<UniqueDatabaseInstanceKey, TResult> assert)
    {
        var assertionFailures = new List<InvalidOperationException>();

        await foreach (var (key, result) in GetResultsAsync())
        {
            try
            {
                assert(key, result);
                return;
            }
            catch (XunitException e)
            {
                assertionFailures.Add(new InvalidOperationException($"Assertion failed for '{key}'.", e));
            }
        }

        throw new AggregateException(assertionFailures);
    }

    public void AssertAll(Action<UniqueDatabaseInstanceKey, TResult> assert)
    {
        AsyncHelpers.RunSync(() => AssertAllAsync(assert));
    }

    public void AssertAny(Action<UniqueDatabaseInstanceKey, TResult> assert)
    {
        AsyncHelpers.RunSync(() => AssertAnyAsync(assert));
    }

    private async IAsyncEnumerable<(UniqueDatabaseInstanceKey Key, TResult Result)> GetResultsAsync()
    {
        _databaseRecord ??= await _executor.Server.SendAsync(new GetDatabaseRecordOperation(_executor._databaseName));
        if (_databaseRecord.IsSharded)
        {
            foreach (var shardNumber in _databaseRecord.Sharding.Shards.Keys)
            {
                var shardTopology = _databaseRecord.Sharding.Shards[shardNumber];

                foreach (var (nKey, nExecutor) in GetExecutors(shardTopology))
                {
                    var shardKey = nKey.ForShard(shardNumber);
                    var shardExecutor = nExecutor.ForShard(shardNumber);

                    yield return (shardKey, await shardExecutor.SendAsync(_factoryWithResult()));
                }
            }

            yield break;
        }

        foreach (var (key, executor) in GetExecutors(_databaseRecord.Topology))
        {
            yield return (key, await executor.SendAsync(_factoryWithResult()));
        }
    }

    private async IAsyncEnumerable<(UniqueDatabaseInstanceKey Key, MaintenanceOperationExecutor Executor)> ExecuteAsync()
    {
        _databaseRecord ??= await _executor.Server.SendAsync(new GetDatabaseRecordOperation(_executor._databaseName));
        if (_databaseRecord.IsSharded)
        {
            foreach (var shardToTopology in _databaseRecord.Sharding.Shards)
            {
                var shardTopology = shardToTopology.Value;

                foreach (var (nKey, nExecutor) in GetExecutors(shardTopology))
                {
                    var shardKey = nKey.ForShard(shardToTopology.Key);
                    var shardExecutor = nExecutor.ForShard(shardToTopology.Key);

                    yield return (shardKey, shardExecutor);
                }
            }

            yield break;
        }

        foreach (var (key, executor) in GetExecutors(_databaseRecord.Topology))
        {
            yield return (key, executor);
        }
    }

    private IEnumerable<(UniqueDatabaseInstanceKey Key, MaintenanceOperationExecutor Executor)> GetExecutors(DatabaseTopology topology)
    {
        foreach (string member in topology.Members)
        {
            var key = new UniqueDatabaseInstanceKey(member);
            var executor = _executor.ForNode(member);

            yield return (key, executor);
        }
    }
}

public interface IMaintenanceOperationExecutorActionTester
{
    Task ExecuteOnAllAsync(Func<Task, Task> assert = null);

    void ExecuteOnAll();
}

public interface IMaintenanceOperationExecutorReadTester<TResult>
{
    Task AssertAllAsync(Action<UniqueDatabaseInstanceKey, TResult> assert);

    void AssertAll(Action<UniqueDatabaseInstanceKey, TResult> assert);

    Task AssertAnyAsync(Action<UniqueDatabaseInstanceKey, TResult> assert);

    void AssertAny(Action<UniqueDatabaseInstanceKey, TResult> assert);

}
