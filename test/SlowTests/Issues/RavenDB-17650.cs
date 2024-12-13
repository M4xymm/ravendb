﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17650 : ClusterTestBase
    {
        public RavenDB_17650(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-21556")]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Should_Retry_When_DatabaseDisabledException_Was_Thrown(Options options)
        {
            using var store = GetDocumentStore(new Options(options)
            {
                ReplicationFactor = 1,
                RunInMemory = false
            });

            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker"));

            // disable database
            var disableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            Assert.True(disableSucceeded.Success);
            Assert.True(disableSucceeded.Disabled);

            var cts = new CancellationTokenSource();
            var failMre = new AsyncManualResetEvent();
            worker.OnSubscriptionConnectionRetry += e =>
            {
                if (e is DatabaseDisabledException)
                {
                    failMre.Set();
                }
            };
            var successMre = new AsyncManualResetEvent();
            var _ = worker.Run(batch =>
            {
                successMre.Set();
            }, cts.Token);

            //enable database
            Assert.True(await failMre.WaitAsync(TimeSpan.FromSeconds(15)), "Subscription didn't fail as expected.");
            var enableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
            Assert.False(enableSucceeded.Disabled);
            Assert.True(enableSucceeded.Success);
            Assert.True(await successMre.WaitAsync(TimeSpan.FromSeconds(15)), "Subscription didn't success as expected.");
        }

        [RavenMultiplatformTheory(RavenTestCategory.Subscriptions, RavenArchitecture.X64)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_Retry_When_AllTopologyNodesDownException_Was_Thrown(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, shouldRunInMemory: false);
            var subscriptionLog = new List<(DateTime, string)>();
            subscriptionLog.Add((DateTime.UtcNow, $"Start running on: {string.Join(", ", nodes.Select(x => $"[{x.ServerStore.NodeTag}: {x.WebUrl}]"))}"));
            using var store = GetDocumentStore(new Options(options)
            {
                ReplicationFactor = 2,
                RunInMemory = false,
                Server = leader
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
            });

            // dispose nodes
            var result0 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[0]);
            var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[1]);

            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var failMre = new AsyncManualResetEvent();
            worker.OnSubscriptionConnectionRetry += e =>
            {
                subscriptionLog.Add((DateTime.UtcNow, $"OnSubscriptionConnectionRetry: {e}"));
                if (e is AllTopologyNodesDownException)
                {
                    failMre.Set();
                }
            };
            worker.OnEstablishedSubscriptionConnection += () =>
            {
                subscriptionLog.Add((DateTime.UtcNow, $"OnEstablishedSubscriptionConnection: {((IPEndPoint)worker?._tcpClient?.Client?.RemoteEndPoint)?.Address.ToString()}"));
            };
            worker.OnUnexpectedSubscriptionError += ex =>
            {
                subscriptionLog.Add((DateTime.UtcNow, $"OnUnexpectedSubscriptionError: {ex}"));
            };
            var successMre = new AsyncManualResetEvent();
            var _ = worker.Run(batch =>
            {
                successMre.Set();
            }, cts.Token);

            //revive node
            Assert.True(await failMre.WaitAsync(TimeSpan.FromSeconds(15)), "Subscription didn't fail as expected.");
            var revivedNodes = new List<RavenServer>();
            var t = Task.Run(() => revivedNodes.Add(ReviveNode(result0.DataDirectory, result0.Url)), cts.Token);
            var tt = Task.Run(() => revivedNodes.Add(ReviveNode(result1.DataDirectory, result1.Url)), cts.Token);
            await Task.WhenAll(t, tt);

            await WaitForRehabAndAssert(revivedNodes, store, options, subscriptionLog);

            if (await successMre.WaitAsync(TimeSpan.FromSeconds(15)) == false)
            {
                subscriptionLog.Add((DateTime.UtcNow, $"Could not reconnect subscription on {result0.Url} & {result1.Url}"));

                if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                {
                    Dictionary<string, List<string>> relevantDatabases = GetRelevantShardedDatabasesForNode(revivedNodes, store);
                    foreach (var node in revivedNodes)
                    {
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            try
                            {
                                foreach (var shard in relevantDatabases[node.ServerStore.NodeTag])
                                {

                                    var json = node.ServerStore.Cluster.ReadDatabaseTopologyForShard(context, store.Database, ShardHelper.GetShardNumberFromDatabaseName(shard)).ToJson();
                                    LogDatabaseTopologyOnFailure(context, json, subscriptionLog, store.Database, node);
                                }
                            }
                            catch (Exception e)
                            {
                                LogDatabaseTopologyErrorOnFailure(subscriptionLog, store.Database, node, e);
                            }
                        }

                    }
                }
                else
                {
                    foreach (var node in revivedNodes)
                    {
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            try
                            {
                                var json = node.ServerStore.Cluster.ReadDatabaseTopology(context, store.Database).ToJson();

                                LogDatabaseTopologyOnFailure(context, json, subscriptionLog, store.Database, node);
                            }
                            catch (Exception e)
                            {
                                LogDatabaseTopologyErrorOnFailure(subscriptionLog, store.Database, node, e);
                            }
                        }
                        subscriptionLog.Add((DateTime.UtcNow, $"GetClusterTopology for ['{node.ServerStore.NodeTag}', {node.WebUrl}]{Environment.NewLine}{node.ServerStore.GetClusterTopology()}"));
                    }
                }

                List<ClusterObserverLogEntry> logs = new List<ClusterObserverLogEntry>();
                await ActionWithLeader((l) =>
        {
            var x = l.ServerStore.Observer.ReadDecisionsForDatabase();
            if (x.List == null)
                return Task.CompletedTask;
            logs = x.List.ToList();

            return Task.CompletedTask;
        }, revivedNodes);
                var sb = new StringBuilder();
                if (logs == null)
                {
                    sb.AppendLine($"ReadDecisionsForDatabase was null");
                }
                else if (logs.Count == 0)
                    sb.AppendLine($"ReadDecisionsForDatabase was empty");
                else
                {
                    sb.AppendLine(
                        $"Cluster Observer Log Entries:{Environment.NewLine}{string.Join(Environment.NewLine, logs.Select(x => x.ToString()))}");
                }

                subscriptionLog.Add((DateTime.UtcNow, sb.ToString()));
                Assert.Fail(string.Join(Environment.NewLine, subscriptionLog.Select(x => $"#### {x.Item1.GetDefaultRavenFormat()}: {x.Item2}")));
            }
        }

        private static void LogDatabaseTopologyErrorOnFailure(List<(DateTime, string)> subscriptionLog, string database, RavenServer node, Exception e)
        {
            subscriptionLog.Add((DateTime.UtcNow, $"Could not ReadDatabaseTopology for ['{database}' @ '{node.ServerStore.NodeTag}', {node.WebUrl}]{Environment.NewLine}{e}"));
        }

        private static void LogDatabaseTopologyOnFailure(TransactionOperationContext context, DynamicJsonValue json, List<(DateTime, string)> subscriptionLog, string database, RavenServer node)
        {
            using var bjro = context.ReadObject(json, "ReadDatabaseTopology", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            subscriptionLog.Add((DateTime.UtcNow, $"ReadDatabaseTopology for ['{database}' @ '{node.ServerStore.NodeTag}', {node.WebUrl}]{Environment.NewLine}{bjro}"));
        }

        private static async Task WaitForRehabAndAssert(List<RavenServer> revivedNodes, DocumentStore store, Options options, List<(DateTime, string)> subscriptionLog)
        {
            if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                Dictionary<string, List<string>> relevantDatabases = GetRelevantShardedDatabasesForNode(revivedNodes, store);

                foreach (var node in revivedNodes)
                {
                    var rehabs = await WaitForValueAsync(() => GetRehabsCountForShards(store, subscriptionLog, node, relevantDatabases), expectedVal: 0, interval: 322 * 2);

                    Assert.Equal(0, rehabs);
                }
            }
            else
            {
                foreach (var node in revivedNodes)
                {
                    var rehabs = await WaitForValueAsync(() =>
                    {
                        using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            try
                            {
                                var dbTopology = node.ServerStore.Cluster.ReadDatabaseTopology(context, store.Database);
                                LogTopologyToSubscriptionLog(subscriptionLog, context, store.Database, dbTopology.ToJson(), node);
                                return dbTopology.Rehabs.Count;

                            }
                            catch (Exception e)
                            {
                                AddErrorToSubscriptionLog(subscriptionLog, node, e);
                                return int.MaxValue;
                            }
                        }
                    }, expectedVal: 0, interval: 322 * 2);

                    Assert.Equal(0, rehabs);
                }
            }
        }

        private static void LogTopologyToSubscriptionLog(List<(DateTime, string)> subscriptionLog, TransactionOperationContext context, string database, DynamicJsonValue json, RavenServer node)
        {
            using var bjro = context.ReadObject(json, $"ReadDatabaseTopology_{database}", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            subscriptionLog.Add((DateTime.UtcNow,
                $"ReadDatabaseTopology in WaitForValueAsync for ['{database}' @ '{node.ServerStore.NodeTag}', {node.WebUrl}]{Environment.NewLine}{bjro}"));
        }

        private static int GetRehabsCountForShards(DocumentStore store, List<(DateTime, string)> subscriptionLog, RavenServer node, Dictionary<string, List<string>> relevantDatabases)
        {
            using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                try
                {
                    if (relevantDatabases.TryGetValue(node.ServerStore.NodeTag, out List<string> shards) == false)
                        return int.MaxValue;

                    var rehabsForShards = 0;
                    foreach (var shard in shards)
                    {
                        var dbTopology = node.ServerStore.Cluster.ReadDatabaseTopologyForShard(context, store.Database, ShardHelper.GetShardNumberFromDatabaseName(shard));
                        LogTopologyToSubscriptionLog(subscriptionLog, context, shard, dbTopology.ToJson(), node);
                        rehabsForShards += dbTopology.Rehabs.Count;
                    }

                    return rehabsForShards;
                }
                catch (Exception e)
                {
                    AddErrorToSubscriptionLog(subscriptionLog, node, e);
                    return int.MaxValue;
                }
            }
        }

        private static void AddErrorToSubscriptionLog(List<(DateTime, string)> subscriptionLog, RavenServer node, Exception e)
        {
            subscriptionLog.Add((DateTime.UtcNow,
                $"Could not ReadDatabaseTopology in WaitForValueAsync for ['{node.ServerStore.NodeTag}', {node.WebUrl}]{Environment.NewLine}{e}"));
        }

        private static Dictionary<string, List<string>> GetRelevantShardedDatabasesForNode(List<RavenServer> revivedNodes, DocumentStore store)
        {
            var relevantDatabases = new Dictionary<string, List<string>>();
            foreach (var node in revivedNodes)
            {
                using (node.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                using (var databaseRecord = node.ServerStore.Cluster.ReadRawDatabaseRecord(context, store.Database))
                {
                    if (databaseRecord == null)
                        continue;

                    foreach (var shard in databaseRecord.Topologies)
                    {
                        if (shard.Topology.RelevantFor(node.ServerStore.NodeTag))
                        {
                            if (relevantDatabases.ContainsKey(node.ServerStore.NodeTag))
                            {
                                var shards = relevantDatabases[node.ServerStore.NodeTag];
                                shards.Add(shard.Name);
                                relevantDatabases[node.ServerStore.NodeTag] = shards;
                            }
                            else
                            {
                                relevantDatabases.Add(node.ServerStore.NodeTag, new List<string>() { shard.Name });
                            }
                        }
                    }
                }
            }

            return relevantDatabases;
        }

        private RavenServer ReviveNode(string nodeDataDirectory, string nodeUrl)
        {
            var cs = new Dictionary<string, string>(DefaultClusterSettings);
            cs[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = nodeUrl;
            return GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = nodeDataDirectory,
                CustomSettings = cs
            });
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_Throw_DatabaseDisabledException_When_MaxErroneousPeriod_Was_Passed(Options options)
        {
            using var store = GetDocumentStore(new Options(options)
            {
                ReplicationFactor = 1,
                RunInMemory = false
            });

            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                MaxErroneousPeriod = TimeSpan.Zero
            });

            // disable database
            var disableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            Assert.True(disableSucceeded.Success);
            Assert.True(disableSucceeded.Disabled);

            var cts = new CancellationTokenSource();

            var aggregateException = await Assert.ThrowsAsync<AggregateException>(() => worker.Run(batch => { }, cts.Token));
            var actualExceptionWasThrown = false;
            var subscriptionInvalidStateExceptionWasThrown = false;
            foreach (var e in aggregateException.InnerExceptions)
            {
                if (e is SubscriptionInvalidStateException)
                {
                    subscriptionInvalidStateExceptionWasThrown = true;
                }
                if (e is DatabaseDisabledException)
                {
                    actualExceptionWasThrown = true;
                }

                if (subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown)
                    break;
            }
            Assert.True(subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown);
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_Throw_DatabaseDisabledException_When_MaxErroneousPeriod_Was_Passed_DuringSubscriptionInProgress(Options options)
        {
            using var store = GetDocumentStore(new Options(options)
            {
                ReplicationFactor = 1,
                RunInMemory = false
            });

            string id = "User/33-A";

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });

                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                MaxErroneousPeriod = TimeSpan.Zero
            });

            var cts = new CancellationTokenSource();
            var mre = new AsyncManualResetEvent();

            var t = worker.Run(batch => { mre.Set(); }, cts.Token);
            await mre.WaitAsync(cts.Token);

            // disable database
            var disableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            Assert.True(disableSucceeded.Success);
            Assert.True(disableSucceeded.Disabled);

            var aggregateException = await Assert.ThrowsAsync<AggregateException>(() => t);

            var actualExceptionWasThrown = false;
            var subscriptionInvalidStateExceptionWasThrown = false;
            foreach (var e in aggregateException.InnerExceptions)
            {
                if (e is SubscriptionInvalidStateException)
                {
                    subscriptionInvalidStateExceptionWasThrown = true;
                }
                if (e is DatabaseDisabledException)
                {
                    actualExceptionWasThrown = true;
                }

                if (subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown)
                    break;
            }
            Assert.True(subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown);
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-21554")]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Should_Throw_AllTopologyNodesDownException_When_MaxErroneousPeriod_Was_Passed(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, shouldRunInMemory: false);
            using var store = GetDocumentStore(new Options(options)
            {
                ReplicationFactor = 2,
                RunInMemory = false,
                Server = leader
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                MaxErroneousPeriod = TimeSpan.Zero
            });

            // dispose nodes
            var result0 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[0]);
            var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(nodes[1]);

            var cts = new CancellationTokenSource();

            var aggregateException = await Assert.ThrowsAsync<AggregateException>(() => worker.Run(batch => { }, cts.Token));
            var actualExceptionWasThrown = false;
            var subscriptionInvalidStateExceptionWasThrown = false;
            foreach (var e in aggregateException.InnerExceptions)
            {
                if (e is SubscriptionInvalidStateException)
                {
                    subscriptionInvalidStateExceptionWasThrown = true;
                }
                if (e is AllTopologyNodesDownException)
                {
                    actualExceptionWasThrown = true;
                }

                if (subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown)
                    break;
            }
            Assert.True(subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown);
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

        }
    }
}
