﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13644 : RavenTestBase
    {
        public RavenDB_13644(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes(Options options)
        {
            CanLoadCompareExchangeInIndexes<Index_With_CompareExchange>(options);
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_JavaScript(Options options)
        {
            CanLoadCompareExchangeInIndexes<Index_With_CompareExchange_JavaScript>(options);
        }

        private void CanLoadCompareExchangeInIndexes<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation())
                    .ExecuteOnAll();

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAll((_, terms) =>  Assert.Equal(0, terms.Length));
                
                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "CF", ExternalId = "companies/cf" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    // waiting for the value to be applied in the cluster using returned header with raft index from previous save changes
                    var companies = session
                        .Query<Company>()
                        .ToList();

                    Assert.Equal(1, companies.Count);
                }

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((key, result) =>
                    {
                        switch (key.ShardNumber)
                        {
                            case 0:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            case 2:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            default:
                                {
                                    Assert.True(result.IsStale);
                                    Assert.Equal(1, result.StalenessReasons.Count);
                                    Assert.Contains("There are still some documents to process from collection", result.StalenessReasons[0]);
                                    break;
                                }
                        }
                    });

                store.Maintenance.ForTesting(() => new StartIndexingOperation())
                    .ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAll((_, terms) =>  Assert.Equal(0, terms.Length));

                store.Maintenance.ForTesting(() => new StopIndexingOperation())
                    .ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    // waiting for the value to be applied in the cluster using returned header with raft index from previous save changes
                    var companies = session
                        .Query<Company>()
                        .ToList();

                    Assert.Equal(1, companies.Count);
                }

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((key, result) =>
                    {
                        switch (key.ShardNumber)
                        {
                            case 0:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            case 2:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            default:
                                {
                                    Assert.True(result.IsStale);
                                    Assert.Equal(1, result.StalenessReasons.Count);
                                    Assert.Contains("There are still some compare exchange references to process for collection", result.StalenessReasons[0]);
                                    break;
                                }
                        }
                    });

                store.Maintenance.ForTesting(() => new StartIndexingOperation())
                    .ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAny((_, terms) =>
                {
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("torun", terms);
                });

                store.Maintenance.ForTesting(() => new StopIndexingOperation())
                    .ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "HR", ExternalId = "companies/hr" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    // waiting for the value to be applied in the cluster using returned header with raft index from previous save changes
                    var companies = session
                        .Query<Company>()
                        .ToList();

                    Assert.Equal(2, companies.Count);
                }

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((key, result) =>
                    {
                        switch (key.ShardNumber)
                        {
                            case 0:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            case 2:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            default:
                                {
                                    Assert.True(result.IsStale);
                                    Assert.Equal(2, result.StalenessReasons.Count);
                                    Assert.Contains("There are still some documents to process from collection", result.StalenessReasons[0]);
                                    Assert.Contains("There are still some compare exchange references to process for collection", result.StalenessReasons[1]);
                                    break;
                                }
                        }
                    });

                store.Maintenance.ForTesting(() => new StartIndexingOperation())
                    .ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));


                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAny((_, terms) =>
                {
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("torun", terms);
                    Assert.Contains("cesarea", terms);
                });

                store.Maintenance.ForTesting(() => new StopIndexingOperation())
                    .ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    // waiting for the value to be applied in the cluster using returned header with raft index from previous save changes
                    var companies = session
                        .Query<Company>()
                        .ToList();

                    Assert.Equal(2, companies.Count);
                }

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((key, result) =>
                    {
                        switch (key.ShardNumber)
                        {
                            case 0:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            case 2:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            default:
                                {
                                    Assert.True(result.IsStale);
                                    Assert.Equal(1, result.StalenessReasons.Count);
                                    Assert.Contains("There are still some compare exchange references to process for collection", result.StalenessReasons[0]);
                                    break;
                                }
                        }
                    });

                store.Maintenance.ForTesting(() => new StartIndexingOperation())
                    .ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));



                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAny((_, terms) =>
                {
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("torun", terms);
                    Assert.Contains("hadera", terms);
                });

                store.Maintenance.ForTesting(() => new StopIndexingOperation())
                    .ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    // waiting for the value to be applied in the cluster using returned header with raft index from previous save changes
                    var companies = session
                        .Query<Company>()
                        .ToList();

                    Assert.Equal(2, companies.Count);
                }

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((key, result) =>
                    {
                        switch (key.ShardNumber)
                        {
                            case 0:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            case 2:
                                {
                                    Assert.False(result.IsStale);
                                    break;
                                }
                            default:
                                {
                                    Assert.True(result.IsStale);
                                    Assert.Equal(1, result.StalenessReasons.Count);
                                    Assert.Contains("There are still some compare exchange tombstone references to process for collection", result.StalenessReasons[0]);
                                    break;
                                }
                        }
                    });

                store.Maintenance.ForTesting(() => new StartIndexingOperation())
                    .ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAny((_, terms) =>
                {
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("torun", terms);
                });

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    // waiting for the value to be applied in the cluster using returned header with raft index from previous save changes
                    var companies = session
                        .Query<Company>()
                        .ToList();

                    Assert.Equal(2, companies.Count);
                }

                try
                {
                    Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));
                }
                catch
                {
                    store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                        .AssertAll((_, result) => Assert.False(result.IsStale));
                }

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAll((_, result) => Assert.False(result.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAny((_, terms) =>
                {
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("torun", terms);
                    Assert.Contains("tel aviv", terms);
                });
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_Simple(Options options)
        {
            CanLoadCompareExchangeInIndexes_Simple<Index_With_CompareExchange_Simple>(options);
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_Simple_JavaScript(Options options)
        {
            CanLoadCompareExchangeInIndexes_Simple<Index_With_CompareExchange_Simple_JavaScript>(options);
        }

        private void CanLoadCompareExchangeInIndexes_Simple<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAll((_, terms) =>  Assert.Equal(0, terms.Length));

                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "CF", ExternalId = "companies/cf" }, "foo/bar");

                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var c = session.Load<Company>("foo/bar"); // ensure the document is propagated to the database
                    Assert.NotNull(c);
                }
                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAny((_, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(1, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);
                    });
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAll((_, terms) =>  Assert.Equal(0, terms.Length));

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", "Torun");

                    session.SaveChanges();
                }

                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAny((_, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(1, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);
                    });
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAny((_, terms) =>
                {
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("torun", terms);
                });

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "HR", ExternalId = "companies/hr" }, "foo/bar/2");
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", "Cesarea");

                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var c = session.Load<Company>("foo/bar/2"); // ensure the document is propagated to the database
                    Assert.NotNull(c);
                }

                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAny((_, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(2, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);
                        Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);
                    });
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("cesarea", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>("companies/hr");
                    value.Value = "Hadera";

                    session.SaveChanges();
                }

                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAny((_, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(1, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);
                    });
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                
                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("hadera", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAny((_, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(1, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some compare exchange tombstone references to process for collection", staleness.StalenessReasons[0]);
                    });
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);
                
                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", "Tel Aviv");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_Simple_Array(Options options)
        {
            CanLoadCompareExchangeInIndexes_Simple_Array<Index_With_CompareExchange_Simple_Array>(options);
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_Simple_Array_JavaScript(Options options)
        {
            CanLoadCompareExchangeInIndexes_Simple_Array<Index_With_CompareExchange_Simple_Array_JavaScript>(options);
        }

        private void CanLoadCompareExchangeInIndexes_Simple_Array<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(0, terms.Length);

                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company_Array { Name = "CF", ExternalIds = new[] { "companies/cf" } });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", "Torun");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company_Array { Name = "HR", ExternalIds = new[] { "companies/hr" } });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", "Cesarea");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("cesarea", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>("companies/hr");
                    value.Value = "Hadera";

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("hadera", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange tombstone references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", "Tel Aviv");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);

                // add doc and 2x compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company_Array { Name = "HR", ExternalIds = new[] { "companies/ms", "companies/wf" } });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/ms", "MS");
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/wf", "WF");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAll((_, staleness) => Assert.False(staleness.IsStale));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Cities", null));
                Assert.Equal(4, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);
                Assert.Contains("ms", terms);
                Assert.Contains("wf", terms);
            }
        }


        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_Query(Options options)
        {
            CanLoadCompareExchangeInIndexes_Query<Index_With_CompareExchange>(options);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_Query_JavaScript(Options options)
        {
            CanLoadCompareExchangeInIndexes_Query<Index_With_CompareExchange_JavaScript>(options);
        }

        private void CanLoadCompareExchangeInIndexes_Query<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "CF", ExternalId = "companies/cf" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
                
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }
                
                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
                
                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
                
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }
                
                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
                
                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "HR", ExternalId = "companies/hr" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
                
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Tel Aviv", terms);

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanLoadCompareExchangeInIndexes_TimeSeries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TimeSeries_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAllAsync((key, terms) => Assert.Equal(0, terms.Length));
                
                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "Heartrate")
                        .Append(DateTime.Now, new double[] { 3 }, company.ExternalId);

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) => 
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.Contains("There are still some time series items to process from collection", staleness.StalenessReasons[0]);
                });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));
                
                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null))
                    .AssertAllAsync((key, terms) => Assert.Equal(0, terms.Length));
                
                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) => 
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);
                });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null))
                    .AssertAnyAsync((key, terms) =>
                {
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("torun", terms);
                });


                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate")
                        .Append(DateTime.Now, new double[] { 5 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(2, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some time series items to process from collection", staleness.StalenessReasons[0]);
                        Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);
                        
                    });
                
                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));


                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null))
                    .AssertAnyAsync((key, terms) =>
                    {
                        Assert.Equal(2, terms.Length);
                        Assert.Contains("torun", terms);
                        Assert.Contains("cesarea", terms);
                    }); ;

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAnyAsync((key, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(1, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);
                    });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null))
                    .AssertAnyAsync((key, terms) =>
                    {
                        Assert.Equal(2, terms.Length);
                        Assert.Contains("torun", terms);
                        Assert.Contains("hadera", terms);
                    }); ;
                
                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAnyAsync((key, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(1, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some compare exchange tombstone references to process for collection", staleness.StalenessReasons[0]);
                    });


                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null))
                    .AssertAnyAsync((key, terms) =>
                    {
                        Assert.Equal(1, terms.Length);
                        Assert.Contains("torun", terms);
                    });

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                    .AssertAllAsync((key, indexStaleness) => Assert.False(indexStaleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null))
                    .AssertAnyAsync((key, terms) =>
                    {
                        Assert.Equal(2, terms.Length);
                        Assert.Contains("torun", terms);
                        Assert.Contains("tel aviv", terms);
                    }); ;

                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var indexInstance = database.IndexStore.GetIndex(indexName);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(2, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                    {
                        session.Delete("companies/1");

                        session.SaveChanges();
                    }

                    await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromSeconds(5));

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(1, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_TimeSeries_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TimeSeries_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate")
                        .Append(DateTime.Now, new double[] { 3 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    WaitForUserToContinueTheTest(store);
                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.Now, 5, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Tel Aviv", terms);

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanLoadCompareExchangeInIndexes_Counters(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Counters_Index_With_CompareExchange();
                var indexName = index.IndexName;
                await index.ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAllAsync((key, terms) => Assert.Equal(0, terms.Length));

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company {Name = "CF", ExternalId = "companies/cf"};
                    session.Store(company, "companies/cf");
                    session.CountersFor(company).Increment("HeartRate", 3);

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.Contains("There are still some counters to process from collection", staleness.StalenessReasons[0]);
                });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));

                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAllAsync((key, terms) => Assert.Equal(0, terms.Length));

                await store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAllAsync();

                // add compare
                using (var session = store.OpenSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address {City = "Torun"});

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);
                });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));


                await store.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAnyAsync((key, terms) =>
                {
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("torun", terms);
                });

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company {Name = "HR", ExternalId = "companies/hr"};
                    session.Store(company, "companies/hr");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address {City = "Cesarea"});

                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);

                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                    {
                        Assert.True(staleness.IsStale);
                        Assert.Equal(2, staleness.StalenessReasons.Count);
                        Assert.Contains("There are still some counters to process from collection", staleness.StalenessReasons[0]);
                        Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);
                    });
                }
                else
                {
                    if (options.DatabaseMode is RavenDatabaseMode.Single)
                    {
                        await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                        {
                            Assert.True(staleness.IsStale);
                            Assert.Equal(1, staleness.StalenessReasons.Count);
                            Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);
                        });
                        await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                        {
                            Assert.True(staleness.IsStale);
                            Assert.Equal(1, staleness.StalenessReasons.Count);
                            Assert.Contains("There are still some counters to process from collection", staleness.StalenessReasons[0]);
                        });
                    }
                }
                
                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));

                await AssertTerms(store, new []{"torun", "cesarea"});

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }
                
                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);
                });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));

                await AssertTerms(store, new []{"torun", "hadera"});

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAnyAsync((key, staleness) =>
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.Contains("There are still some compare exchange tombstone references to process for collection", staleness.StalenessReasons[0]);
                });

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                await Indexes.WaitForIndexingAsync(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));

                await AssertTerms(store, new []{"torun"});
                
                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                await store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName)).AssertAllAsync((key, staleness) => Assert.False(staleness.IsStale));

                await AssertTerms(store, new []{"torun", "tel aviv"});

                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var indexInstance = database.IndexStore.GetIndex(indexName);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(2, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                    {
                        session.Delete("companies/hr");

                        session.SaveChanges();
                    }

                    await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromSeconds(5));

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(1, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }
                }

                async Task AssertTerms(IDocumentStore dbStore, string[] expectedTerms)
                {
                    if (options.DatabaseMode is RavenDatabaseMode.Single)
                    {
                        await dbStore.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAnyAsync((key, terms) =>
                        {
                            Assert.Equal(expectedTerms.Length, terms.Length);
                            foreach (var expected in expectedTerms)
                                Assert.Contains(expected, terms);
                        });
                    }
                    else
                    {
                        foreach (var expected in expectedTerms)
                            await dbStore.Maintenance.ForTesting(() => new GetTermsOperation(indexName, "City", null)).AssertAnyAsync((key, terms) =>
                            {
                                Assert.Equal(1, terms.Length);
                                Assert.Contains(expected, terms);
                            });
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_Counters_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Counters_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/cf");
                    session.CountersFor(company).Increment("HeartRate", 3);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company, "companies/hr");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Tel Aviv", terms);

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_MapReduce_Query(Options options)
        {
            CanLoadCompareExchangeInIndexes_MapReduce_Query<Index_With_CompareExchange_MapReduce>(options);
        }

        private void CanLoadCompareExchangeInIndexes_MapReduce_Query<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "CF", ExternalId = "companies/cf" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "HR", ExternalId = "companies/hr" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Tel Aviv", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_MapReduce_Counters_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Counters_Index_With_CompareExchange_MapReduce();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/cf");
                    session.CountersFor(company).Increment("HeartRate", 3);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company, "companies/hr");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Tel Aviv", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanLoadCompareExchangeInIndexes_MapReduce_TimeSeries_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TimeSeries_Index_With_CompareExchange_MapReduce();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.Now, new double[] { 3 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.Now, new double[] { 5 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Tel Aviv", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        private class MapIndexResult
        {
            public string City { get; set; }
        }

        private class MapReduceIndexResult
        {
            public string City { get; set; }

            public int Count { get; set; }
        }

        private class Index_With_CompareExchange : AbstractIndexCreationTask<Company>
        {
            public Index_With_CompareExchange()
            {
                Map = companies => from c in companies
                                   let address = LoadCompareExchangeValue<Address>(c.ExternalId)
                                   select new
                                   {
                                       address.City
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Index_With_CompareExchange_MapReduce : AbstractIndexCreationTask<Company, MapReduceIndexResult>
        {
            public Index_With_CompareExchange_MapReduce()
            {
                Map = companies => from c in companies
                                   let address = LoadCompareExchangeValue<Address>(c.ExternalId)
                                   select new
                                   {
                                       address.City,
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class Index_With_CompareExchange_Simple : AbstractIndexCreationTask<Company>
        {
            public Index_With_CompareExchange_Simple()
            {
                Map = companies => from c in companies
                                   let city = LoadCompareExchangeValue<string>(c.ExternalId)
                                   select new
                                   {
                                       City = city
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Index_With_CompareExchange_Simple_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Index_With_CompareExchange_Simple_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    "map('Companies', function (c) { var city = cmpxchg(c.ExternalId); return { City: city };})",
                };

                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class Index_With_CompareExchange_Simple_Array : AbstractIndexCreationTask<Company_Array>
        {
            public Index_With_CompareExchange_Simple_Array()
            {
                Map = companies => from c in companies
                                   let cities = LoadCompareExchangeValue<string>(c.ExternalIds)
                                   select new
                                   {
                                       Cities = cities
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Index_With_CompareExchange_Simple_Array_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Index_With_CompareExchange_Simple_Array_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    "map('Company_Arrays', function (c) { var cities = cmpxchg(c.ExternalIds); return { Cities: cities };})",
                };

                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class Index_With_CompareExchange_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Index_With_CompareExchange_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    "map('Companies', function (c) { var address = cmpxchg(c.ExternalId); return { City: address.City };})",
                };

                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }


        private class TimeSeries_Index_With_CompareExchange : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public TimeSeries_Index_With_CompareExchange()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadCompareExchangeValue<Address>(entry.Tag)
                                  select new
                                  {
                                      address.City
                                  });
            }
        }

        private class Counters_Index_With_CompareExchange : AbstractCountersIndexCreationTask<Company>
        {
            public Counters_Index_With_CompareExchange()
            {
                AddMap(
                    "HeartRate",
                    counters => from counter in counters
                                let address = LoadCompareExchangeValue<Address>(counter.DocumentId)
                                select new
                                {
                                    address.City
                                });
            }
        }

        private class Counters_Index_With_CompareExchange_MapReduce : AbstractCountersIndexCreationTask<Company, MapReduceIndexResult>
        {
            public Counters_Index_With_CompareExchange_MapReduce()
            {
                AddMap(
                    "HeartRate",
                    counters => from counter in counters
                                let address = LoadCompareExchangeValue<Address>(counter.DocumentId)
                                select new
                                {
                                    address.City,
                                    Count = 1
                                });

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class TimeSeries_Index_With_CompareExchange_MapReduce : AbstractTimeSeriesIndexCreationTask<Company, MapReduceIndexResult>
        {
            public TimeSeries_Index_With_CompareExchange_MapReduce()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadCompareExchangeValue<Address>(entry.Tag)
                                  select new
                                  {
                                      address.City,
                                      Count = 1
                                  });

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        public class Company_Array
        {
            public string Id { get; set; }
            public string[] ExternalIds { get; set; }
            public string Name { get; set; }
        }
    }
}
