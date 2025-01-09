﻿using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Indexes
{
    public class BoostingDuringIndexing : RavenTestBase
    {
        public BoostingDuringIndexing(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        private class Account
        {
            public string Name { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   FirstName = user.FirstName.Boost(3),
                                   user.LastName
                               };
            }
        }

        private class UsersAndAccounts : AbstractMultiMapIndexCreationTask<UsersAndAccounts.Result>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public UsersAndAccounts()
            {
                AddMap<User>(users =>
                             from user in users
                             select new { Name = user.FirstName }
                    );
                AddMap<Account>(accounts =>
                                from account in accounts
                                select new { account.Name }.Boost(3)
                    );
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanBoostFullDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UsersAndAccounts().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Oren",
                    });

                    session.Store(new Account()
                    {
                        Name = "Oren",
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<UsersAndAccounts.Result, UsersAndAccounts>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Oren")
                        .OrderByScore()
                        .As<object>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                    Assert.IsType<Account>(results[0]);
                    Assert.IsType<User>(results[1]);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "Field boosting is not supported")]
        public void CanGetBoostedValues(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Oren",
                        LastName = "Eini"
                    });

                    session.Store(new User
                    {
                        FirstName = "Ayende",
                        LastName = "Rahien"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User, UsersByName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende" || x.LastName == "Eini")
                        .ToList();

                    Assert.Equal("Ayende", users[0].FirstName);
                    Assert.Equal("Oren", users[1].FirstName);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, SearchEngineMode = RavenSearchEngineMode.All)]
        public void QueryWillThrowWhenIndexHasBoostingAndAutomaticallySortingByScoreIsNotDisable(Options options)
        {
            using var store = GetDocumentStore(options);
            using var session = store.OpenSession();
            session.Store(new User()
            {
                FirstName = "Maciej",
                LastName = "Kowalski"
            });
            session.SaveChanges();
            new UsersByName().Execute(store);
            Indexes.WaitForIndexing(store);

            var ex = Assert.Throws<NotSupportedInShardingException>(() => session.Query<User, UsersByName>().ToList());
            Assert.Contains($"Ordering by score is not supported in sharding. You received this exception because your index has boosting, and we attempted to sort the results since the configuration `{RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)}` is enabled or, when you used `vector.search` method in the query when having `{RavenConfiguration.GetKey(i => i.Indexing.CoraxVectorSearchOrderByScoreAutomatically)}` enabled.",
                ex.Message);
        }
    }
}
