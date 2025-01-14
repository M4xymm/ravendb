﻿using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5730 : ReplicationTestBase
    {
        public RavenDB_5730(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Whitespace_at_the_beginning_of_replication_destination_url_should_not_cause_issues(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var url = " " + storeB.Urls.First();
                await DoReplicationTest(storeA, storeB, url);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Whitespace_at_the_end_of_replication_destination_url_should_not_cause_issues(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var url = storeB.Urls.First() + " ";
                await DoReplicationTest(storeA, storeB, url);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Whitespace_at_the_beginning_and_end_of_replication_destination_url_should_not_cause_issues(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var url = " " + storeB.Urls.First() + " ";
                await DoReplicationTest(storeA, storeB, url);
            }
        }       

        private async Task DoReplicationTest(DocumentStore storeA, DocumentStore storeB, string url)
        {
            var watcher = new ExternalReplication(storeB.Database, "Connection");

             await AddWatcherToReplicationTopology(storeA, watcher, new[] { url }).ConfigureAwait(false);
            
            using (var session = storeA.OpenSession())
            {
                session.Store(new User {Name = "foo/bar"}, "foo-id");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(storeB, "foo-id"));
        }
    }
}
