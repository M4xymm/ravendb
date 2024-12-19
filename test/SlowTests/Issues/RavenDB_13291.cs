﻿using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Voron.Data;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13291 : RavenTestBase
    {
        public RavenDB_13291(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanMigrateTablesWithCounterWord()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "northwind.ravendb-snapshot");

            ExtractFile(fullBackupPath);

            using (var store = GetDocumentStore())
            {
                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    var stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfCounterEntries);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var o = await session.LoadAsync<object>("LoginCounters/1");

                        Assert.NotNull(o);

                        var d = await session.LoadAsync<object>("downloads/1");

                        Assert.NotNull(d);

                        var details = await store.Operations
                            .ForDatabase(databaseName)
                            .SendAsync(new GetCountersOperation("downloads/1", returnFullResults: true));

                        Assert.Equal(1, details.Counters.Count);
                        Assert.Equal("NumberOfDownloads", details.Counters[0].CounterName);
                        Assert.Equal(1, details.Counters[0].TotalValue);
                    }

                    var db = await GetDatabase(databaseName);

                    using (var tx = db.DocumentsStorage.Environment.ReadTransaction())
                    {
                        Assert.Null(tx.ReadTree("CounterKeys"));
                        Assert.Null(tx.ReadTree("AllCountersEtags", RootObjectType.FixedSizeTree));
                        Assert.Null(tx.ReadTree("Counters.Tombstones", RootObjectType.Table));

                        Assert.Null(tx.ReadTree("Collection.Counters.downloads", RootObjectType.Table));
                        Assert.Null(tx.ReadTree("Collection.Counters.logincounters", RootObjectType.Table));

                        Assert.NotNull(tx.ReadTree("Collection.Documents.logincounters", RootObjectType.Table));
                    }
                }
            }
        }

        private static void ExtractFile(string path)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_13291.collection-with-counter-word-4.1.5-nightly.ravendb-snapshot"))
            {
                stream.CopyTo(file);
            }
        }
    }
}
