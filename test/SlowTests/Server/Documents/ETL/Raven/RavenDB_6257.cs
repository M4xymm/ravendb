﻿using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_6257 : RavenTestBase
    {
        public RavenDB_6257(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task Collection_specific_etl_process_is_aware_of_processed_tombstones()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: null);

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone.Reset();

                using (var session = src.OpenAsyncSession())
                {
                    session.Delete("users/1-A");
                    session.Delete("orders/1-A");
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var db = await GetDatabase(src.Database);

                var etlProcess = db.EtlLoader;

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    var tombstoneEtags = etlProcess.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);

                    Assert.Equal(tombstones.First(x => x.Collection.CompareTo("Users") == 0).Etag, tombstoneEtags["Users"]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task All_docs_etl_process_is_aware_of_processed_tombstones()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, collections: new string[0], script: null, applyToAllDocuments: true);

                var etlDone = Etl.WaitForEtlToComplete(src, (n, s) => s.LoadSuccesses >= 4); // 2 docs and 2 HiLos

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone = Etl.WaitForEtlToComplete(src, (n, s) => s.LoadSuccesses >= 6);

                using (var session = src.OpenAsyncSession())
                {
                    session.Delete("users/1-A");
                    session.Delete("orders/1-A");
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var db = await GetDatabase(src.Database);

                var etlProcess = db.EtlLoader;

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue).ToList();

                    var tombstoneEtags = etlProcess.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);

                    Assert.Equal(tombstones.Max(x => x.Etag), tombstoneEtags[Constants.Documents.Collections.AllDocumentsCollection]);
                }
            }
        }
    }
}
