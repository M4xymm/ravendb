﻿using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_12800 : RavenTestBase
    {
        public RavenDB_12800(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [InlineData(@"
            var doc = loadToUsers(this);

            var attachments = getAttachments();

            for (var i = 0; i < attachments.length; i++) {
                doc.addAttachment(loadAttachment(attachments[i].Name));
            }
        ")]
        [InlineData(null)]
        public async Task Should_stop_batch_if_size_limit_exceeded(string script)
        {
            using (var src = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxBatchSize)] = "5"
            }))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenAsyncSession())
                {
                    for (int i = 0; i < 6; i++)
                    {
                        var user = new User();
                        await session.StoreAsync(user);

                        var r = new Random(i);

                        var bytes = new byte[1024 * 1024 * 1];

                        r.NextBytes(bytes);

                        session.Advanced.Attachments.Store(user, "my-attachment", new MemoryStream(bytes));
                    }

                    await session.SaveChangesAsync();
                }

                Etl.AddEtl(src, dest, "Users", script: script);

                var etlDone = Etl.WaitForEtlToComplete(src, (n, s) => s.LoadSuccesses >= 5);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var database = await GetDatabase(src.Database);

                var etlProcess = (RavenEtl)database.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                Assert.Contains("Stopping the batch because maximum batch size limit was reached (5 MBytes)", stats.Select(x => x.BatchTransformationCompleteReason).ToList());

                etlDone = Etl.WaitForEtlToComplete(src, (n, s) => s.LoadSuccesses >= 6);

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
            }
        }
    }
}
