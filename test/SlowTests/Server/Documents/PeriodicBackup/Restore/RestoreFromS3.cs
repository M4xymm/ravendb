﻿using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public abstract class RestoreFromS3 : CloudBackupTestBase
    {
        private readonly bool _isCustom;
        private readonly string _remoteFolderName;

        protected RestoreFromS3(ITestOutputHelper output, bool isCustom = false) : base(output)
        {
            _isCustom = isCustom;
            _remoteFolderName = GetRemoteFolder(GetType().Name);
        }

        protected async Task can_backup_and_restore_internal(string dbName = null, BackupUploadMode backupUploadMode = BackupUploadMode.Default)
        {
            var s3Settings = GetS3Settings();
            Options options = null;
            if (dbName != null)
            {
                options = new Options()
                {
                    ModifyDatabaseName = s => s + dbName
                };
            }

            using (var store = GetDocumentStore(options: options))
            {
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 100);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings, backupUploadMode: backupUploadMode);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var id = await Backup.GetBackupOperationIdAsync(store, backupTaskId);
                var backupResult = (await store.Maintenance.SendAsync(new GetOperationStateOperation(id))).Result as BackupResult;
                Assert.NotNull(backupResult);
                Assert.True(backupResult.Counters.Processed, "backupResult.Counters.Processed");
                Assert.Equal(1, backupResult.Counters.ReadCount);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                s3Settings.RemoteFolderName = $"{s3Settings.RemoteFolderName}/{status.FolderName}";

                using (Backup.RestoreDatabaseFromCloud(
                    store,
                    new RestoreFromS3Configuration { DatabaseName = databaseName, Settings = s3Settings, DisableOngoingTasks = true },
                    TimeSpan.FromSeconds(60)))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var val = await session.CountersFor("users/1").GetAsync("likes");
                        Assert.Equal(100, val);
                        val = await session.CountersFor("users/2").GetAsync("downloads");
                        Assert.Equal(200, val);
                    }

                    var originalDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var restoredDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                        Assert.DoesNotContain(originalDatabase.DbBase64Id, databaseChangeVector);
                        Assert.Contains($"A:10-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        protected async Task can_backup_and_restore_snapshot_internal(BackupUploadMode backupUploadMode)
        {
            var s3Settings = GetS3Settings();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session
                        .Query<User>()
                        .Where(x => x.Name == "oren")
                        .ToListAsync(); // create an index to backup

                    await session
                        .Query<Order>()
                        .Where(x => x.Freight > 20)
                        .ToListAsync(); // create an index to backup

                    session.CountersFor("users/1").Increment("likes", 100); //create a counter to backup
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupType: BackupType.Snapshot, s3Settings: s3Settings, backupUploadMode: backupUploadMode);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var backupStatus = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";
                s3Settings.RemoteFolderName = $"{s3Settings.RemoteFolderName}/{backupStatus.FolderName}";

                using (Backup.RestoreDatabaseFromCloud(store,
                    new RestoreFromS3Configuration
                    {
                        DatabaseName = restoredDatabaseName,
                        Settings = s3Settings
                    },
                    TimeSpan.FromSeconds(60)))
                {
                    using (var session = store.OpenAsyncSession(restoredDatabaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.NotNull(users["users/1"]);
                        Assert.NotNull(users["users/2"]);
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var val = await session.CountersFor("users/1").GetAsync("likes");
                        Assert.Equal(100, val);
                        val = await session.CountersFor("users/2").GetAsync("downloads");
                        Assert.Equal(200, val);
                    }

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfIndexes);

                    var originalDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var restoredDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(restoredDatabaseName);
                    using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                        Assert.Contains($"A:4-{originalDatabase.DbBase64Id}", databaseChangeVector);
                        Assert.Contains($"A:12-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        protected async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key_internal(BackupUploadMode backupUploadMode)
        {
            var s3Settings = GetS3Settings();
            var result = await Encryption.EncryptedServerAsync();

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = result.Certificates.ServerCertificate.Value,
                ClientCertificate = result.Certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => result.DatabaseName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = NewDataPath()
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings, fullBackupFrequency: null, incrementalBackupFrequency: "0 */6 * * *", backupEncryptionSettings: new BackupEncryptionSettings
                {
                    EncryptionMode = EncryptionMode.UseDatabaseKey
                }, backupUploadMode: backupUploadMode);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var backupStatus = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                var databaseName = $"restored_database-{Guid.NewGuid()}";
                s3Settings.RemoteFolderName = $"{s3Settings.RemoteFolderName}/{backupStatus.FolderName}";

                using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = s3Settings,
                    DatabaseName = databaseName,
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = result.Key,
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/2");
                        Assert.NotNull(users);
                    }
                }
            }
        }

        protected async Task incremental_and_full_check_last_file_for_backup_internal()
        {
            var s3Settings = GetS3Settings();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "user-1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings, fullBackupFrequency: null, incrementalBackupFrequency: "0 */6 * * *");
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "user-2" }, "users/2");

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var backupStatus = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                string lastFileToRestore;
                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
                using (var client = new RavenAwsS3Client(s3Settings, DefaultConfiguration, cancellationToken: cts.Token))
                {
                    var fullBackupPath = $"{s3Settings.RemoteFolderName}/{backupStatus.FolderName}";
                    lastFileToRestore = (await client.ListObjectsAsync(fullBackupPath, string.Empty, false)).FileInfoDetails.Last().FullPath;
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "user-3" }, "users/3");

                    await session.SaveChangesAsync();
                }

                lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                s3Settings.RemoteFolderName = $"{s3Settings.RemoteFolderName}/{backupStatus.FolderName}";

                using (Backup.RestoreDatabaseFromCloud(store,
                    new RestoreFromS3Configuration
                    {
                        Settings = s3Settings,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = lastFileToRestore
                    }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/2");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/3");
                        Assert.Null(users);
                    }
                }
            }
        }

        protected async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key_internal()
        {
            var s3Settings = GetS3Settings();
            var result = await Encryption.EncryptedServerAsync();

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = result.Certificates.ServerCertificate.Value,
                ClientCertificate = result.Certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => result.DatabaseName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = NewDataPath()
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(s3Settings: s3Settings, fullBackupFrequency: null, incrementalBackupFrequency: "0 */6 * * *", backupEncryptionSettings: new BackupEncryptionSettings
                {
                    Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                    EncryptionMode = EncryptionMode.UseProvidedKey
                });
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var backupStatus = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                s3Settings.RemoteFolderName = $"{s3Settings.RemoteFolderName}/{backupStatus.FolderName}";

                using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = s3Settings,
                    DatabaseName = databaseName,
                    EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/2");
                        Assert.NotNull(users);
                    }
                }
            }
        }

        protected async Task snapshot_encrypted_db_and_restore_to_encrypted_DB_internal(BackupUploadMode backupUploadMode)
        {
            var result = await Encryption.EncryptedServerAsync();

            var s3Settings = GetS3Settings();
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = result.Certificates.ServerCertificate.Value,
                ClientCertificate = result.Certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => result.DatabaseName,
                ModifyDatabaseRecord = record => record.Encrypted = true
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupType: BackupType.Snapshot, s3Settings: s3Settings, backupUploadMode: backupUploadMode);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                var backupStatus = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backupTaskId)).Status;
                var databaseName = GetDatabaseName();
                var subfolderS3Settings = GetS3Settings(backupStatus.FolderName);

                s3Settings.RemoteFolderName = $"{s3Settings.RemoteFolderName}/{backupStatus.FolderName}";
                using (Backup.RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = s3Settings,
                    DatabaseName = databaseName,
                    EncryptionKey = result.Key,
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        EncryptionMode = EncryptionMode.UseDatabaseKey
                    }
                }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                    }
                }
            }
        }

        public S3Settings GetS3Settings(string subPath = null, [CallerMemberName] string caller = null)
        {
            var s3Settings = _isCustom ? CustomS3RetryFactAttribute.S3Settings : AmazonS3RetryFactAttribute.S3Settings;
            if (s3Settings == null)
                return null;

            var remoteFolderName = $"{s3Settings.RemoteFolderName}/{_remoteFolderName}/{GetRemoteFolder(caller)}";
            if (string.IsNullOrEmpty(subPath) == false)
                remoteFolderName = $"{remoteFolderName}/{subPath}";

            var settings = new S3Settings
            {
                BucketName = s3Settings.BucketName,
                RemoteFolderName = remoteFolderName,
                AwsAccessKey = s3Settings.AwsAccessKey,
                AwsSecretKey = s3Settings.AwsSecretKey,
                AwsRegionName = s3Settings.AwsRegionName
            };

            if (_isCustom)
            {
                settings.CustomServerUrl = s3Settings.CustomServerUrl;
                settings.ForcePathStyle = s3Settings.ForcePathStyle;
            }

            return settings;
        }

        public override void Dispose()
        {
            base.Dispose();

            var s3Settings = _isCustom ? CustomS3RetryFactAttribute.S3Settings : AmazonS3RetryFactAttribute.S3Settings;
            if (s3Settings == null)
                return;

            try
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
                using (var s3Client = new RavenAwsS3Client(s3Settings, DefaultConfiguration, cancellationToken: cts.Token))
                {
                    var cloudObjects = s3Client.ListObjects($"{s3Settings.RemoteFolderName}/{_remoteFolderName}/", string.Empty, false, includeFolders: true);
                    var pathsToDelete = cloudObjects.FileInfoDetails.Select(x => x.FullPath).ToList();

                    s3Client.DeleteMultipleObjects(pathsToDelete);
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
