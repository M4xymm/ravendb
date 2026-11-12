using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Backups.Policies.Database;
using Raven.Server.ServerWide.Backups.Policies.Server;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using static Raven.Server.ServerWide.Backups.BackupDecisionTracker;
using Exception = System.Exception;

namespace Raven.Server.ServerWide.Backups;

public class ServerBackupRunner : IDisposable
{

    private readonly ServerStore _serverStore;

    private PoolOfThreads.LongRunningWork _thread;

    private readonly ConcurrentQueue<DatabaseBackupState> _backupQueue = new();

    internal readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DatabaseBackupState>> BackupsPerDatabaseId = new();

    private List<IServerBackupPolicy> _serverBackupPolicies;

    private List<IDatabaseBackupPolicy> _databaseBackupPolicies;

    private List<IDatabaseBackupPolicy> _afterDatabaseLoadPolicies;

    private readonly RavenLogger _logger;

    private readonly RavenAuditLogger _auditLog;

    internal readonly BackupDecisionTracker Decisions;

    private readonly ConcurrentDictionary<string, IdleDatabase> _idleDatabases = new();

    public ServerBackupRunner(ServerStore serverStore)
    {
        _serverStore = serverStore;
        _logger = RavenLogManager.Instance.GetLoggerForServer<ServerBackupRunner>();
        _auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
        Decisions = new BackupDecisionTracker(this, serverStore, _logger);
    }

    public void Initialize()
    {
        Decisions.Start();

        _serverBackupPolicies =
        [
            ServerStartupPolicy.Instance,
            new ServerCpuCreditsPolicy(),
            new ServerLowMemoryPolicy(),
            new ServerHighDirtyMemoryPolicy(),
            ClusterHealthPolicy.Instance
        ];

        _databaseBackupPolicies =
        [
            BackupRunningPolicy.Instance,
            BackupDisabledPolicy.Instance,
            DatabaseExistsPolicy.Instance,
            BackupShouldRunOnThisNodePolicy.Instance,
            BackupTimePolicy.Instance,
            DatabaseStatePolicy.Instance,
            new NoChangesPolicy(this),
            new ConcurrentBackupPolicy(_serverStore.ConcurrentBackupsCounter),
            DatabaseLoadedPolicy.Instance
        ];

        _afterDatabaseLoadPolicies =
        [
            BackupDisabledPolicy.Instance,
            DatabaseExistsPolicy.Instance,
            BackupShouldRunOnThisNodePolicy.Instance
        ];

        _thread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(Run, null, ThreadNames.ForServerBackupRunner());
    }

    public void RegisterAllDatabases()
    {
        List<string> databaseNames;
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            databaseNames = _serverStore.Cluster.GetDatabaseNames(context);
        }

        foreach (var databaseName in databaseNames)
        {
            try
            {
                UpdateDatabase(databaseName);
            }
            catch (Exception e)
            {
                if (_logger.IsWarnEnabled)
                    _logger.Warn($"Failed to register the backup tasks of database '{databaseName}'.", e);
            }
        }
    }

    public Task OnClusterDatabaseChanged(string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType changeType, object changeState)
    {
        try
        {
            switch (changeType)
            {
                case DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged:
                case DatabasesLandlord.ClusterDatabaseChangeType.RecordRestored:
                    UpdateDatabase(databaseName);
                    break;

                case DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged:
                    switch (type)
                    {
                        case nameof(UpdateResponsibleNodeForTasksCommand):
                            UpdateDatabase(databaseName);
                            break;

                        case nameof(DelayBackupCommand):
                            ApplyDelay(databaseName, (DelayBackupCommand.DelayBackupCommandState)changeState);
                            break;
                    }
                    break;
            }
        }
        catch (Exception e)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn($"Failed to update the backup tasks of database '{databaseName}' after '{type}' (index {index}).", e);
        }

        return Task.CompletedTask;
    }

    private void UpdateDatabase(string databaseName)
    {
        var name = ShardHelper.ToDatabaseName(databaseName);
        var hostedDatabaseIds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, name))
        {
            if (rawRecord == null)
            {
                foreach (var (trackedName, _, _) in GetTrackedDatabasesOf(name))
                    _forTestingPurposes?.DatabaseTestingStuffInternals?.TryRemove(trackedName, out _);
            }
            else
            {
                foreach (var record in GetDatabaseRecords(rawRecord))
                {
                    if (IsHostedOnThisNode(record))
                        hostedDatabaseIds[record.DatabaseName] = GetDatabaseId(record);
                }
            }
        }

        foreach (var (trackedName, databaseId) in hostedDatabaseIds)
            ApplyDatabaseRecord(trackedName, databaseId);

        foreach (var (trackedName, databaseId, backupStates) in GetTrackedDatabasesOf(name))
        {
            if (hostedDatabaseIds.TryGetValue(trackedName, out var hostedDatabaseId) && hostedDatabaseId == databaseId)
                continue;

            lock (backupStates)
            {
                if (IsTracked(databaseId, backupStates) && IsCurrent(trackedName, databaseId) == false)
                    StopTracking(databaseId, backupStates);
            }
        }
    }

    private void ApplyDatabaseRecord(string trackedName, string databaseId)
    {
        while (true)
        {
            var backupStates = BackupsPerDatabaseId.GetOrAdd(databaseId, static _ => new ConcurrentDictionary<long, DatabaseBackupState>());

            lock (backupStates)
            {
                if (IsTracked(databaseId, backupStates) == false)
                    continue;

                using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, ShardHelper.ToDatabaseName(trackedName)))
                {
                    var record = FindHostedRecord(rawRecord, trackedName);
                    if (record != null && GetDatabaseId(record) == databaseId)
                    {
                        ApplyDatabaseRecord(context, trackedName, databaseId, backupStates, record);

                        if (backupStates.IsEmpty)
                            StopTracking(databaseId, backupStates);

                        return;
                    }

                    StopTracking(databaseId, backupStates);

                    if (record == null)
                        return;

                    databaseId = GetDatabaseId(record);
                }
            }
        }
    }

    private bool IsTracked(string databaseId, ConcurrentDictionary<long, DatabaseBackupState> backupStates)
    {
        return BackupsPerDatabaseId.TryGetValue(databaseId, out var current) && ReferenceEquals(current, backupStates);
    }

    private bool IsCurrent(string trackedName, string databaseId)
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, ShardHelper.ToDatabaseName(trackedName)))
        {
            var record = FindHostedRecord(rawRecord, trackedName);
            return record != null && GetDatabaseId(record) == databaseId;
        }
    }

    private void StopTracking(string databaseId, ConcurrentDictionary<long, DatabaseBackupState> backupStates)
    {
        foreach (var kvp in backupStates.ForceEnumerateInThreadSafeManner())
            kvp.Value.Stale.Raise();

        backupStates.Clear();

        BackupsPerDatabaseId.TryRemove(new KeyValuePair<string, ConcurrentDictionary<long, DatabaseBackupState>>(databaseId, backupStates));
        _idleDatabases.TryRemove(databaseId, out _);
    }

    private RawDatabaseRecord FindHostedRecord(RawDatabaseRecord rawRecord, string trackedName)
    {
        var record = FindRecord(rawRecord, trackedName);
        return record != null && IsHostedOnThisNode(record) ? record : null;
    }

    private static RawDatabaseRecord FindRecord(RawDatabaseRecord rawRecord, string trackedName)
    {
        if (rawRecord == null)
            return null;

        foreach (var record in GetDatabaseRecords(rawRecord))
        {
            if (string.Equals(record.DatabaseName, trackedName, StringComparison.OrdinalIgnoreCase))
                return record;
        }

        return null;
    }

    private static IEnumerable<RawDatabaseRecord> GetDatabaseRecords(RawDatabaseRecord rawRecord)
    {
        return rawRecord.IsSharded ? rawRecord.GetShardedDatabaseRecords() : [rawRecord];
    }

    internal static string GetDatabaseId(DocumentDatabase database)
    {
        return database.DatabaseGroupId ?? database.Name;
    }

    private static string GetDatabaseId(RawDatabaseRecord record)
    {
        return record.Topology?.DatabaseTopologyIdBase64 ?? record.DatabaseName;
    }

    internal string ReadDatabaseId(string databaseName)
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, ShardHelper.ToDatabaseName(databaseName)))
        {
            var record = FindRecord(rawRecord, databaseName);
            return record == null ? null : GetDatabaseId(record);
        }
    }

    private bool IsHostedOnThisNode(RawDatabaseRecord record)
    {
        return record.Topology != null && record.Topology.RelevantFor(_serverStore.NodeTag);
    }

    private List<(string DatabaseName, string DatabaseId, ConcurrentDictionary<long, DatabaseBackupState> BackupStates)> GetTrackedDatabasesOf(string databaseName)
    {
        var trackedDatabases = new List<(string, string, ConcurrentDictionary<long, DatabaseBackupState>)>();

        foreach (var (databaseId, backupStates) in BackupsPerDatabaseId)
        {
            var trackedName = backupStates.Values.FirstOrDefault()?.DatabaseName;
            if (trackedName != null && string.Equals(ShardHelper.ToDatabaseName(trackedName), databaseName, StringComparison.OrdinalIgnoreCase))
                trackedDatabases.Add((trackedName, databaseId, backupStates));
        }

        return trackedDatabases;
    }

    private void RegisterNewBackup(DatabaseBackupState backupState)
    {
        if (BackupsPerDatabaseId.TryGetValue(backupState.DatabaseId, out var backupStates) &&
            backupStates.TryAdd(backupState.Configuration.TaskId, backupState))
            Enqueue(backupState);
    }

    private void Enqueue(DatabaseBackupState backupState)
    {
        if (backupState.InQueue.Raise())
            _backupQueue.Enqueue(backupState);
    }

    private void Run(object state)
    {
        try
        {
            while (_serverStore.ServerShutdown.IsCancellationRequested == false)
            {
                DatabaseBackupState firstState = null;
                var now = _serverStore.Server.Time.GetUtcNow();
                var skippedIncrementalBackups = new List<DatabaseBackupState>();

                while (_serverStore.ServerShutdown.IsCancellationRequested == false &&
                       _backupQueue.Count > 0 &&
                       _backupQueue.TryPeek(out var backupState) &&
                       backupState != firstState &&
                       AreAllServerPoliciesCompliant(now, backupState.DatabaseName) &&
                       _backupQueue.TryDequeue(out backupState))
                {
                    backupState.InQueue.Lower();

                    if (backupState.Stale)
                        continue;

                    firstState ??= backupState;

                    try
                    {
                        if (AreAllDatabasePoliciesCompliant(backupState, now, out var failedPolicy))
                            RunScheduledBackup(backupState);
                        else if (failedPolicy is NoChangesPolicy && backupState.StatusUpdateInProgress.Raise())
                            skippedIncrementalBackups.Add(backupState);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsWarnEnabled)
                            _logger.Warn($"Could not process the scheduled backup of {backupState}.", e);
                    }

                    Enqueue(backupState);
                }

                SaveSkippedIncrementalBackups(skippedIncrementalBackups);

                Decisions.RefreshSummary();

                _serverStore.ServerShutdown.WaitHandle.WaitOne(
                    _serverStore.Configuration.Backup.BackupRunnerFrequency.AsTimeSpan);
            }
        }
        catch (Exception e)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error("Server backup runner has stopped due to an unexpected error.", e);

            _serverStore.NotificationCenter.Add(AlertRaised.Create(
                _serverStore.NodeTag,
                "Server Backup Runner",
                "The server backup runner has stopped unexpectedly. No backups will be scheduled until the server is restarted.",
                AlertReason.PeriodicBackup,
                NotificationSeverity.Error,
                details: new ExceptionDetails(e)));
        }
    }

    private void RunScheduledBackup(DatabaseBackupState backupState)
    {
        InvokeBeforeBackupStarted(backupState);

        var nextBackup = backupState.NextBackup;
        if (nextBackup == null || TryStartRunning(backupState) == false)
            return;

        _ = Task.Run(async () =>
        {
            DocumentDatabase database;

            try
            {
                var result = _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(backupState.DatabaseName);
                if (result.DatabaseStatus != DatabasesLandlord.DatabaseSearchResult.Status.Database)
                {
                    backupState.AddToDecisionLog(BackupDecisionCategory.Cancelled, CancelReasonTaskDeleted, GetCancelReasonText(CancelReasonTaskDeleted), DateTime.UtcNow);
                    FinishBackup(backupState, reschedule: false);
                    return;
                }

                database = await result.DatabaseTask;
                backupState.DatabaseWakeUpTimeUtc = DateTime.UtcNow;
            }
            catch (Exception e)
            {
                OnDatabaseLoadFailure(backupState, nextBackup, e);
                return;
            }

            bool canStart;
            long operationId = 0;

            try
            {
                canStart = CanStartAfterDatabaseLoad(database, backupState, nextBackup);
                if (canStart)
                    operationId = database.Operations.GetNextOperationId();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Could not check the scheduled backup {backupState} after loading its database.", e);

                canStart = false;
            }

            if (canStart == false)
            {
                FinishBackup(backupState, reschedule: false);
                return;
            }

            try
            {
                StartBackup(database, backupState, nextBackup.IsFull, nextBackup.DateTime, operationId);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Could not start the scheduled backup {backupState}.", e);
            }
        });
    }

    private void OnDatabaseLoadFailure(DatabaseBackupState backupState, NextBackup nextBackup, Exception exception)
    {
        var now = DateTime.UtcNow;

        switch (exception.ExtractSingleInnerException())
        {
            case DatabaseConcurrentLoadTimeoutException:
                backupState.AddToDecisionLog(BackupDecisionCategory.Waiting, DatabaseLoadedPolicy.Instance.Name,
                    "Too many databases are loading at the same time. The backup will be retried on the next run.", now);
                FinishBackup(backupState, reschedule: false);
                return;

            case DatabaseDisabledException:
                backupState.AddToDecisionLog(BackupDecisionCategory.Waiting, DatabaseStatePolicy.Instance.Name, "The database is disabled or being deleted on this node.", now);
                FinishBackup(backupState, reschedule: false);
                return;

            case DatabaseRestoringException:
                backupState.AddToDecisionLog(BackupDecisionCategory.Waiting, DatabaseStatePolicy.Instance.Name, "The database is being restored.", now);
                FinishBackup(backupState, reschedule: false);
                return;

            case DatabaseNotRelevantException:
                backupState.AddToDecisionLog(BackupDecisionCategory.Waiting, DatabaseStatePolicy.Instance.Name, "The database isn't hosted on this node.", now);
                FinishBackup(backupState, reschedule: false);
                return;

            case ObjectDisposedException:
            case OperationCanceledException:
                if (_serverStore.ServerShutdown.IsCancellationRequested)
                {
                    FinishBackup(backupState, reschedule: false);
                    return;
                }

                break;
        }

        ReportStartFailure(backupState, nextBackup.IsFull, nextBackup.DateTime, exception);
        FinishBackup(backupState);
    }

    internal void OnIdleDatabaseUnloaded(DocumentDatabase database, long lastEtag, string lastChangeVector)
    {
        var databaseId = GetDatabaseId(database);
        _idleDatabases[databaseId] = new IdleDatabase(lastEtag, lastChangeVector);

        if (BackupsPerDatabaseId.ContainsKey(databaseId) == false)
            _idleDatabases.TryRemove(databaseId, out _);
    }

    internal void OnIdleDatabaseChangedWhileUnloading(DocumentDatabase database)
    {
        _idleDatabases.TryRemove(GetDatabaseId(database), out _);
    }

    internal bool HasBackupTasks(DocumentDatabase database)
    {
        return BackupsPerDatabaseId.TryGetValue(GetDatabaseId(database), out var backupStates) && backupStates.IsEmpty == false;
    }

    internal bool ShouldKeepLoadedForBackup(DocumentDatabase database, long lastEtag, string lastChangeVector)
    {
        if (BackupsPerDatabaseId.TryGetValue(GetDatabaseId(database), out var backupStates) == false)
            return false;

        var window = _serverStore.Configuration.Backup.KeepIdleDatabaseLoadedBeforeBackup.AsTimeSpan;
        if (window <= TimeSpan.Zero)
            return false;

        var now = DateTime.UtcNow;

        try
        {
            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var backupState in backupStates.Values)
                {
                    if (backupState.Stale)
                        continue;

                    var nextBackup = backupState.NextBackup;
                    if (nextBackup == null || nextBackup.DateTime - now > window)
                        continue;

                    if (IsActiveOnThisNode(context, backupState) == false)
                        continue;

                    if (nextBackup.IsFull)
                        return true;

                    var backupStatus = backupState.GetMostUpdatedLocalBackupStatus(backupState.Configuration.TaskId, backupState.BackupStatus, backupState.DatabaseName);
                    if (HasNoChangesSinceLastBackup(context, backupState, backupStatus, lastEtag, lastChangeVector) == false)
                        return true;
                }
            }
        }
        catch (Exception e)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn($"Could not check whether the idle database '{database.Name}' should stay loaded for a backup that is due soon.", e);
        }

        return false;
    }

    internal bool CanSkipIncrementalBackup(ClusterOperationContext context, DatabaseBackupState backupState)
    {
        var nextBackup = backupState.NextBackup;
        if (nextBackup == null || nextBackup.IsFull)
            return false;

        if (TryGetIdleDatabase(backupState, out var idleDatabase) == false)
            return false;

        var backupStatus = backupState.GetMostUpdatedLocalBackupStatus(backupState.Configuration.TaskId, backupState.BackupStatus, backupState.DatabaseName);

        return HasNoChangesSinceLastBackup(context, backupState, backupStatus, idleDatabase.LastEtag, idleDatabase.LastChangeVector);
    }

    private bool HasNoChangesSinceLastBackup(ClusterOperationContext context, DatabaseBackupState backupState, PeriodicBackupStatus backupStatus, long lastEtag, string lastChangeVector)
    {
        if (backupStatus == null)
            return false;

        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, backupState.DatabaseName))
        {
            if (rawRecord == null || BackupUtils.HasChangesSinceLastBackup(backupStatus, lastEtag, lastChangeVector, rawRecord.EtagForBackup))
                return false;
        }

        return BackupUtils.IsFullBackupRequired(backupStatus, backupState.Configuration) == false;
    }

    private bool TryGetIdleDatabase(DatabaseBackupState backupState, out IdleDatabase idleDatabase)
    {
        idleDatabase = default;

        return _serverStore.IdleDatabases.ContainsKey(backupState.DatabaseName) &&
               _serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(backupState.DatabaseName, out _) == false &&
               _idleDatabases.TryGetValue(backupState.DatabaseId, out idleDatabase);
    }

    private readonly record struct IdleDatabase(long LastEtag, string LastChangeVector);

    private void SaveSkippedIncrementalBackups(List<DatabaseBackupState> backupStates)
    {
        if (backupStates.Count == 0)
            return;

        _ = Task.Run(async () =>
        {
            var processed = 0;

            try
            {
                var useBatch = _serverStore.Engine.CommandsVersionManager.CanPutCommand(nameof(UpdatePeriodicBackupStatusBatchCommand));

                foreach (var chunk in backupStates.Chunk(_serverStore.Configuration.Backup.MaxStatusUpdatesPerBatch))
                {
                    try
                    {
                        var updates = GetSkippedIncrementalBackupStatuses(chunk);
                        if (updates.Count == 0)
                            continue;

                        if (useBatch)
                            await SaveBackupStatusesAsync(updates);
                        else
                            await SaveBackupStatusesOneByOneAsync(updates);
                    }
                    catch (OperationCanceledException) when (_serverStore.ServerShutdown.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsWarnEnabled)
                            _logger.Warn("Failed to save the backup statuses of the skipped incremental backups.", e);
                    }
                    finally
                    {
                        foreach (var backupState in chunk)
                            backupState.StatusUpdateInProgress.Lower();

                        processed += chunk.Length;
                    }
                }
            }
            finally
            {
                for (var i = processed; i < backupStates.Count; i++)
                    backupStates[i].StatusUpdateInProgress.Lower();
            }
        });
    }

    private List<(DatabaseBackupState BackupState, PeriodicBackupStatus BackupStatus)> GetSkippedIncrementalBackupStatuses(DatabaseBackupState[] backupStates)
    {
        var updates = new List<(DatabaseBackupState BackupState, PeriodicBackupStatus BackupStatus)>(backupStates.Length);

        using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            foreach (var backupState in backupStates)
            {
                var nextBackup = backupState.NextBackup;
                if (backupState.Stale || backupState.Running || nextBackup == null || TryGetIdleDatabase(backupState, out _) == false)
                    continue;

                var currentStatus = backupState.GetMostUpdatedLocalBackupStatus(backupState.Configuration.TaskId, backupState.BackupStatus, backupState.DatabaseName);
                var backupStatus = JsonDeserializationClient.PeriodicBackupStatus(context.ReadObject(currentStatus.ToJson(), "skipped-incremental-backup-status"));

                backupStatus.Version++;
                backupStatus.Error = null;
                backupStatus.IsFull = false;
                backupStatus.NodeTag = _serverStore.NodeTag;
                backupStatus.DurationInMs = 0;
                backupStatus.LastIncrementalBackup = backupStatus.LastIncrementalBackupInternal = nextBackup.DateTime;

                if (backupStatus.LocalBackup != null)
                {
                    backupStatus.LocalBackup.LastIncrementalBackup = nextBackup.DateTime;
                    backupStatus.LocalBackup.IncrementalBackupDurationInMs = 0;
                }

                updates.Add((backupState, backupStatus));
            }
        }

        return updates;
    }

    private async Task SaveBackupStatusesAsync(List<(DatabaseBackupState BackupState, PeriodicBackupStatus BackupStatus)> updates)
    {
        var commands = updates
            .Select(x => new UpdatePeriodicBackupStatusCommand(x.BackupState.DatabaseName, RaftIdGenerator.DontCareId) { PeriodicBackupStatus = x.BackupStatus })
            .ToList();

        var (index, _) = await _serverStore.SendToLeaderAsync(new UpdatePeriodicBackupStatusBatchCommand(commands, RaftIdGenerator.NewId()), _serverStore.ServerShutdown);
        await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, _serverStore.ServerShutdown);

        foreach (var (backupState, backupStatus) in updates)
            OnBackupStatusSaved(backupState, backupStatus);
    }

    private async Task SaveBackupStatusesOneByOneAsync(List<(DatabaseBackupState BackupState, PeriodicBackupStatus BackupStatus)> updates)
    {
        foreach (var (backupState, backupStatus) in updates)
        {
            try
            {
                var command = new UpdatePeriodicBackupStatusCommand(backupState.DatabaseName, RaftIdGenerator.NewId()) { PeriodicBackupStatus = backupStatus };

                var (index, _) = await _serverStore.SendToLeaderAsync(command, _serverStore.ServerShutdown);
                await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, _serverStore.ServerShutdown);

                OnBackupStatusSaved(backupState, backupStatus);
            }
            catch (Exception e) when (_serverStore.ServerShutdown.IsCancellationRequested == false)
            {
                if (_logger.IsWarnEnabled)
                    _logger.Warn($"Failed to save the backup status of the skipped incremental backup {backupState}.", e);
            }
        }
    }

    private void OnBackupStatusSaved(DatabaseBackupState backupState, PeriodicBackupStatus backupStatus)
    {
        if (backupState.Running)
            return;

        backupState.BackupStatus = backupStatus;
        backupState.NextBackup = backupState.Stale ? null : backupState.GetNextBackupDetails(backupStatus, out _);
    }

    private bool CanStartAfterDatabaseLoad(DocumentDatabase database, DatabaseBackupState backupState, NextBackup nextBackup)
    {
        var now = DateTime.UtcNow;

        if (backupState.Stale && ReferenceEquals(GetDatabaseState(backupState.DatabaseId, backupState.Configuration.TaskId), backupState) == false)
        {
            backupState.AddToDecisionLog(BackupDecisionCategory.Cancelled, CancelReasonTaskDeleted, GetCancelReasonText(CancelReasonTaskDeleted), now);
            return false;
        }

        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var backupPolicy in _afterDatabaseLoadPolicies)
            {
                if (backupPolicy.CanDoBackup(context, _serverStore, backupState, now, out string reason, out var category))
                    continue;

                backupState.AddToDecisionLog(category, backupPolicy.Name, reason, now);
                return false;
            }
        }

        if (database.DisableOngoingTasks)
        {
            backupState.NextBackup = backupState.GetNextOccurrenceAfter(now);

            var heldUntil = backupState.NextBackup == null ? string.Empty : $" until '{backupState.NextBackup.DateTime}'";
            backupState.AddToDecisionLog(BackupDecisionCategory.HeldBack, DisableOngoingTasksDetail,
                $"The ongoing tasks of the database are disabled by the 'disable.tasks.marker' file. Held back{heldUntil}.", now);
            return false;
        }

        return true;
    }

    public long StartBackupTask(DocumentDatabase database, long taskId, bool isFullBackup, long operationId, DateTime? startTimeUtc = null)
    {
        var backupState = GetDatabaseStateByTaskId(database, taskId);
        if (backupState == null)
            throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");

        if (database.DisableOngoingTasks)
            throw new InvalidOperationException($"Backup task is disabled via marker file for database '{database.Name}'.");

        InvokeBeforeBackupStarted(backupState);

        if (backupState.StatusUpdateInProgress || backupState.Running.Raise() == false)
            throw CreateBackupAlreadyRunningException(backupState);

        try
        {
            BackupUtils.CheckServerHealthBeforeBackup(_serverStore, backupState.Configuration.Name);
        }
        catch
        {
            backupState.Running.Lower();
            throw;
        }

        OnRunningRaised(backupState);

        StartBackup(database, backupState, isFullBackup, startTimeUtc ?? SystemTime.UtcNow, operationId);

        return operationId;
    }

    private BackupAlreadyRunningException CreateBackupAlreadyRunningException(DatabaseBackupState backupState)
    {
        var runningTask = backupState.RunningTask;
        var message = runningTask != null
            ? $"Could not start backup task '{backupState.Configuration.TaskId}' because there is already a running backup under operation id '{runningTask.Id}'"
            : backupState.StatusUpdateInProgress
                ? $"Could not start backup task '{backupState.Configuration.TaskId}' because the status of a skipped incremental backup of this task is being saved, please try again in a few seconds"
                : $"Could not start backup task '{backupState.Configuration.TaskId}' because a backup of this task is already starting";

        return new BackupAlreadyRunningException(message)
        {
            OperationId = runningTask?.Id ?? 0,
            NodeTag = _serverStore.NodeTag
        };
    }

    private bool TryStartRunning(DatabaseBackupState backupState)
    {
        if (backupState.Running.Raise() == false)
            return false;

        OnRunningRaised(backupState);

        return true;
    }

    private void OnRunningRaised(DatabaseBackupState backupState)
    {
        backupState.ResetCancelReason();
        _serverStore.ConcurrentBackupsCounter.StartBackup(backupState.OriginalDatabaseName, backupState.Configuration.Name, _logger, enforceLimit: false);
    }

    internal bool IsActiveOnThisNode(ClusterOperationContext context, DatabaseBackupState backupState)
    {
        var configuration = backupState.Configuration;
        return configuration.Disabled == false &&
               configuration.HasBackup() &&
               BackupUtils.GetResponsibleNodeTag(context, backupState.DatabaseName, configuration.TaskId) == _serverStore.NodeTag;
    }

    private bool TryGetTestingStuff(string databaseName, out TestingStuffInternal testingStuff)
    {
        testingStuff = null;
        return _forTestingPurposes?.DatabaseTestingStuffInternals?.TryGetValue(databaseName, out testingStuff) == true;
    }

    private void InvokeBeforeBackupStarted(DatabaseBackupState backupState)
    {
        if (TryGetTestingStuff(backupState.DatabaseName, out var testingStuffInternal))
        {
            testingStuffInternal.OnBeforeBackupStarted?.Invoke(backupState);
        }
    }

    private void StartBackup(DocumentDatabase database, DatabaseBackupState backupState, bool isFullBackup, DateTime startTimeInUtc, long operationId)
    {
        try
        {
            var localBackupStatus = backupState.BackupStatus = backupState.GetMostUpdatedLocalBackupStatus(backupState.Configuration.TaskId, inMemoryBackupStatus: backupState.BackupStatus, database.Name);
            var backupToLocalFolder = BackupConfiguration.CanBackupUsing(backupState.Configuration.LocalSettings);
            if (BackupUtils.IsFullBackupRequired(localBackupStatus, backupState.Configuration))
                isFullBackup = true;

            backupState.OperationId = operationId;
            backupState.StartTimeInUtc = startTimeInUtc;

            var backupTypeText = GetBackupTypeText(isFullBackup, backupState.Configuration.BackupType);

            var backupParameters = new BackupParameters
            {
                RetentionPolicy = backupState.Configuration.RetentionPolicy,
                StartTimeUtc = backupState.StartTimeInUtc,
                BackupStatus = backupState.BackupStatus,
                IsOneTimeBackup = false,
                IsFullBackup = isFullBackup,
                BackupToLocalFolder = backupToLocalFolder,
                TempBackupPath = BackupUtils.GetBackupTempPath(database.Configuration, "PeriodicBackupTemp", out _),
                OperationId = backupState.OperationId,
                Name = backupState.Configuration.Name
            };

            var backupTask = BackupUtils.GetBackupTask(database, backupParameters, backupState.Configuration, token: null, _logger, _forTestingPurposes);

            var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);

            backupState.AddToDecisionLog(BackupDecisionCategory.Started, detail: null, reason: null, DateTime.UtcNow);

            var task = database.Operations.AddLocalOperation(
                backupState.OperationId,
                OperationType.DatabaseBackup,
                $"{backupTypeText} Periodic Backup '{backupState.Configuration.Name} ({backupState.Configuration.TaskId})' for database '{backupState.DatabaseName}'.",
                detailedDescription: null,
                taskFactory: onProgress => StartBackupThread(database, backupState, backupTask, tcs, onProgress),
                token: backupTask.TaskCancelToken);

            _ = task.ContinueWith(_ => backupTask.TaskCancelToken.Dispose());
        }
        catch (Exception e)
        {
            ReportStartFailure(backupState, isFullBackup, startTimeInUtc, e);
            FinishBackup(backupState);
            throw;
        }
    }

    private void OnBackupFinished(DatabaseBackupState backupState, BackupDecisionCategory category, string detail, string reason)
    {
        try
        {
            backupState.AddToDecisionLog(category, detail, reason, DateTime.UtcNow);
            FinishBackup(backupState);
        }
        catch (Exception e)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn($"Failed to finish the backup {backupState}.", e);
        }
    }

    private static string GetCancelReason(DocumentDatabase database, DatabaseBackupState backupState)
    {
        var reason = backupState.CancelReason;
        if (reason != null)
            return reason;

        return database.DatabaseShutdown.IsCancellationRequested ? CancelReasonUnloaded : CancelReasonUser;
    }

    private void ReportStartFailure(DatabaseBackupState backupState, bool isFullBackup, DateTime startTimeInUtc, Exception e)
    {
        backupState.AddToDecisionLog(BackupDecisionCategory.Failed, detail: null, e.AllInnerMessages(), DateTime.UtcNow);

        if (_logger.IsErrorEnabled)
            _logger.Error($"Could not start backup for {backupState}.", e);

        try
        {
            backupState.BackupStatus = backupState.GetMostUpdatedLocalBackupStatus(backupState.Configuration.TaskId, backupState.BackupStatus, backupState.DatabaseName);
            backupState.BackupStatus.Version++;
            backupState.BackupStatus.Error = new Error { Exception = e.ToString(), At = DateTime.UtcNow };
            backupState.BackupStatus.IsFull = isFullBackup;
            backupState.BackupStatus.NodeTag = _serverStore.NodeTag;
            if (isFullBackup)
                backupState.BackupStatus.LastFullBackupInternal = startTimeInUtc;
            else
                backupState.BackupStatus.LastIncrementalBackupInternal = startTimeInUtc;

            BackupUtils.SaveBackupStatus(backupState.BackupStatus, backupState.DatabaseName, _serverStore, _logger);

            _serverStore.NotificationCenter.Add(AlertRaised.Create(
                backupState.DatabaseName,
                $"Periodic Backup task: '{backupState.Configuration.Name}'",
                $"Failed to start the backup task '{backupState.Configuration.Name}'. The next backup will be rescheduled.",
                AlertReason.PeriodicBackup, NotificationSeverity.Error, details: new ExceptionDetails(e)));
        }
        catch (Exception reportError)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error($"Failed to report backup start-failure for {backupState}.", reportError);
        }
    }

    private void FinishBackup(DatabaseBackupState backupState, bool reschedule = true)
    {
        if (TryGetTestingStuff(backupState.DatabaseName, out var value))
        {
            value.HoldBackupFromFinishing?.WaitOne();
        }

        try
        {
            if (backupState.Stale)
                backupState.NextBackup = null;
            else if (reschedule)
                backupState.NextBackup = backupState.GetNextBackupDetails(backupState.BackupStatus, out _);
        }
        catch (Exception e)
        {
            backupState.NextBackup = null;

            if (_logger.IsWarnEnabled)
                _logger.Warn($"Failed to schedule the next backup for {backupState}.", e);
        }

        var runningBackupStatus = backupState.RunningBackupStatus;

        backupState.RunningTask = null;
        backupState.RunningBackupStatus = null;
        backupState.RunningCancel = null;

        try
        {
            _serverStore.ConcurrentBackupsCounter.FinishBackup(backupState.OriginalDatabaseName, backupState.Configuration.Name, runningBackupStatus, null, _logger);
        }
        finally
        {
            backupState.Running.Lower();
        }
    }

    private Task<IOperationResult> StartBackupThread(DocumentDatabase database, DatabaseBackupState backupState, BackupTask backupTask, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
    {
        var threadName = $"Backup task {backupState.Configuration.Name} for database '{database.Name}'";

        try
        {
            PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ => RunBackupThread(database, backupState, backupTask, threadName, tcs, onProgress), null, ThreadNames.ForBackupTask(threadName,
                database.Name, backupState.Configuration.Name));
        }
        catch (Exception e)
        {
            OnBackupFinished(backupState, BackupDecisionCategory.Failed, detail: null, e.AllInnerMessages());
            tcs.TrySetException(e);
        }

        return tcs.Task;
    }

    private void RunBackupThread(DocumentDatabase database, DatabaseBackupState backupState, BackupTask backupTask, string threadName, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
    {
        var runningBackupStatus = new PeriodicBackupStatus
        {
            TaskId = backupState.Configuration.TaskId,
            BackupType = backupState.Configuration.BackupType,
            LastEtag = backupState.BackupStatus.LastEtag,
            LastRaftIndex = backupState.BackupStatus.LastRaftIndex,
            LastFullBackup = backupState.BackupStatus.LastFullBackup,
            LastIncrementalBackup = backupState.BackupStatus.LastIncrementalBackup,
            LastFullBackupInternal = backupState.BackupStatus.LastFullBackupInternal,
            LastIncrementalBackupInternal = backupState.BackupStatus.LastIncrementalBackupInternal,
            IsFull = backupTask._isFullBackup,
            LocalBackup = backupState.BackupStatus.LocalBackup,
            LastOperationId = backupState.BackupStatus.LastOperationId,
            FolderName = backupState.BackupStatus.FolderName,
            LastDatabaseChangeVector = backupState.BackupStatus.LastDatabaseChangeVector
        };

        backupState.RunningBackupStatus = runningBackupStatus;

        try
        {
            ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, _logger);
            NativeMemory.EnsureRegistered();

            using (database.PreventFromUnloadingByIdleOperations())
            {
                BackupResult backupResult = backupTask.RunPeriodicBackup(onProgress, backupState, tcs.Task, ref runningBackupStatus);

                backupState.BackupStatus = runningBackupStatus;

                OnBackupFinished(backupState, BackupDecisionCategory.Completed, detail: null, reason: null);
                tcs.SetResult(backupResult);
            }

            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var backupKind = backupTask._isFullBackup ? BackupKind.Full : BackupKind.Incremental;
                    var configurationString = context.ReadObject(backupState.Configuration.ToAuditJson(), nameof(PeriodicBackupConfiguration)).ToString();
                    _auditLog.Audit($"BACKUP {backupKind} backup executed automatically as scheduled with configuration: '{configurationString}'");
                }
            }
        }
        catch (Exception e) when (tcs.Task.IsCompleted)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error($"Failed after the backup thread completed: '{backupState.Configuration.Name}'", e);
        }
        catch (Exception e) when (e.ExtractSingleInnerException() is OperationCanceledException oce)
        {
            var inMemoryBackupStatus = GetDatabaseState(backupState.DatabaseId, backupState.BackupStatus.TaskId);
            if (inMemoryBackupStatus != null)
            {
                runningBackupStatus.DelayUntil = inMemoryBackupStatus.BackupStatus.DelayUntil;
                runningBackupStatus.OriginalBackupTime = inMemoryBackupStatus.BackupStatus.OriginalBackupTime;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"Canceled the backup thread: '{backupState.Configuration.Name}'", oce);

            backupState.BackupStatus = runningBackupStatus;

            var cancelReason = GetCancelReason(database, backupState);
            OnBackupFinished(backupState, BackupDecisionCategory.Cancelled, cancelReason, GetCancelReasonText(cancelReason));
            tcs.SetCanceled();
        }
        catch (Exception e)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error($"Failed to run the backup thread: '{backupState.Configuration.Name}'", e);

            backupState.BackupStatus = runningBackupStatus;

            OnBackupFinished(backupState, BackupDecisionCategory.Failed, detail: null, e.ExtractSingleInnerException().AllInnerMessages());
            tcs.SetException(e);
        }
    }

    public async Task DelayAsync(DocumentDatabase database, long operationId, DateTime delayUntil, X509Certificate2 clientCert, CancellationToken token)
    {
        var databaseName = database.Name;

        if (BackupsPerDatabaseId.TryGetValue(GetDatabaseId(database), out var backupStates) == false)
            throw new InvalidOperationException($"Fail to delay backup task with operationId '{operationId}',  the operation with that number isn't registered");

        foreach (var databaseBackupState in backupStates)
        {
            var runningTask = databaseBackupState.Value.RunningTask;
            if (runningTask == null || runningTask.Id != operationId)
                continue;

            var nextBackup = databaseBackupState.Value.GetNextBackupDetails(databaseBackupState.Value.BackupStatus, skipErrorLog: true);

            var originalBackupTime = nextBackup != null && delayUntil > nextBackup.DateTime
                ? nextBackup.DateTime
                : databaseBackupState.Value.StartTimeInUtc;

            var command = new DelayBackupCommand(databaseName, RaftIdGenerator.NewId())
            {
                TaskId = databaseBackupState.Key,
                DelayUntil = delayUntil,
                OriginalBackupTime = originalBackupTime
            };

            try
            {
                (long index, _) = await _serverStore.SendToLeaderAsync(command);
                await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
            }
            catch (Exception e)
            {
                if (_logger.IsWarnEnabled)
                {
                    var msg =
                        $"Fail to delay backup task with task id '{operationId}' cluster-wide, the task was delayed until '{delayUntil}' UTC only on the current node.";

                    _logger.Warn(msg, e);
                }
            }
            databaseBackupState.Value.BackupStatus.DelayUntil = delayUntil;
            databaseBackupState.Value.BackupStatus.OriginalBackupTime = originalBackupTime;
            databaseBackupState.Value.NextBackup = databaseBackupState.Value.GetNextBackupDetails(databaseBackupState.Value.BackupStatus, out string _);

            if (TryGetTestingStuff(databaseName, out var testingStuffInternal))
            {
                testingStuffInternal.OnBackupTaskRunHoldBackupExecution?.SetResult(null);
            }

            databaseBackupState.Value.SetCancelReason(CancelReasonDelay);

            await database.Operations.KillOperationAsync(operationId, token);

            try
            {
                await runningTask.Task;
            }
            catch
            {
            }

            return;
        }

        throw new InvalidOperationException($"Fail to delay backup task with task id '{operationId}', the operation with that number isn't registered");
    }

    public PeriodicBackupStatus GetMostUpdatedClusterBackupStatus(DocumentDatabase database, long taskId)
    {
        return GetMostUpdatedClusterBackupStatus(database.Name, GetDatabaseState(GetDatabaseId(database), taskId), taskId);
    }

    private PeriodicBackupStatus GetMostUpdatedClusterBackupStatus(string databaseName, DatabaseBackupState databaseBackupState, long taskId)
    {
        PeriodicBackupStatus inMemoryBackupStatus = null;
        if (databaseBackupState != null)
        {
            inMemoryBackupStatus = databaseBackupState.BackupStatus;
        }

        if (TryGetTestingStuff(databaseName, out var testingStuffInternal))
        {
            if (testingStuffInternal.BackupStatusFromMemoryOnly)
                return inMemoryBackupStatus;
        }

        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var backupStatus = BackupUtils.GetBackupStatusFromCluster(context, databaseName, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }
    }

    private void ApplyDelay(string databaseName, DelayBackupCommand.DelayBackupCommandState state)
    {
        var databaseId = ReadDatabaseId(databaseName);
        var databaseBackupState = databaseId == null ? null : GetDatabaseState(databaseId, state.TaskId);
        if (databaseBackupState == null)
            return;

        databaseBackupState.BackupStatus ??= new PeriodicBackupStatus { TaskId = state.TaskId };
        databaseBackupState.BackupStatus.DelayUntil = state.DelayUntil;
        databaseBackupState.BackupStatus.OriginalBackupTime = state.OriginalBackupTime;
    }

    private void ApplyDatabaseRecord(ClusterOperationContext context, string databaseName, string databaseId,
        ConcurrentDictionary<long, DatabaseBackupState> backupStates, RawDatabaseRecord databaseRecord)
    {
        var configurations = databaseRecord.PeriodicBackups;

        if (configurations == null || configurations.Count == 0)
        {
            foreach (var kvp in backupStates.ForceEnumerateInThreadSafeManner())
            {
                kvp.Value.Stale.Raise();
                kvp.Value.CancelRunningBackup(CancelReasonTaskDeleted);
            }

            backupStates.Clear();
            return;
        }

        var allBackupTaskIds = new List<long>(configurations.Count);
        foreach (var configuration in configurations)
        {
            allBackupTaskIds.Add(configuration.TaskId);
            ApplyBackupConfiguration(context, databaseName, databaseId, configuration);
        }

        foreach (var deletedBackupTaskId in backupStates.Keys.Except(allBackupTaskIds))
        {
            if (backupStates.TryRemove(deletedBackupTaskId, out var backupState) == false)
                continue;

            backupState.Stale.Raise();
            backupState.CancelRunningBackup(CancelReasonTaskDeleted);
        }
    }

    private void ApplyBackupConfiguration(ClusterOperationContext context, string databaseName, string databaseId, PeriodicBackupConfiguration configuration)
    {
        var existingBackupState = GetDatabaseState(databaseId, configuration.TaskId);
        var alreadyKnownWithoutDestinations = existingBackupState != null && existingBackupState.Configuration.HasBackup() == false;
        var taskState = GetTaskStatus(configuration, databaseName, out _, disableLog: alreadyKnownWithoutDestinations, context: context);
        if (TryGetTestingStuff(databaseName, out var testingStuffInternal))
        {
            if (testingStuffInternal.SimulateActiveByOtherNodeStatus_UpdateConfigurations)
            {
                taskState = TaskStatus.ActiveByOtherNode;
            }
            else if (testingStuffInternal.SimulateDisableNodeStatus_UpdateConfigurations)
            {
                taskState = TaskStatus.Disabled;
            }
            else if (testingStuffInternal.SimulateActiveByCurrentNode_UpdateConfigurations)
            {
                taskState = TaskStatus.ActiveByCurrentNode;
            }
        }

        UpdatePeriodicBackup(databaseName, databaseId, configuration.TaskId, configuration, taskState);
    }

    private void UpdatePeriodicBackup(string databaseName, string databaseId, long taskId,
        PeriodicBackupConfiguration newConfiguration,
        TaskStatus taskState)
    {
        Debug.Assert(taskId == newConfiguration.TaskId);

        var name = databaseName;
        var existingBackupState = GetDatabaseState(databaseId, taskId);

        if (existingBackupState == null)
        {
            var backupState = new DatabaseBackupState(databaseName, databaseId, newConfiguration, ShardHelper.IsShardName(databaseName), _serverStore);

            if (taskState == TaskStatus.ActiveByCurrentNode)
                backupState.NextBackup = backupState.GetNextBackupDetails(backupState.GetMostUpdatedLocalBackupStatus(taskId, backupState.BackupStatus, databaseName), skipErrorLog: false);
            else if (taskState is TaskStatus.Disabled or TaskStatus.ActiveByOtherNode)
                backupState.Stale.Raise();

            RegisterNewBackup(backupState);
            return;
        }

        var previousConfiguration = existingBackupState.Configuration;
        existingBackupState.Configuration = newConfiguration;

        if (BackupHelper.BackupTypeChanged(previousConfiguration, newConfiguration))
        {
            existingBackupState.BackupStatus = new PeriodicBackupStatus
            {
                TaskId = newConfiguration.TaskId,
                BackupType = newConfiguration.BackupType
            };
        }

        switch (taskState)
        {
            case TaskStatus.Disabled:
                existingBackupState.Stale.Raise();
                existingBackupState.NextBackup = null;
                existingBackupState.CancelRunningBackup(CancelReasonDisabled);
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', will cancel the backup for it.");

                return;
            case TaskStatus.ActiveByOtherNode:
                existingBackupState.Stale.Raise();
                existingBackupState.NextBackup = null;
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', will skip polling for it.");

                return;

            case TaskStatus.MissingResponsibleNode:
                return;

            case TaskStatus.ActiveByCurrentNode:
                if (existingBackupState.Stale)
                {
                    existingBackupState.Stale.Lower();
                    Enqueue(existingBackupState);
                }

                if (existingBackupState.RunningTask != null)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Backup task '{taskId}' state is '{taskState}', and currently are being executed since '{existingBackupState.StartTimeInUtc}'.");
                    return;
                }

                if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false && existingBackupState.NextBackup != null)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Backup task '{taskId}' state is '{taskState}', the task doesn't have frequency changes and has scheduled backup, will continue to execute by the current node '{_serverStore.NodeTag}'.");

                    return;
                }

                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', the task has frequency changes or doesn't have scheduled backup, the timer will be rearranged and the task will be executed by current node '{_serverStore.NodeTag}'.");


                var localBackupStatus = existingBackupState.GetMostUpdatedLocalBackupStatus(taskId, inMemoryBackupStatus: null, name);
                existingBackupState.NextBackup = existingBackupState.GetNextBackupDetails(localBackupStatus, skipErrorLog: false);

                return;

            default:
                throw new ArgumentOutOfRangeException(nameof(taskState), taskState, null);
        }
    }

    public TaskStatus GetTaskStatus(PeriodicBackupConfiguration configuration, string databaseName, out string responsibleNodeTag, bool disableLog = false, ClusterOperationContext context = null)
    {
        if (configuration.Disabled)
        {
            responsibleNodeTag = null;
            return TaskStatus.Disabled;
        }

        if (configuration.HasBackup() == false)
        {
            if (disableLog == false)
            {
                var message = $"All backup destinations are disabled for backup task id: {configuration.TaskId}";
                _serverStore.NotificationCenter.Add(AlertRaised.Create(
                    databaseName,
                    "Periodic Backup",
                    message,
                    AlertReason.PeriodicBackup,
                    NotificationSeverity.Info));
            }

            responsibleNodeTag = null;
            return TaskStatus.Disabled;
        }

        responsibleNodeTag = context == null
            ? BackupUtils.GetResponsibleNodeTag(_serverStore, databaseName, configuration.TaskId)
            : BackupUtils.GetResponsibleNodeTag(context, databaseName, configuration.TaskId);
        if (responsibleNodeTag == null)
        {
            if (TryGetTestingStuff(databaseName, out var testingStuffInternal))
            {
                testingStuffInternal.OnMissingResponsibleNode?.Invoke();
            }

            return TaskStatus.MissingResponsibleNode;
        }

        if (responsibleNodeTag == _serverStore.NodeTag)
            return TaskStatus.ActiveByCurrentNode;

        if (disableLog == false && _logger.IsDebugEnabled)
            _logger.Debug($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                          $"by '{responsibleNodeTag}' node and not the current node ({_serverStore.NodeTag})");

        return TaskStatus.ActiveByOtherNode;
    }

    private static string GetBackupTypeText(bool isFullBackup, BackupType backupType)
    {
        if (backupType == BackupType.Backup)
        {
            return isFullBackup ? "Full" : "Incremental";
        }

        return isFullBackup ? "Snapshot" : "Incremental Snapshot";
    }

    public string WhoseTaskIsIt(DocumentDatabase database, long taskId)
    {
        return WhoseTaskIsIt(GetDatabaseState(GetDatabaseId(database), taskId), taskId);
    }

    private string WhoseTaskIsIt(DatabaseBackupState databaseBackupState, long taskId)
    {
        if (databaseBackupState == null)
        {
            throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
        }

        if (databaseBackupState.Configuration.Disabled)
        {
            throw new InvalidOperationException($"Backup task id: {taskId} is disabled");
        }

        if (databaseBackupState.Configuration.HasBackup() == false)
        {
            throw new InvalidOperationException($"All backup destinations are disabled for backup task id: {taskId}");
        }

        return BackupUtils.GetResponsibleNodeTag(_serverStore, databaseBackupState.DatabaseName, databaseBackupState.Configuration.TaskId);
    }

    public DatabaseBackupState GetDatabaseStateByTaskId(DocumentDatabase database, long taskId)
    {
        return GetDatabaseState(GetDatabaseId(database), taskId);
    }

    internal DatabaseBackupState GetDatabaseState(string databaseId, long taskId)
    {
        if (BackupsPerDatabaseId.TryGetValue(databaseId, out var backupStates) &&
            backupStates.TryGetValue(taskId, out var databaseBackupState))
        {
            return databaseBackupState;
        }

        return null;
    }

    public List<DatabaseBackupState> GetDatabaseBackups(DocumentDatabase database)
    {
        return GetDatabaseBackupsById(GetDatabaseId(database));
    }

    internal List<DatabaseBackupState> GetDatabaseBackupsById(string databaseId)
    {
        return BackupsPerDatabaseId.TryGetValue(databaseId, out var backupStates) ? backupStates.Values.ToList() : [];
    }

    public NextBackup GetNextBackupDetails(long taskId, DocumentDatabase database, PeriodicBackupStatus backupStatus, out string tag)
    {
        tag = null;

        var state = GetDatabaseState(GetDatabaseId(database), taskId);
        return state == null ? null : state.GetNextBackupDetails(backupStatus, out tag);
    }

    public enum TaskStatus
    {
        Disabled,
        ActiveByCurrentNode,
        ActiveByOtherNode,
        MissingResponsibleNode
    }

    public BackupInfo GetBackupInfo(DocumentDatabase database)
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            return GetBackupInfo(context, database.Name, GetDatabaseId(database));
        }
    }

    public BackupInfo GetBackupInfo(ClusterOperationContext context, DocumentDatabase database)
    {
        return GetBackupInfo(context, database.Name, GetDatabaseId(database));
    }

    private BackupInfo GetBackupInfo(ClusterOperationContext context, string databaseName, string databaseId)
    {
        List<DatabaseBackupState> BackupStates = null;
        if (databaseId != null && BackupsPerDatabaseId.TryGetValue(databaseId, out var backupStates))
        {
            BackupStates = backupStates.Values.ToList();
        }

        return BackupUtils.GetBackupInfo(
            new BackupUtils.BackupInfoParameters
            {
                Context = context,
                DatabaseBackupStates = BackupStates,
                DatabaseName = databaseName
            }
        );
    }

    private bool AreAllServerPoliciesCompliant(DateTime now, string databaseName)
    {
        foreach (var backupPolicy in _serverBackupPolicies)
        {
            if (backupPolicy.CanDoBackup(_serverStore, now, out string reason, out var category) == false)
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug(reason);

                Decisions.AddServerDecision(category, backupPolicy.Name, reason, now);

                if (TryGetTestingStuff(databaseName, out var serverViolationTestingStuff))
                {
                    serverViolationTestingStuff.OnServerPolicyViolation?.Invoke(backupPolicy, reason);
                }

                return false;
            }
        }

        return true;
    }

    private bool AreAllDatabasePoliciesCompliant(DatabaseBackupState backupState, DateTime now, out IDatabaseBackupPolicy failedPolicy)
    {
        failedPolicy = null;

        if (TryGetTestingStuff(backupState.DatabaseName, out var testingStuffInternal) &&
            testingStuffInternal.SimulateActiveByOtherNodeStatus_Reschedule)
        {
            var reason = $"Cannot start backup {backupState} because SimulateActiveByOtherNodeStatus_Reschedule is set.";
            backupState.AddToDecisionLog(BackupDecisionCategory.Waiting, BackupShouldRunOnThisNodePolicy.Instance.Name, reason, now);
            UpdatePeriodicBackup(backupState.DatabaseName, backupState.DatabaseId, backupState.Configuration.TaskId, backupState.Configuration, TaskStatus.ActiveByOtherNode);
            return false;
        }

        using (_serverStore.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var backupPolicy in _databaseBackupPolicies)
            {
                if (backupPolicy.CanDoBackup(context, _serverStore, backupState, now, out string reason, out var category) == false)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"{backupState}: {reason}");

                    backupState.AddToDecisionLog(category, backupPolicy.Name, reason, now);

                    if (TryGetTestingStuff(backupState.DatabaseName, out var dbViolationTestingStuff))
                    {
                        dbViolationTestingStuff.OnDatabasePolicyViolation?.Invoke(backupPolicy, reason, backupState);
                    }

                    failedPolicy = backupPolicy;
                    return false;
                }
            }

            return true;
        }
    }

    public void Dispose()
    {
        var thread = _thread;
        if (thread != null && thread != PoolOfThreads.LongRunningWork.Current)
            thread.Join(int.MaxValue);

    }

    public RunningBackup OnGoingBackup(DocumentDatabase database, long taskId)
    {
        var state = GetDatabaseState(GetDatabaseId(database), taskId);
        if (state == null)
            return null;

        var runningTask = state.RunningTask;
        if (state.Running == false || runningTask == null)
            return null;

        return new RunningBackup
        {
            StartTime = state.StartTimeInUtc,
            IsFull = state.RunningBackupStatus?.IsFull ?? false,
            RunningBackupTaskId = runningTask.Id
        };
    }

    internal List<PeriodicBackupInfo> GetPeriodicBackupsInformation(DocumentDatabase database)
    {
        return GetPeriodicBackupsInformationById(GetDatabaseId(database));
    }

    internal List<PeriodicBackupInfo> GetPeriodicBackupsInformationById(string databaseId)
    {
        if (BackupsPerDatabaseId.TryGetValue(databaseId, out var backupStates) == false)
            return new List<PeriodicBackupInfo>();

        return backupStates.Values
            .Select(x => new PeriodicBackupInfo
            {
                Database = x.DatabaseName,
                TaskId = x.Configuration.TaskId,
                Name = x.Configuration.Name,
                FullBackupFrequency = x.Configuration.FullBackupFrequency,
                IncrementalBackupFrequency = x.Configuration.IncrementalBackupFrequency,
                NextBackup = x.NextBackup,
                CreatedAt = x.CreatedAt,
            })
            .ToList();
    }

    public sealed class RunningBackupTask
    {
        public Task Task { get; set; }

        public long Id { get; set; }
    }


    internal TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff(this);
    }


    public class TestingStuff
    {
        private readonly ServerBackupRunner _runner;

        public TestingStuff()
        {
            DatabaseTestingStuffInternals = new ConcurrentDictionary<string, TestingStuffInternal>();
        }

        internal TestingStuff(ServerBackupRunner runner) : this()
        {
            _runner = runner;
        }

        internal ConcurrentDictionary<string, TestingStuffInternal> DatabaseTestingStuffInternals { get; set; }

        internal int CountInQueue(DatabaseBackupState backupState) => _runner._backupQueue.Count(x => ReferenceEquals(x, backupState));
    }

    internal class TestingStuffInternal
    {
        internal Action OnMissingResponsibleNode;
        internal bool SimulateActiveByOtherNodeStatus_Reschedule;
        internal bool SimulateActiveByOtherNodeStatus_UpdateConfigurations;
        internal bool SimulateActiveByCurrentNode_UpdateConfigurations;
        internal bool SimulateDisableNodeStatus_UpdateConfigurations;
        internal bool SimulateFailedBackup;
        internal bool BackupStatusFromMemoryOnly;

        internal TaskCompletionSource<object> OnBackupTaskRunHoldBackupExecution;

        internal Action AfterBackupBatchCompleted;

        internal ManualResetEvent HoldBackupFromFinishing;
        internal Action<IDatabaseBackupPolicy, string, DatabaseBackupState> OnDatabasePolicyViolation;
        internal Action<IServerBackupPolicy, string> OnServerPolicyViolation;
        internal Action<DatabaseBackupState> OnBeforeBackupStarted;
    }

}
