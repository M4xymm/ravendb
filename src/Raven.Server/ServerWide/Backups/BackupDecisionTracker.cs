using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.ServerWide.Backups;

public sealed class BackupDecisionTracker
{
    public const int MaxDecisionLogSize = 1024;

    internal const string CancelReasonDisabled = "disabled";
    internal const string CancelReasonTaskDeleted = "task-deleted";
    internal const string CancelReasonDelay = "delay";
    internal const string CancelReasonUnloaded = "unloaded";
    internal const string CancelReasonUser = "user";

    internal const string DisableOngoingTasksDetail = "DisableOngoingTasks";

    internal const string ConcurrentBackupDetail = "ConcurrentBackup";

    private readonly ServerBackupRunner _runner;

    private readonly ServerStore _serverStore;

    private readonly RavenLogger _logger;

    private readonly BackupDecisionLog _serverDecisionLog = new(MaxDecisionLogSize);

    private readonly object _decisionEtagLock = new();

    private long _lastDecisionId;

    private long _lastDecisionEtag;

    private TaskCompletionSource<object> _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly object _summaryLock = new();

    private BackupDecisionLogSummary _summary;

    private long _summaryVersion;

    private readonly object _outcomesLock = new();

    private long _failedCount;

    private long _completedCount;

    private BackupDecisionLogSummary.Outcome _latestFailure;

    private BackupDecisionLogSummary.Outcome _latestCompletion;

    private DateTime _runnerStartTime;

    internal BackupDecisionTracker(ServerBackupRunner runner, ServerStore serverStore, RavenLogger logger)
    {
        _runner = runner;
        _serverStore = serverStore;
        _logger = logger;
    }

    internal void Start()
    {
        _runnerStartTime = DateTime.UtcNow;
    }

    internal static string GetCancelReasonText(string cancelReason)
    {
        return cancelReason switch
        {
            CancelReasonDisabled => "The task was disabled while its backup was running.",
            CancelReasonTaskDeleted => "The task or its database was deleted, or the database was removed from this node, while its backup was starting or running.",
            CancelReasonDelay => "The backup was delayed.",
            CancelReasonUnloaded => "The database or the server shut down while the backup was running.",
            _ => "The backup was stopped by a user or through the API."
        };
    }

    internal BackupDecision AddServerDecision(BackupDecisionCategory category, string detail, string reason, DateTime now)
    {
        return AddDecision(_serverDecisionLog, category, detail, reason, now);
    }

    internal BackupDecision AddDecision(BackupDecisionLog decisionLog, BackupDecisionCategory category, string detail, string reason, DateTime now)
    {
        lock (_decisionEtagLock)
        {
            return decisionLog.Add(category, detail, reason, now, NextDecisionId, NextDecisionEtag);
        }
    }

    private long NextDecisionId()
    {
        return Interlocked.Increment(ref _lastDecisionId);
    }

    private long NextDecisionEtag()
    {
        _lastDecisionEtag++;
        _changed.TrySetResult(null);

        return _lastDecisionEtag;
    }

    internal long LastDecisionEtag
    {
        get
        {
            lock (_decisionEtagLock)
                return _lastDecisionEtag;
        }
    }

    internal long LastDecisionId => Interlocked.Read(ref _lastDecisionId);

    internal long SummaryVersion
    {
        get
        {
            lock (_decisionEtagLock)
                return _summaryVersion;
        }
    }

    internal Task WaitForChangeAsync(long etag, long summaryVersion)
    {
        lock (_decisionEtagLock)
        {
            if (_lastDecisionEtag > etag || _summaryVersion > summaryVersion)
                return Task.CompletedTask;

            if (_changed.Task.IsCompleted)
                _changed = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            return _changed.Task;
        }
    }

    internal void RefreshSummary()
    {
        try
        {
            lock (_summaryLock)
            {
                var summary = GetSummary(DateTime.UtcNow);

                lock (_decisionEtagLock)
                {
                    _summary = summary;
                    _summaryVersion++;
                    _changed.TrySetResult(null);
                }
            }
        }
        catch (Exception e)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn("Failed to refresh the backup decision log summary.", e);
        }
    }

    internal void OnBackupOutcome(DatabaseBackupState backupState, BackupDecision decision)
    {
        var outcome = new BackupDecisionLogSummary.Outcome
        {
            Database = backupState.OriginalDatabaseName,
            TaskId = backupState.Configuration.TaskId,
            TaskName = backupState.Configuration.Name,
            Time = decision.LastTime,
            Error = decision.Category == BackupDecisionCategory.Failed ? decision.Reason : null
        };

        lock (_outcomesLock)
        {
            if (decision.Category == BackupDecisionCategory.Failed)
            {
                _failedCount++;
                _latestFailure = outcome;
            }
            else
            {
                _completedCount++;
                _latestCompletion = outcome;
            }
        }
    }

    internal BackupDecisionLogDetails GetDecisionLogDetails(BackupDecisionLogFilter filter, long? afterEtag)
    {
        var nodeTag = _serverStore.NodeTag;
        var now = DateTime.UtcNow;

        var details = new BackupDecisionLogDetails
        {
            NodeTag = nodeTag,
            RunnerStartTime = _runnerStartTime,
            Summary = _summary ?? GetSummary(now)
        };

        var includeFilterOptions = afterEtag == null;
        var matches = new List<BackupDecisionLogEntry>();
        var databases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tasks = new Dictionary<(string Database, long TaskId), BackupDecisionLogTask>();

        foreach (var decision in _serverDecisionLog.GetDecisions(afterEtag))
            AddIfMatches(BackupDecisionLogEntry.Create(decision, nodeTag));

        foreach (var (_, backupStates) in _runner.BackupsPerDatabaseId)
        {
            foreach (var (taskId, backupState) in backupStates)
            {
                if (includeFilterOptions)
                {
                    databases.Add(backupState.OriginalDatabaseName);
                    tasks.TryAdd((backupState.OriginalDatabaseName, taskId), new BackupDecisionLogTask
                    {
                        Database = backupState.OriginalDatabaseName,
                        TaskId = taskId,
                        TaskName = backupState.Configuration.Name
                    });
                }

                if (filter.MatchesTask(backupState.OriginalDatabaseName, taskId) == false)
                    continue;

                foreach (var decision in backupState.GetDecisionLog(afterEtag))
                    AddIfMatches(BackupDecisionLogEntry.Create(decision, nodeTag, backupState));
            }
        }

        details.TotalResults = matches.Count;
        details.Entries = matches
            .OrderByDescending(x => x.LastTime)
            .ThenByDescending(x => x.Id)
            .Take(afterEtag == null ? filter.Take : int.MaxValue)
            .ToList();
        details.Databases = includeFilterOptions ? databases.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList() : null;
        details.Tasks = includeFilterOptions ? tasks.Values.OrderBy(x => x.Database, StringComparer.OrdinalIgnoreCase).ThenBy(x => x.TaskId).ToList() : null;

        return details;

        void AddIfMatches(BackupDecisionLogEntry entry)
        {
            if (filter.Matches(entry))
                matches.Add(entry);
        }
    }

    private BackupDecisionLogSummary GetSummary(DateTime now)
    {
        var summary = new BackupDecisionLogSummary();
        summary.Running.Max = _serverStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups;

        var activeWindow = _serverStore.Configuration.Backup.BackupRunnerFrequency.AsTimeSpan * 2;

        if (_serverDecisionLog.TryGetLatest(out var serverDecision) &&
            serverDecision.Category == BackupDecisionCategory.HeldBack &&
            now - serverDecision.LastTime <= activeWindow)
        {
            summary.ServerBlock = new BackupDecisionLogSummary.ServerBlockSummary
            {
                Detail = serverDecision.Detail,
                Reason = serverDecision.Reason,
                Since = serverDecision.FirstTime
            };
        }

        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        foreach (var (_, backupStates) in _runner.BackupsPerDatabaseId)
        {
            foreach (var (taskId, backupState) in backupStates)
            {
                if (backupState.Stale)
                    continue;

                if (backupState.Running)
                    continue;

                if (backupState.TryGetLatestDecision(out var latest) && now - latest.LastTime <= activeWindow)
                {
                    if (latest.Category == BackupDecisionCategory.HeldBack)
                    {
                        summary.HeldBack.Count++;

                        if (summary.HeldBack.Longest == null || latest.FirstTime < summary.HeldBack.Longest.Since)
                        {
                            summary.HeldBack.Longest = new BackupDecisionLogSummary.HeldBackTask
                            {
                                Database = backupState.OriginalDatabaseName,
                                TaskId = taskId,
                                TaskName = backupState.Configuration.Name,
                                Detail = latest.Detail,
                                Reason = latest.Reason,
                                Since = latest.FirstTime
                            };
                        }

                        continue;
                    }

                    if (latest.Category == BackupDecisionCategory.Waiting && latest.Detail == ConcurrentBackupDetail)
                    {
                        summary.Running.WaitingForSlot++;
                        continue;
                    }
                }

                var nextBackup = backupState.NextBackup;
                if (nextBackup == null || nextBackup.DateTime <= now)
                    continue;

                if (_runner.IsActiveOnThisNode(context, backupState) == false)
                    continue;

                summary.WaitingForSchedule.Count++;

                if (summary.WaitingForSchedule.NextBackupUtc == null || nextBackup.DateTime < summary.WaitingForSchedule.NextBackupUtc)
                {
                    summary.WaitingForSchedule.NextBackupUtc = nextBackup.DateTime;
                    summary.WaitingForSchedule.NextBackupDatabase = backupState.OriginalDatabaseName;
                    summary.WaitingForSchedule.NextBackupTaskName = backupState.Configuration.Name;
                }
            }
        }

        summary.Running.Current = _serverStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups;

        lock (_outcomesLock)
        {
            summary.Failed.Count = _failedCount;
            summary.Failed.Latest = _latestFailure;
            summary.Completed.Count = _completedCount;
            summary.Completed.Latest = _latestCompletion;
        }

        return summary;
    }
}
