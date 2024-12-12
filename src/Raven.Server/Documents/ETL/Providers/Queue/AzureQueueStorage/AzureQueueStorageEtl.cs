﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Storage.Queues;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Queue.AzureQueueStorage;

public sealed class AzureQueueStorageEtl : QueueEtl<AzureQueueStorageItem>
{
    private readonly HashSet<string> _alreadyCreatedQueues = new();
    private readonly Dictionary<string, QueueClient> _queueClients = new();

    public AzureQueueStorageEtl(Transformation transformation, QueueEtlConfiguration configuration,
        DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore)
    {
    }

    protected override
        EtlTransformer<QueueItem, QueueWithItems<AzureQueueStorageItem>, EtlStatsScope, EtlPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new AzureQueueStorageDocumentTransformer<AzureQueueStorageItem>(Transformation, Database, context,
            Configuration);
    }

    protected override int PublishMessages(List<QueueWithItems<AzureQueueStorageItem>> itemsPerQueue,
        BlittableJsonEventBinaryFormatter formatter, out List<string> idsToDelete)
    {
        if (itemsPerQueue.Count == 0)
        {
            idsToDelete = null;
            return 0;
        }

        var tooLargeDocsErrors = new Queue<EtlErrorInfo>();
        idsToDelete = [];
        int count = 0;

        foreach (QueueWithItems<AzureQueueStorageItem> queue in itemsPerQueue)
        {
            string queueName = queue.Name.ToLower();

            if (_queueClients.TryGetValue(queueName, out QueueClient queueClient) == false)
            {
                queueClient =
                    QueueBrokerConnectionHelper.CreateAzureQueueStorageClient(
                        Configuration.Connection.AzureQueueStorageConnectionSettings, queueName);
                _queueClients.Add(queueName, queueClient);
            }

            if (Configuration.SkipAutomaticQueueDeclaration == false)
                CreateQueueIfNotExists(queueClient);

            foreach (AzureQueueStorageItem queueItem in queue.Items)
            {
                CancellationToken.ThrowIfCancellationRequested();
                string base64CloudEvent = CreateBase64CloudEvent(queueItem);

                TimeSpan? timeToLive = Database.Configuration.Etl.AzureQueueStorageTimeToLive.AsTimeSpan;
                TimeSpan? visibilityTimeout = Database.Configuration.Etl.AzureQueueStorageVisibilityTimeout.AsTimeSpan;

                try
                {
                    queueClient.SendMessage(base64CloudEvent, visibilityTimeout, timeToLive);
                    count++;

                    if (queue.DeleteProcessedDocuments)
                        idsToDelete.Add(queueItem.DocumentId);
                }
                catch (Azure.RequestFailedException ex)
                {
                    if (ex.ErrorCode is "RequestBodyTooLarge")
                    {
                        tooLargeDocsErrors.Enqueue(new EtlErrorInfo()
                        {
                            Date = DateTime.UtcNow,
                            DocumentId = queueItem.DocumentId,
                            Error = ex.Message 
                        });
                    }
                    else
                    {
                        throw new QueueLoadException(
                            $"Failed to deliver message, Azure error code: '{ex.ErrorCode}', error reason: '{ex.Message}' for document with id: '{queueItem.DocumentId}'",
                            ex);
                    }

                }
                catch (Exception ex)
                {
                    throw new QueueLoadException($"Failed to deliver message, error reason: '{ex.Message}'", ex);
                }
            }

            if (tooLargeDocsErrors.Count > 0)
            {
                Database.NotificationCenter.EtlNotifications.AddLoadErrors(Tag, Name, tooLargeDocsErrors,
                    "ETL has partially loaded the data. " +
                    "Some of the documents were too big (>64KB) to be handled by Azure Queue Storage. " +
                    "It caused load errors, that have been skipped. ");
            }

        }

        return count;
    }

    private string CreateBase64CloudEvent(AzureQueueStorageItem queueItem)
    {
        var cloudEvent = CreateCloudEvent(queueItem);
        var options = new JsonSerializerOptions { Converters = { CloudEventConverter.Instance } };
        byte[] cloudEventBytes = JsonSerializer.SerializeToUtf8Bytes(cloudEvent, options);
        return Convert.ToBase64String(cloudEventBytes);
    }

    protected override void OnProcessStopped()
    {
        _queueClients.Clear();
        _alreadyCreatedQueues.Clear();
    }

    private void CreateQueueIfNotExists(QueueClient queueClient)
    {
        if (_alreadyCreatedQueues.Contains(queueClient.Name)) return;
        try
        {
            queueClient.CreateIfNotExists();
            _alreadyCreatedQueues.Add(queueClient.Name);
        }
        catch (Azure.RequestFailedException ex)
        {
            throw new QueueLoadException(
                $"Failed to deliver message, Azure error code: '{ex.ErrorCode}', error reason: '{ex.Message}'", ex);
        }
    }
}
