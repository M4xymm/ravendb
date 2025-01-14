﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    /// <summary>
    /// Operation to retrieve information about a specific pull replication task.
    /// The information includes the pull replication hub definition and the current connections associated with the task.
    /// </summary>
    public sealed class GetPullReplicationTasksInfoOperation : IMaintenanceOperation<PullReplicationDefinitionAndCurrentConnections>
    {
        private readonly long _taskId;

        /// <inheritdoc cref="GetPullReplicationTasksInfoOperation"/>
        /// <param name="taskId">
        /// The unique identifier of the pull replication task to retrieve information for.
        /// </param>
        public GetPullReplicationTasksInfoOperation(long taskId)
        {
            _taskId = taskId;
        }

        public RavenCommand<PullReplicationDefinitionAndCurrentConnections> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetPullReplicationTasksInfoCommand(_taskId);
        }

        internal sealed class GetPullReplicationTasksInfoCommand : RavenCommand<PullReplicationDefinitionAndCurrentConnections>
        {
            private readonly long _taskId;

            public GetPullReplicationTasksInfoCommand(long taskId)
            {
                _taskId = taskId;
            }

            public GetPullReplicationTasksInfoCommand(long taskId, string nodeTag)
            {
                _taskId = taskId;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/tasks/pull-replication/hub?key={_taskId}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                {
                    Result = JsonDeserializationClient.PullReplicationDefinitionAndCurrentConnectionsResult(response);
                }
            }

            public override bool IsReadRequest => false;
        }
    }
}
