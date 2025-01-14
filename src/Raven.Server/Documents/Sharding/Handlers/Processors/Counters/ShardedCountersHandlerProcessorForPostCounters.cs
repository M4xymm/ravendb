﻿using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Counters;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Counters
{
    internal sealed class ShardedCountersHandlerProcessorForPostCounters : AbstractCountersHandlerProcessorForPostCounters<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCountersHandlerProcessorForPostCounters([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<CountersDetail> ApplyCountersOperationsAsync(TransactionOperationContext context, CounterBatch counterBatch)
        {
            var commandsPerShard = new Dictionary<int, CounterBatchOperation.CounterBatchCommand>();

            var shardsToPositions = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, GetDocIds(counterBatch));
            foreach (var (shardNumber, idsByShard) in shardsToPositions)
            {
                var countersBatchForShard = new CounterBatch()
                {
                    FromEtl = counterBatch.FromEtl,
                    ReplyWithAllNodesValues = counterBatch.ReplyWithAllNodesValues,
                    Documents = new()
                };

                foreach (var pos in idsByShard.Positions)
                {
                    countersBatchForShard.Documents.Add(counterBatch.Documents[pos]);
                }
                
                commandsPerShard[shardNumber] = new CounterBatchOperation.CounterBatchCommand(RequestHandler.ShardExecutor.Conventions, countersBatchForShard);
                
            }

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                return await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
                    new ShardedCounterBatchOperation(RequestHandler.HttpContext.Request, commandsPerShard), token.Token);
            }
        }

        private List<string> GetDocIds(CounterBatch counterBatch)
        {
            var docIds = new List<string>();

            foreach (var doc in counterBatch.Documents)
            {
                docIds.Add(doc.DocumentId);
            }

            return docIds;
        }
    }

    internal readonly struct ShardedCounterBatchOperation : IShardedOperation<CountersDetail>
    {
        private readonly HttpRequest _request;
        private readonly Dictionary<int, CounterBatchOperation.CounterBatchCommand> _commandsPerShard;
        
        internal ShardedCounterBatchOperation(HttpRequest request, Dictionary<int, CounterBatchOperation.CounterBatchCommand> commandsPerShard)
        {
            _request = request;
            _commandsPerShard = commandsPerShard;
        }

        public HttpRequest HttpRequest => _request;

        public CountersDetail Combine(Dictionary<int, ShardExecutionResult<CountersDetail>> results)
        {
            var combined = new CountersDetail();
            var counterDetailsResult = new List<CounterDetail>();

            foreach (var countersDetail in results.Values)
            {
                counterDetailsResult.AddRange(countersDetail.Result.Counters);
            }

            combined.Counters = counterDetailsResult;

            return combined;
        }

        public RavenCommand<CountersDetail> CreateCommandForShard(int shardNumber) => _commandsPerShard[shardNumber];
    }
}
