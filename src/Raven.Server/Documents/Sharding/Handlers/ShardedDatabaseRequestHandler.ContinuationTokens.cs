﻿using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public partial class ShardedDatabaseRequestHandler
    {
        public class ShardedContinuationTokensHandler
        {
            private readonly ShardedDatabaseRequestHandler _handler;

            public ShardedContinuationTokensHandler(ShardedDatabaseRequestHandler handler)
            {
                _handler = handler;
            }

            public ShardedPagingContinuation GetOrCreateContinuationToken(JsonOperationContext context)
            {
                var qToken = _handler.GetStringQueryString(ContinuationToken.ContinuationTokenQueryString, required: false);
                var token = ContinuationToken.FromBase64<ShardedPagingContinuation>(context, qToken) ??
                            new ShardedPagingContinuation(_handler.DatabaseContext, _handler.GetStart(), _handler.GetPageSize());
                return token;
            }

            public ShardedPagingContinuation GetOrCreateContinuationToken(JsonOperationContext context, int start, int pageSize)
            {
                var qToken = _handler.GetStringQueryString(ContinuationToken.ContinuationTokenQueryString, required: false);
                var token = ContinuationToken.FromBase64<ShardedPagingContinuation>(context, qToken) ??
                            new ShardedPagingContinuation(_handler.DatabaseContext, start: start, pageSize: pageSize);
                return token;
            }
        }
    }
}
