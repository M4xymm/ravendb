﻿using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Debugging.Processors
{
    internal class TransactionDebugHandlerProcessorForGetClusterInfo : AbstractTransactionDebugHandlerProcessorForGetClusterInfo<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TransactionDebugHandlerProcessorForGetClusterInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}