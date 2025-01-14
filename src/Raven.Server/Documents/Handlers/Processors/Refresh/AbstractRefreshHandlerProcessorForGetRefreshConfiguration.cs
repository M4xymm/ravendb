﻿using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Refresh;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Refresh;

internal abstract class AbstractRefreshHandlerProcessorForGetRefreshConfiguration<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractRefreshHandlerProcessorForGetRefreshConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract RefreshConfiguration GetRefreshConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var refreshConfiguration = GetRefreshConfiguration();

        if (refreshConfiguration == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            context.Write(writer, refreshConfiguration.ToJson());
    }
}
