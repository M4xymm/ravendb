﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.DocumentsCompression
{
    /// <summary>
    /// Operation to update the documents compression configuration.
    /// </summary>
    public sealed class UpdateDocumentsCompressionConfigurationOperation : IMaintenanceOperation<DocumentCompressionConfigurationResult>
    {
        private readonly DocumentsCompressionConfiguration _documentsCompressionConfiguration;

        /// <inheritdoc cref="UpdateDocumentsCompressionConfigurationOperation"/>
        /// <param name="configuration">
        /// The <see cref="DocumentsCompressionConfiguration"/> object containing the new compression settings to apply.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        public UpdateDocumentsCompressionConfigurationOperation(DocumentsCompressionConfiguration configuration)
        {
            _documentsCompressionConfiguration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<DocumentCompressionConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdateDocumentCompressionConfigurationCommand(conventions, _documentsCompressionConfiguration);
        }

        private sealed class UpdateDocumentCompressionConfigurationCommand : RavenCommand<DocumentCompressionConfigurationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly DocumentsCompressionConfiguration _documentsCompressionConfiguration;

            public UpdateDocumentCompressionConfigurationCommand(DocumentConventions conventions, DocumentsCompressionConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _documentsCompressionConfiguration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }
            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/documents-compression/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_documentsCompressionConfiguration, ctx)).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.DocumentCompressionConfigurationOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
