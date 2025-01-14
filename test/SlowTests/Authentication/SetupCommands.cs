﻿// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Sparrow.Json;

namespace SlowTests.Authentication
{
    public partial class AuthenticationLetsEncryptTests
    {
        private class ClaimDomainCommand : RavenCommand<ClaimDomainResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _payload;

            public ClaimDomainCommand(DocumentConventions conventions, JsonOperationContext context, ClaimDomainInfo claimInfo)
            {
                _conventions = conventions;
                _payload = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(claimInfo, context);
            }

            public override bool IsReadRequest => false;
            

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/setup/dns-n-cert?action=claim";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _payload).ConfigureAwait(false), _conventions)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();
                Result = JsonDeserializationClient.ClaimDomainResult(response);
            }
        }

        private class ForceRenewCertCommand : RavenCommand<ForceRenewResult>, IRaftCommand
        {
            public ForceRenewCertCommand(DocumentConventions conventions, JsonOperationContext context)
            {
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/letsencrypt/force-renew";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();
                Result = JsonDeserializationClient.ForceRenewResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        private class SetupLetsEncryptCommand : RavenCommand<byte[]>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _payload;

            public SetupLetsEncryptCommand(DocumentConventions conventions, JsonOperationContext context, SetupInfo setupInfo)
            {
                _conventions = conventions;
                _payload = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(setupInfo, context);
                ResponseType = RavenCommandResponseType.Raw;
                Timeout = TimeSpan.FromMinutes(30);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/setup/letsencrypt";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _payload).ConfigureAwait(false), _conventions)
                };
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                if (response == null)
                    return;

                var ms = new MemoryStream();
                stream.CopyTo(ms);

                Result = ms.ToArray();
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
