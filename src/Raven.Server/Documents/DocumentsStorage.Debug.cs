﻿using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Documents;

namespace Raven.Server.Documents
{
    public partial class DocumentsStorage
    {
        public Debugging ForDebug => new Debugging(this);

        public class Debugging
        {
            private readonly DocumentsStorage _storage;

            public Debugging(DocumentsStorage storage)
            {
                _storage = storage;
            }
            // Used to delete corrupted document from the JS admin console
            public void DeleteDocumentByEtag(long etag)
            {
                using (_storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var table = context.DocumentsTable(_storage);
                    var index = _storage.DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice];

                    if (table.FindByIndex(index, etag, out var reader))
                    {
                        var doc = _storage.TableValueToDocument(context, ref reader, DocumentFields.Id);
                        _storage.Delete(context, doc.Id, DocumentFlags.None);
                        tx.Commit();
                    }
                }
            }
        }
    }
}
