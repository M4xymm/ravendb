﻿using System;
using System.Globalization;
using System.IO;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class DocumentReplicationItem : ReplicationBatchItem
    {
        public BlittableJsonReaderObject Data;
        public LazyStringValue Collection;
        public LazyStringValue Id;
        public DocumentFlags Flags;

        public override long Size => GetReplicationBatchItemSize();

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();

            if (Flags.Contain(DocumentFlags.Revision))
            {
                djv[nameof(Type)] = "Revision";
            }

            djv[nameof(Collection)] = Collection?.ToString(CultureInfo.InvariantCulture) ?? CollectionName.GetCollectionName(Data);
            djv[nameof(Id)] = Id.ToString(CultureInfo.InvariantCulture);
            djv[nameof(Flags)] = Flags;
            return djv;
        }

        public static DocumentReplicationItem From(Document doc, DocumentsOperationContext context)
        {
            var result = new DocumentReplicationItem
            {
                Type = ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Data = doc.Data,
                Id = doc.Id,
                Flags = doc.Flags,
                TransactionMarker = doc.TransactionMarker,
                LastModifiedTicks = doc.LastModified.Ticks,
            };

            return result;
        }

        public static DocumentReplicationItem From(DocumentConflict doc)
        {
            return new DocumentReplicationItem
            {
                Type = doc.Doc == null ? ReplicationItemType.DocumentTombstone : ReplicationItemType.Document,
                Etag = doc.Etag,
                ChangeVector = doc.ChangeVector,
                Collection = doc.Collection,
                Data = doc.Doc,
                Id = doc.Id,
                Flags = doc.Flags,
                LastModifiedTicks = doc.LastModified.Ticks,
                TransactionMarker = -1 // not relevant
            };
        }

        public override long AssertChangeVectorSize() => GetReplicationBatchItemSize(includeDetails: false);

        private long GetReplicationBatchItemSize(bool includeDetails = true)
        {
            var size = base.Size + // common 

                    sizeof(long) + // Last modified ticks

                    sizeof(DocumentFlags) +

                    sizeof(int) + // size of document ID
                    Id.Size +

                    sizeof(int); // size of document

            if (includeDetails)
            {
                if (Data != null)
                    size += Data.Size;

                if (Collection != null)
                    size += Collection.Size + sizeof(int);
            }

            return size;
        }

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                if (AssertChangeVectorSize() > tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(this, Id);

                var tempBufferPos = WriteCommon(changeVector, pTemp);

                *(long*)(pTemp + tempBufferPos) = LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(DocumentFlags*)(pTemp + tempBufferPos) = Flags;
                tempBufferPos += sizeof(DocumentFlags);

                *(int*)(pTemp + tempBufferPos) = Id.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, Id.Buffer, Id.Size);
                tempBufferPos += Id.Size;

                if (Data != null)
                {
                    *(int*)(pTemp + tempBufferPos) = Data.Size;
                    tempBufferPos += sizeof(int);

                    var docReadPos = 0;
                    while (docReadPos < Data.Size)
                    {
                        var sizeToCopy = Math.Min(Data.Size - docReadPos, tempBuffer.Length - tempBufferPos);
                        if (sizeToCopy == 0) // buffer is full, need to flush it
                        {
                            stream.Write(tempBuffer, 0, tempBufferPos);
                            tempBufferPos = 0;
                            continue;
                        }
                        Memory.Copy(pTemp + tempBufferPos, Data.BasePointer + docReadPos, sizeToCopy);
                        tempBufferPos += sizeToCopy;
                        docReadPos += sizeToCopy;
                    }

                    if (Flags.Contain(DocumentFlags.Revision))
                        stats.RecordRevisionOutput(Size);
                    else
                        stats.RecordDocumentOutput(Size);
                }
                else
                {
                    int dataSize;
                    if (Type == ReplicationItemType.DocumentTombstone)
                        dataSize = -1;
                    else if ((Flags & DocumentFlags.DeleteRevision) == DocumentFlags.DeleteRevision)
                        dataSize = -2;
                    else
                        throw new InvalidDataException("Cannot write document with empty data.");
                    *(int*)(pTemp + tempBufferPos) = dataSize;
                    tempBufferPos += sizeof(int);

                    if (Collection == null) //precaution
                        throw new InvalidDataException("Cannot write item with empty collection name...");

                    *(int*)(pTemp + tempBufferPos) = Collection.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, Collection.Buffer, Collection.Size);
                    tempBufferPos += Collection.Size;

                    stats.RecordDocumentTombstoneOutput(Size);
                }

                stream.Write(tempBuffer, 0, tempBufferPos);
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            var scope = Type == ReplicationItemType.Document ?
                stats.For(ReplicationOperation.Incoming.DocumentRead, start: false) :
                stats.For(ReplicationOperation.Incoming.TombstoneRead, start: false);

            using (scope.Start())
            {
                LastModifiedTicks = *(long*)Reader.ReadExactly(sizeof(long));

                Flags = *(DocumentFlags*)Reader.ReadExactly(sizeof(DocumentFlags)) | DocumentFlags.FromReplication;

                SetLazyStringValueFromString(context, out Id);

                var documentSize = *(int*)Reader.ReadExactly(sizeof(int));
                if (documentSize != -1) //if -1, then this is a tombstone
                {
                    var mem = Reader.AllocateMemory(documentSize);
                    Reader.ReadExactly(mem, documentSize);

                    Data = new BlittableJsonReaderObject(mem, documentSize, context);
                    Data.BlittableValidation();

                    if (Flags.Contain(DocumentFlags.Revision))
                        stats.RecordRevisionRead(Size);
                    else
                        stats.RecordDocumentRead(Size);
                }
                else
                {
                    SetLazyStringValueFromString(context, out Collection);

                    stats.RecordDocumentTombstoneRead(Size);
                }
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
        {
            return new DocumentReplicationItem
            {
                Id = Id.Clone(context),
                Data = Data?.Clone(context),
                Collection = Collection?.Clone(context),
                Flags = Flags
            };
        }

        protected override void InnerDispose()
        {
            Data?.Dispose();
            Id?.Dispose();
            Collection?.Dispose();
        }

        public override string ToString()
        {
            var type = Data == null ? "Tombstone" : "Document";

            if (Flags.Contain(DocumentFlags.Revision))
            {
                type = "Revision";
            }
            if (Flags.Contain(DocumentFlags.DeleteRevision))
            {
                type = "Deleted Revision";
            }

            return $"{Id} : {ChangeVector} ({type})";
        }
    }
}
