﻿using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Sharding;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public partial class LuceneIndexReadOperation
{
    partial void AddOrderByFields(IndexQueryServerSide query, global::Lucene.Net.Documents.Document document, int doc, ref Document d)
    {
        // * for sharded queries, we'll send the order by fields separately
        // * for a map-reduce index, it's fields are the ones that are used for sorting
        if (_index.DocumentDatabase is ShardedDocumentDatabase == false || query.Metadata.OrderBy?.Length > 0 == false || _indexType.IsMapReduce())
            return;

        //https://issues.hibernatingrhinos.com/issue/RavenDB-18457
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "review after Corax is merged");

        var documentWithOrderByFields = DocumentWithOrderByFields.From(d);

        foreach (var field in query.Metadata.OrderBy)
        {
            switch (field.OrderingType)
            {
                case OrderByFieldType.Long:
                    documentWithOrderByFields.AddLongOrderByField(_searcher.IndexReader.GetLongValueFor(field.OrderByName, FieldCache_Fields.NUMERIC_UTILS_LONG_PARSER, doc, _state));
                    break;
                case OrderByFieldType.Double:
                    documentWithOrderByFields.AddDoubleOrderByField(_searcher.IndexReader.GetDoubleValueFor(field.OrderByName, FieldCache_Fields.NUMERIC_UTILS_DOUBLE_PARSER, doc, _state));
                    break;
                case OrderByFieldType.Random:
                    // we order by random when merging results from shards
                    break;
                case OrderByFieldType.Distance:
                    documentWithOrderByFields.AddDoubleOrderByField(d.Distance.Value.Distance);
                    break;
                case OrderByFieldType.Score:
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-13927 Order by score");

                    throw new NotSupportedInShardingException("Ordering by score is not supported in sharding");
                default:
                    documentWithOrderByFields.AddStringOrderByField(_searcher.IndexReader.GetStringValueFor(field.OrderByName, doc, _state));
                    break;
            }
        }

        d = documentWithOrderByFields;
    }
}
