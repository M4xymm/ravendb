﻿using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorAutoIndexClientApi(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector)]
    public void SinglesToSinglesTest() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(Singles)",
        rql: "from 'AutoVecDocs' where vector.search(Singles, $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Singles), 
                value => value.ByEmbedding([0.1f, 0.1f])));
   
    [RavenFact(RavenTestCategory.Vector)]
    public void SinglesToInt8Test() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.f32_i8(Singles))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.f32_i8(Singles), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Singles).TargetQuantization(VectorEmbeddingType.Int8), 
                value => value.ByEmbedding([0.1f, 0.1f])));
    
    [RavenFact(RavenTestCategory.Vector)]
    public void SinglesToBinaryTest() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.f32_i1(Singles))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.f32_i1(Singles), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Singles).TargetQuantization(VectorEmbeddingType.Binary), 
                value => value.ByEmbedding([0.1f, 0.1f])));

    [RavenFact(RavenTestCategory.Vector)]
    public void Int8Test() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.i8(Int8))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.i8(Int8), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Int8, VectorEmbeddingType.Int8), 
                value => value.ByEmbedding([0.1f, 0.1f])));
    
    [RavenFact(RavenTestCategory.Vector)]
    public void Int1Test() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.i1(Binary))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.i1(Binary), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithEmbedding(f => f.Binary, VectorEmbeddingType.Binary), 
                value => value.ByEmbedding([0.1f, 0.1f])));
    
    [RavenFact(RavenTestCategory.Vector)]
    public void TextToSinglesTest() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text(Text))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.text(Text), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithText(x => x.Text), 
                value => value.ByText("test")));
    
    [RavenFact(RavenTestCategory.Vector)]
    public void TextToInt8Test() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text_i8(Text))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.text_i8(Text), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithText(x => x.Text).TargetQuantization(VectorEmbeddingType.Int8), 
                value => value.ByText("test")));
    
    [RavenFact(RavenTestCategory.Vector)]
    public void TextToInt1Test() => AutoIndexingTestingBase(
        autoIndexName: "Auto/AutoVecDocs/ByVector.search(embedding.text_i1(Text))",
        rql: "from 'AutoVecDocs' where vector.search(embedding.text_i1(Text), $p0)",
        vectorWhere: docs => docs.
            VectorSearch(field => field.WithText(x => x.Text).TargetQuantization(VectorEmbeddingType.Binary), 
                value => value.ByText("test")));
    
    private void AutoIndexingTestingBase(string autoIndexName, string rql, Func<IRavenQueryable<AutoVecDoc>, IRavenQueryable<AutoVecDoc>> vectorWhere)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("Test", [1.0f, 1.0f], [-1, 1], [1, 1]));
        session.SaveChanges();
        var baseQuery = session.Query<AutoVecDoc>().Statistics(out var stats).Customize(x => x.WaitForNonStaleResults());
        baseQuery = vectorWhere(baseQuery);
        _ = baseQuery.ToList(); // evaluate

        Assert.Equal(autoIndexName, stats.IndexName);
        Assert.Equal(rql, baseQuery.ToString());
    }

    [RavenFact(RavenTestCategory.Vector)]
    public void NonExistingFieldDoesntEndWithNre()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.SaveChanges();

        var documentQueryNonExistingField = session!.Advanced.DocumentQuery<AutoVecDoc>()
            .VectorSearch(f => f.WithText("NonExistingField"), v => v.ByText("---"))
            .Statistics(out var stats)
            .ToList();
        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));

        var linqQueryNonExistingField = session.Query<AutoVecDoc>()
            .VectorSearch(f => f.WithText("NonExistingField"), v => v.ByText("---"))
            .Statistics(out stats)
            .ToList();
        
        WaitForUserToContinueTheTest(store);
    }

    private record AutoVecDoc(string Text, float[] Singles, sbyte[] Int8, byte[] Binary, string Id = null);
}
