﻿using System;
using System.Collections.Generic;
using System.Linq;
using Esprima;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptIndex : StaticIndexBase
    {
        private const string GlobalDefinitions = "globalDefinition";
        private const string MapsProperty = "maps";
        private const string CollectionProperty = "collection";
        private const string MethodProperty = "method";
        private const string MoreArgsProperty = "moreArgs";
        private const string ReduceProperty = "reduce";
        private const string AggregateByProperty = "aggregateBy";
        private const string KeyProperty = "key";

        public JavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration)
        {
            _definitions = definition;

            var indexConfiguration = new SingleIndexConfiguration(definition.Configuration, configuration);

            // we create the Jint instance directly instead of using SingleRun
            // because the index is single threaded and long lived
            var resolver = new JintPreventResolvingTasksReferenceResolver();
            _engine = new Engine(options =>
            {
                options.LimitRecursion(64)
                    .SetReferencesResolver(resolver)
                    .MaxStatements(indexConfiguration.MaxStepsForScript)
                    .Strict(configuration.Patching.StrictMode)
                    .AddObjectConverter(new JintGuidConverter())
                    .AddObjectConverter(new JintStringConverter())
                    .AddObjectConverter(new JintEnumConverter())
                    .AddObjectConverter(new JintDateTimeConverter())
                    .AddObjectConverter(new JintTimeSpanConverter())
                    .LocalTimeZone(TimeZoneInfo.Utc);
            });

            using (_engine.DisableMaxStatements())
            {
                var (mapList, mapReferencedCollections) = InitializeEngine(definition);

                var definitions = GetDefinitions();

                ProcessMaps(definitions, resolver, mapList, mapReferencedCollections, out var collectionFunctions);

                ProcessReduce(definition, definitions, resolver);

                ProcessFields(definition, collectionFunctions);
            }

            _javaScriptUtils = new JavaScriptUtils(null, _engine);
        }

        private void ProcessFields(IndexDefinition definition, Dictionary<string, List<JavaScriptMapOperation>> collectionFunctions)
        {
            var fields = new HashSet<string>();
            HasDynamicFields = false;
            foreach (var (key, val) in collectionFunctions)
            {
                Maps.Add(key, val.Select(x => (IndexingFunc)x.IndexingFunction).ToList());

                //TODO: Validation of matches fields between group by / collections / etc
                foreach (var operation in val)
                {
                    HasDynamicFields |= operation.HasDynamicReturns;
                    fields.UnionWith(operation.Fields);
                    foreach (var (k, v) in operation.FieldOptions)
                    {
                        _definitions.Fields.Add(k, v);
                    }
                }
            }

            if (definition.Fields != null)
            {
                foreach (var item in definition.Fields)
                {
                    if (string.Equals(item.Key, Constants.Documents.Indexing.Fields.AllFields))
                        continue;

                    fields.Add(item.Key);
                }
            }

            OutputFields = fields.ToArray();
        }

        private void ProcessReduce(IndexDefinition definition, ObjectInstance definitions, JintPreventResolvingTasksReferenceResolver resolver)
        {
            var reduceObj = definitions.GetProperty(ReduceProperty)?.Value;
            if (reduceObj != null && reduceObj.IsObject())
            {
                var reduceAsObj = reduceObj.AsObject();
                var groupByKey = reduceAsObj.GetProperty(KeyProperty).Value.As<ArrowFunctionInstance>();
                var reduce = reduceAsObj.GetProperty(AggregateByProperty).Value.As<ArrowFunctionInstance>();
                ReduceOperation = new JavaScriptReduceOperation(reduce, groupByKey, _engine, resolver) { ReduceString = definition.Reduce };
                GroupByFields = ReduceOperation.GetReduceFieldsNames();
                Reduce = ReduceOperation.IndexingFunction;
            }
        }

        private void ProcessMaps(ObjectInstance definitions, JintPreventResolvingTasksReferenceResolver resolver, List<string> mapList,
            List<HashSet<CollectionName>> mapReferencedCollections, out Dictionary<string, List<JavaScriptMapOperation>> collectionFunctions)
        {
            var mapsArray = definitions.GetProperty(MapsProperty).Value;
            if (mapsArray.IsNull() || mapsArray.IsUndefined() || mapsArray.IsArray() == false)
                ThrowIndexCreationException($"doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");
            var maps = mapsArray.AsArray();
            collectionFunctions = new Dictionary<string, List<JavaScriptMapOperation>>();
            for (int i = 0; i < maps.GetLength(); i++)
            {
                var mapObj = maps.Get(i.ToString());
                if (mapObj.IsNull() || mapObj.IsUndefined() || mapObj.IsObject() == false)
                    ThrowIndexCreationException($"map function #{i} is not a valid object");
                var map = mapObj.AsObject();
                if (map.HasProperty(CollectionProperty) == false)
                    ThrowIndexCreationException($"map function #{i} is missing a collection name");
                var mapCollectionStr = map.Get(CollectionProperty);
                if (mapCollectionStr.IsString() == false)
                    ThrowIndexCreationException($"map function #{i} collection name isn't a string");
                var mapCollection = mapCollectionStr.AsString();
                if (collectionFunctions.TryGetValue(mapCollection, out var list) == false)
                {
                    list = new List<JavaScriptMapOperation>();
                    collectionFunctions.Add(mapCollection, list);
                }

                if (map.HasProperty(MethodProperty) == false)
                    ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");
                var funcInstance = map.Get(MethodProperty).As<FunctionInstance>();
                if (funcInstance == null)
                    ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a 'FunctionInstance'");
                var operation = new JavaScriptMapOperation(_engine, resolver)
                {
                    MapFunc = funcInstance,
                    IndexName = _definitions.Name,
                    MapString = mapList[i]
                };
                if (map.HasOwnProperty(MoreArgsProperty))
                {
                    var moreArgsObj = map.Get(MoreArgsProperty);
                    if (moreArgsObj.IsArray())
                    {
                        var array = moreArgsObj.AsArray();
                        if (array.GetLength() > 0)
                        {
                            operation.MoreArguments = array;
                        }
                    }
                }

                operation.Analyze(_engine);
                if (ReferencedCollections.TryGetValue(mapCollection, out var collectionNames) == false)
                {
                    collectionNames = new HashSet<CollectionName>();
                    ReferencedCollections.Add(mapCollection, collectionNames);
                }

                collectionNames.UnionWith(mapReferencedCollections[i]);

                list.Add(operation);
            }
        }

        private ObjectInstance GetDefinitions()
        {
            var definitionsObj = _engine.GetValue(GlobalDefinitions);

            if (definitionsObj.IsNull() || definitionsObj.IsUndefined() || definitionsObj.IsObject() == false)
                ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

            var definitions = definitionsObj.AsObject();
            if (definitions.HasProperty(MapsProperty) == false)
                ThrowIndexCreationException("is missing its 'globalDefinition.maps' property, are you modifying it in your script?");

            return definitions;
        }

        private static readonly ParserOptions DefaultParserOptions = new ParserOptions();

        private HashSet<CollectionName> ExecuteCodeAndCollectReferencedCollections(string code)
        {
            var javascriptParser = new JavaScriptParser(code, DefaultParserOptions);
            var program = javascriptParser.ParseProgram();
            _engine.ExecuteWithReset(program);
            var loadVisitor = new EsprimaReferencedCollectionVisitor();
            loadVisitor.Visit(program);
            return loadVisitor.ReferencedCollection;
        }

        private (List<string> Maps, List<HashSet<CollectionName>> MapReferencedCollections) InitializeEngine(IndexDefinition definition)
        {
            _engine.SetValue("load", new ClrFunctionInstance(_engine, "load", LoadDocument));
            _engine.SetValue("getMetadata", new ClrFunctionInstance(_engine, "getMetadata", GetMetadata));
            _engine.SetValue("id", new ClrFunctionInstance(_engine, "id", GetDocumentId));
            _engine.ExecuteWithReset(Code);

            if (definition.AdditionalSources != null)
            {
                foreach (var script in definition.AdditionalSources.Values)
                {
                    _engine.ExecuteWithReset(script);
                }
            }

            var maps = definition.Maps.ToList();
            var mapReferencedCollections = new List<HashSet<CollectionName>>();

            foreach (var t in maps)
            {
                mapReferencedCollections.Add(ExecuteCodeAndCollectReferencedCollections(t));
            }

            if (definition.Reduce != null)
            {
                _engine.ExecuteWithReset(definition.Reduce);
            }

            return (maps, mapReferencedCollections);
        }

        private JsValue GetDocumentId(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(_javaScriptUtils);

            return _javaScriptUtils.GetDocumentId(self, args);
        }

        private JsValue GetMetadata(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(_javaScriptUtils);

            return _javaScriptUtils.GetMetadata(self, args);
        }

        private void ThrowIndexCreationException(string message)
        {
            throw new IndexCreationException($"Javascript index {_definitions.Name} {message}");
        }

        private JsValue LoadDocument(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
            }

            if (args[0].IsNull() || args[0].IsUndefined())
                return JsValue.Undefined;

            if (args[0].IsString() == false ||
                args[1].IsString() == false)
            {
                throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
            }

            object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString(), args[1].AsString());
            if (JavaScriptIndexUtils.GetValue(_engine, doc, out var item))
                return item;

            return JsValue.Undefined;
        }


        private const string Code = @"
var globalDefinition =
{
    maps: [],
    reduce: null
}

function map(name, lambda) {

    var map = {
        collection: name,
        method: lambda,
        moreArgs: Array.prototype.slice.call(arguments, 2)
    };    
    globalDefinition.maps.push(map);
}

function groupBy(lambda) {
    var reduce = globalDefinition.reduce = { };
    reduce.key = lambda;
 
    reduce.aggregate = function(reduceFunction){
        reduce.aggregateBy = reduceFunction;
    }
    return reduce;
}

function createSpatialField(wkt) {    
    return { $spatial: wkt }   
}

function createSpatialField(lat, lng) {    
    return { $spatial: {Lng:lng, Lat:lat} }   
}
";

        private readonly IndexDefinition _definitions;
        private readonly Engine _engine;
        private readonly JavaScriptUtils _javaScriptUtils;

        public JavaScriptReduceOperation ReduceOperation { get; private set; }

        public void SetBufferPoolForTestingPurposes(UnmanagedBuffersPoolWithLowMemoryHandling bufferPool)
        {
            ReduceOperation?.SetBufferPoolForTestingPurposes(bufferPool);
        }

        public void SetAllocatorForTestingPurposes(ByteStringContext byteStringContext)
        {
            ReduceOperation?.SetAllocatorForTestingPurposes(byteStringContext);
        }
    }
}
