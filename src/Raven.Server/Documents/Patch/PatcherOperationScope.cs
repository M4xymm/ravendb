using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Patch
{
    public class PatcherOperationScope : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, KeyValuePair<object, JsValue>> _propertiesByValue = new Dictionary<string, KeyValuePair<object, JsValue>>();

        public readonly DynamicJsonArray DebugInfo = new DynamicJsonArray(); 

        private static readonly List<string> InheritedProperties = new List<string>
        {
            "length",
            "Map",
            "Where",
            "RemoveWhere",
            "Remove"
        };

        public bool DebugMode { get; }

        public DynamicJsonValue DebugActions { get; set; }

        public string CustomFunctions { get; set; }

        public int AdditionalStepsPerSize { get; set; }
        public int MaxSteps { get; set; }

        public JsValue ActualPatchResult { get; set; }
        public JsValue PatchObject;

        public PatcherOperationScope(DocumentDatabase database, DocumentsOperationContext context, bool debugMode = false)
        {
            _database = database;
            _context = context;
            DebugMode = debugMode;
        }

        public JsValue ToJsArray(Engine engine, BlittableJsonReaderArray json, string propertyKey)
        {
            var elements = new JsValue[json.Length];
            for (var i = 0; i < json.Length; i++)
            {
                var value = json.GetValueTokenTupleByIndex(i);
                elements[i] = ToJsValue(engine, value.Item1, value.Item2, propertyKey + "[" + i + "]");
            }

            var result = engine.Array.Construct(Arguments.Empty);
            engine.Array.PrototypeObject.Push(result, elements);
            return result;
        }

        public JsValue ToJsObject(Engine engine, BlittableJsonReaderObject json, string propertyName = null)
        {
            var jsObject = engine.Object.Construct(Arguments.Empty);
            for (int i = 0; i < json.Count; i++)
            {
                var property = json.GetPropertyByIndex(i);
                var name = property.Item1.ToString();
                var propertyKey = CreatePropertyKey(name, propertyName);
                var value = property.Item2;
                JsValue jsValue = ToJsValue(engine, value, property.Item3, propertyKey);
                _propertiesByValue[propertyKey] = new KeyValuePair<object, JsValue>(value, jsValue);
                jsObject.Put(name, jsValue, true);
            }
            return jsObject;
        }

        public JsValue ToJsValue(Engine engine, object value, BlittableJsonToken token, string propertyKey = null)
        {
            switch (token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return JsValue.Null;
                case BlittableJsonToken.Boolean:
                    return new JsValue((bool) value);

                case BlittableJsonToken.Integer:
                    return new JsValue((long) value);
                case BlittableJsonToken.Float:
                    return new JsValue((double) (LazyDoubleValue) value);
                case BlittableJsonToken.String:
                    return new JsValue(((LazyStringValue) value).ToString());
                case BlittableJsonToken.CompressedString:
                    return new JsValue(((LazyCompressedStringValue) value).ToString());

                case BlittableJsonToken.StartObject:
                    return ToJsObject(engine, (BlittableJsonReaderObject) value, propertyKey);
                case BlittableJsonToken.StartArray:
                    return ToJsArray(engine, (BlittableJsonReaderArray)value, propertyKey);

                default:
                    throw new ArgumentOutOfRangeException(token.ToString());
            }
        }

        private static string CreatePropertyKey(string key, string property)
        {
            if (string.IsNullOrEmpty(property))
                return key;

            return property + "." + key;
        }

        public DynamicJsonValue ToBlittable(JsValue jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            var objectInstance = jsObject.AsObject();
            if (objectInstance.Class == "Function")
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return null;
            }

            var obj = new DynamicJsonValue();
            foreach (var property in objectInstance.GetOwnProperties())
            {
                if (property.Key == Constants.ReduceKeyFieldName || property.Key == Constants.DocumentIdFieldName)
                    continue;

                var value = property.Value.Value;
                if (value.HasValue == false)
                    continue;

                if (value.Value.IsRegExp())
                    continue;

                var recursive = jsObject == value;
                if (recursiveCall && recursive)
                    obj[property.Key] = null;
                else
                    obj[property.Key] = ToBlittableInstance(value.Value, CreatePropertyKey(property.Key, propertyKey), recursive);
            }
            return obj;
        }

        private object ToBlittableInstance(JsValue v, string propertyKey, bool recursiveCall)
        {
            if (v.IsBoolean())
                return v.AsBoolean();

            if (v.IsString())
            {
                const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                var valueAsObject = v.ToObject();
                var value = valueAsObject?.ToString();
                if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                {
                    value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                    var byteArray = Convert.FromBase64String(value);
                    return Encoding.UTF8.GetString(byteArray);
                }
                return value;
            }

            if (v.IsNumber())
            {
                var num = v.AsNumber();

                KeyValuePair<object, JsValue> property;
                if (_propertiesByValue.TryGetValue(propertyKey, out property))
                {
                    var originalValue = property.Key;
                    if (originalValue is float || originalValue is int)
                    {
                        // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                        // which will convert a Int64 to jsFloat.
                        var jsValue = property.Value;
                        if (jsValue.IsNumber() && Math.Abs(num - jsValue.AsNumber()) < double.Epsilon)
                            return originalValue;

                        if (originalValue is int)
                            return (long) num;
                        return num; //float
                    }
                }

                // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                var integer = Math.Truncate(num);
                if (Math.Abs(num - integer) < double.Epsilon)
                    return (long) integer;
                return num;
            }
            if (v.IsNull())
                return null;
            if (v.IsUndefined())
                return null;
            if (v.IsArray())
            {
                var jsArray = v.AsArray();
                var array = new DynamicJsonArray();

                foreach (var property in jsArray.GetOwnProperties())
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value.Value;
                    if (!jsInstance.HasValue)
                        continue;

                    var ravenJToken = ToBlittableInstance(jsInstance.Value, propertyKey + "[" + property.Key + "]", recursiveCall);
                    if (ravenJToken == null)
                        continue;

                    array.Add(ravenJToken);
                }

                return array;
            }
            if (v.IsDate())
            {
                return v.AsDate().ToDateTime();
            }
            if (v.IsObject())
            {
                return ToBlittable(v, propertyKey, recursiveCall);
            }
            if (v.IsRegExp())
                return null;

            throw new NotSupportedException(v.Type.ToString());
        }

        public void Dispose()
        {
        }

        private readonly Dictionary<string, Document> _documentKeyContext = new Dictionary<string, Document>();
        private readonly List<Document> _incompleteDocumentKeyContext = new List<Document>();

        protected void RecordActionForDebug(string actionName, string documentKey, Document document)
        {
            if (DebugMode == false)
                return;

            throw new NotImplementedException();
        }

        public virtual JsValue LoadDocument(string documentKey, Engine engine, ref int totalStatements)
        {
            Document document;
            if (_documentKeyContext.TryGetValue(documentKey, out document) == false)
                document = _database.DocumentsStorage.Get(_context, documentKey);

            RecordActionForDebug("LoadDocument", documentKey, document);

            if (document == null)
                return JsValue.Null;

            totalStatements += (MaxSteps/2 + (document.Data.Size*AdditionalStepsPerSize));
            engine.Options.MaxStatements(totalStatements);

            // TODO: Make sure to add Constants.DocumentIdFieldName to document.Data
            return ToJsObject(engine, document.Data);
        }

        public virtual string PutDocument(string key, object documentAsObject, object metadataAsObject, Engine engine)
        {
            throw new NotImplementedException();
        }

        public virtual void DeleteDocument(string documentKey)
        {
            throw new NotSupportedException("Deleting documents is not supported.");
        }

        protected void AddToContext(string key, Document document)
        {
            if (string.IsNullOrEmpty(key) || key.EndsWith("/"))
                _incompleteDocumentKeyContext.Add(document);
            else
                _documentKeyContext[key] = document;
        }

        protected void DeleteFromContext(string key)
        {
            _documentKeyContext[key] = null;
        }

        /*public IEnumerable<ScriptedJsonPatcher.Operation> GetOperations()
        {
            return _documentKeyContext.Select(x => new ScriptedJsonPatcher.Operation
            {
                Type = x.Value != null ? ScriptedJsonPatcher.OperationType.Put : ScriptedJsonPatcher.OperationType.Delete,
                DocumentKey = x.Key,
                Document = x.Value
            }).Union(_incompleteDocumentKeyContext.Select(x => new ScriptedJsonPatcher.Operation
            {
                Type = ScriptedJsonPatcher.OperationType.Put,
                DocumentKey = x.Key,
                Document = x
            }));
        }

        public IEnumerable<Document> GetPutOperations()
        {
            return GetOperations().Where(x => x.Type == ScriptedJsonPatcher.OperationType.Put).Select(x => x.Document);
        }*/
    }
}