/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.qpid.disttest.json;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.TreeNode;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.cfg.EnumFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.ValueNode;

import org.apache.qpid.disttest.client.property.PropertyValue;
import org.apache.qpid.disttest.client.property.PropertyValueFactory;
import org.apache.qpid.disttest.client.property.SimplePropertyValue;

public class ObjectMapperFactory
{

    public ObjectMapper createObjectMapper()
    {
        SimpleModule module = new SimpleModule()
                .addDeserializer(PropertyValue.class, new PropertyValueDeserializer())
                .addSerializer(SimplePropertyValue.class, new SimplePropertyValueSerializer());

        return JsonMapper.builder()
                .changeDefaultVisibility(visibilityChecker -> visibilityChecker
                        .withVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                        .withVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
                        .withVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE)
                        .withVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE)
                        .withVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.DEFAULT))
                .enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
                .disable(EnumFeature.READ_ENUMS_USING_TO_STRING)
                .disable(EnumFeature.WRITE_ENUMS_USING_TO_STRING)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
                .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
                .addModule(module)
                .build();
    }

    private static class SimplePropertyValueSerializer extends ValueSerializer<SimplePropertyValue>
    {
        @Override
        public void serialize(final SimplePropertyValue simplePropertyValue,
                              final JsonGenerator jsonGenerator,
                              final SerializationContext serializerContext)
        {
            if (simplePropertyValue == null)
            {
                jsonGenerator.writeNull();
            }
            else if (simplePropertyValue.getValue() instanceof Number)
            {
                if (simplePropertyValue.getValue() instanceof Long)
                {
                    jsonGenerator.writeNumber((Long) simplePropertyValue.getValue());
                }
                else if (simplePropertyValue.getValue() instanceof Integer)
                {
                    jsonGenerator.writeNumber((Integer) simplePropertyValue.getValue());
                }
                else if (simplePropertyValue.getValue() instanceof Double)
                {
                    jsonGenerator.writeNumber((Double) simplePropertyValue.getValue());
                }
                else if (simplePropertyValue.getValue() instanceof Float)
                {
                    jsonGenerator.writeNumber((Float) simplePropertyValue.getValue());
                }
                else
                {
                    throw new IllegalArgumentException("Unsupported numeric type " + simplePropertyValue);
                }
            }
            else if (simplePropertyValue.getValue() instanceof Boolean)
            {
                jsonGenerator.writeBoolean((Boolean) simplePropertyValue.getValue());
            }
            else if (simplePropertyValue.getValue() instanceof String)
            {
                jsonGenerator.writeString((String) simplePropertyValue.getValue());
            }
            else
            {
                throw new IllegalArgumentException("Unsupported type " + simplePropertyValue);
            }
        }
    }

    public static class PropertyValueDeserializer extends ValueDeserializer<PropertyValue>
    {
        private static final String DEF_FIELD = "@def";
        private final PropertyValueFactory _factory = new PropertyValueFactory();

        @Override
        public PropertyValue deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
        {
            TreeNode root = deserializationContext.readTree(jsonParser);
            ObjectReadContext objectReadContext = jsonParser.objectReadContext();

            if (root.isValueNode())
            {
                ValueNode value = (ValueNode) root;

                Object result = null;
                if (value.isNumber())
                {
                    if (value.isLong())
                    {
                        result = value.asLong();
                    }
                    else if (value.isDouble())
                    {
                        result = value.asDouble();
                    }
                    else
                    {
                        result = value.asInt();
                    }
                }
                else if (value.isBoolean())
                {
                    result = value.asBoolean();
                }
                else if (value.isTextual())
                {
                    result = value.asText();
                }
                return new SimplePropertyValue(result);
            }
            else if (root.isArray())
            {
                // TODO Do we use this?
                throw new IllegalStateException("Array deserialisation is not supported");
            }
            else
            {
                ObjectNode objectNode = (ObjectNode) root;
                Iterator<Map.Entry<String, JsonNode>> fieldIterator = objectNode.properties().iterator();
                String definition = null;
                Map<String, Object> result = new HashMap<>();
                while (fieldIterator.hasNext())
                {
                    Map.Entry<String, JsonNode> entry = fieldIterator.next();
                    if (DEF_FIELD.equals(entry.getKey()))
                    {
                        definition = entry.getValue().asText();
                    }
                    result.put(entry.getKey(), entry.getValue());
                }
                if (definition != null)
                {
                    try
                    {
                        Class<PropertyValue> classInstance = _factory.getPropertyValueClass(definition);
                        JsonParser treeParser = objectNode.traverse(deserializationContext);
                        treeParser.nextToken();
                        PropertyValue propertyValue = objectReadContext.readValue(treeParser, classInstance);
                        return propertyValue;
                    }
                    catch (ClassNotFoundException e)
                    {
                        return null;
                    }
                }
                else
                {
                    return new SimplePropertyValue(result);
                }
            }
        }
    }
}
