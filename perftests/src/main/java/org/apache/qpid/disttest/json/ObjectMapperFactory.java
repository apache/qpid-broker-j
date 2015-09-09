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


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import org.apache.qpid.disttest.client.property.PropertyValue;
import org.apache.qpid.disttest.client.property.PropertyValueFactory;
import org.apache.qpid.disttest.client.property.SimplePropertyValue;

public class ObjectMapperFactory
{

    public ObjectMapper createObjectMapper()
    {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(PropertyValue.class, new PropertyValueDeserializer());
        module.addSerializer(SimplePropertyValue.class, new SimplePropertyValueSerializer());

        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        objectMapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        objectMapper.registerModule(module);

        objectMapper.registerModule(module);

        return objectMapper;
    }

    private static class SimplePropertyValueSerializer extends JsonSerializer<SimplePropertyValue>
    {
        @Override
        public void serialize(final SimplePropertyValue simplePropertyValue,
                              final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException
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

    public static class PropertyValueDeserializer extends JsonDeserializer<PropertyValue>
    {
        private static final String DEF_FIELD = "@def";
        private PropertyValueFactory _factory = new PropertyValueFactory();

        @Override
        public PropertyValue deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext)
                throws IOException
        {
            ObjectMapper objectMapper = (ObjectMapper) jsonParser.getCodec();
            TreeNode root = objectMapper.readTree(jsonParser);

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
                Iterator<Map.Entry<String, JsonNode>> fieldIterator = objectNode.fields();
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
                        PropertyValue propertyValue = objectMapper.readValue(objectNode.traverse(objectMapper), classInstance);
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
