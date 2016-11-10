/*
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

package org.apache.qpid.server.queue;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdArraySerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

class MessageContentJsonConverter
{
    private static final int BRACKETS_COUNT = 2;
    private static final int NULL_LENGTH = 4;
    private static final String DOTS = "...";

    private final ObjectMapper _objectMapper;
    private final Object _messageBody;
    private long _remaining;

    MessageContentJsonConverter(Object messageBody, long limit)
    {
        _messageBody = messageBody;
        _objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(new NoneBase64ByteArraySerializer());
        _objectMapper.registerModule(module);
        _remaining = limit;
    }

    public void convertAndWrite(OutputStream outputStream) throws IOException
    {
        Object messageBody = _messageBody;
        if (_remaining >= 0)
        {
            messageBody = convertAndTruncate(_messageBody);
        }
        _objectMapper.writeValue(outputStream, messageBody);
    }

    private Object convertAndTruncate(final Object source) throws IOException
    {
        if (source == null)
        {
            _remaining = _remaining - NULL_LENGTH;
            return null;
        }
        else if (source instanceof String || source instanceof Character)
        {
            return copyString(String.valueOf(source));
        }
        else if (source instanceof Number || source instanceof Boolean || source.getClass().isPrimitive())
        {
            _remaining = _remaining - _objectMapper.writeValueAsString(source).length();
            return source;
        }
        else if (source instanceof Map)
        {
            return copyMap((Map)source);
        }
        else if (source instanceof Collection)
        {
            return copyCollection((Collection) source);
        }
        else if (source instanceof UUID)
        {
            return copyString(source.toString());
        }
        else if (source.getClass().isArray())
        {
            return copyArray(source);
        }
        else
        {
            // other types are not supported by map and list messages
            // the code execution should not really reach this point
            // to play safe returning them as string
            return copyString(String.valueOf(source));
        }
    }

    private Object copyString(final String source) throws IOException
    {
        String value = _objectMapper.writeValueAsString(source);
        if (_remaining >= value.length())
        {
            _remaining = _remaining - value.length();
            return source;
        }
        else if (_remaining > 0)
        {
            int limit = Math.min((int) _remaining, source.length()) ;
            String truncated =  source.substring(0, limit) + DOTS;
            _remaining = _remaining - truncated.length();
            return truncated;
        }
        else
        {
            return DOTS;
        }
    }

    private Object copyCollection(final Collection source) throws IOException
    {
        _remaining = _remaining - BRACKETS_COUNT;
        List copy = new LinkedList();
        for (Object item : source)
        {
            if (_remaining > 0)
            {
                Object copiedItem = convertAndTruncate(item);
                copy.add(copiedItem);
                if (copy.size() > 0)
                {
                    _remaining = _remaining - 1; // comma character
                }
            }
            else
            {
                break;
            }
        }
        return copy;
    }

    private Object copyMap(final Map source) throws IOException
    {
        _remaining = _remaining - BRACKETS_COUNT;
        Map copy = new LinkedHashMap();
        for (Object key : source.keySet())
        {
            if (_remaining > 0)
            {
                Object copiedKey = convertAndTruncate(key);
                Object copiedValue = convertAndTruncate(source.get(key));
                copy.put(copiedKey, copiedValue);
                _remaining = _remaining - 1; // colon character
                if (copy.size() > 0)
                {
                    _remaining = _remaining - 1; // comma character
                }
            }
            else
            {
                break;
            }
        }
        return copy;
    }

    private Object copyArray(final Object source) throws IOException
    {
        List copy = new LinkedList();
        int length = Array.getLength(source);
        for (int i = 0; i < length; i++)
        {
            copy.add(Array.get(source, i));
        }
        return copyCollection(copy);
    }

    private static class NoneBase64ByteArraySerializer extends StdSerializer<byte[]>
    {
        final StdArraySerializers.IntArraySerializer _underlying = new StdArraySerializers.IntArraySerializer();

        public NoneBase64ByteArraySerializer()
        {
            super(byte[].class);
        }

        @Override
        public void serialize(final byte[] value, final JsonGenerator jgen, final SerializerProvider provider)
                throws IOException
        {
            _underlying.serialize(Ints.toArray(Bytes.asList(value)), jgen, provider);
        }
    }
}
