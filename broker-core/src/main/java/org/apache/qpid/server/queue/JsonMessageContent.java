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
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.CharMatcher;

import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.RestContentHeader;

class JsonMessageContent implements Content, CustomRestHeaders
{
    private static final int BRACKETS_COUNT = 2;
    private static final int NULL_LENGTH = 4;
    private static final String DOTS = "...";

    private final InternalMessage _internalMessage;
    private final MessageReference<?> _messageReference;
    private final ObjectMapper _objectMapper;
    private final String _queueName;
    private final long _limit;
    private long _remaining;

    JsonMessageContent(String queueName, MessageReference<?> messageReference, InternalMessage message, long limit)
    {
        _queueName = queueName;
        _messageReference = messageReference;
        _internalMessage = message;
        _objectMapper = new ObjectMapper();
        _limit = limit;
        _remaining = limit;
    }

    @Override
    public void write(final OutputStream outputStream) throws IOException
    {
        Object messageBody = _internalMessage.getMessageBody();
        if (_limit == -1 || _messageReference.getMessage().getSize() <= _limit)
        {
            _objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }
        else
        {
            messageBody= copyObject(messageBody);
        }
        _objectMapper.writeValue(outputStream, messageBody);
    }

    @Override
    public void release()
    {
        _messageReference.release();
    }

    @SuppressWarnings("unused")
    @RestContentHeader("Content-Type")
    public String getContentType()
    {
        return "application/json";
    }

    @SuppressWarnings("unused")
    @RestContentHeader("Content-Disposition")
    public String getContentDisposition()
    {
        String queueName = _queueName;
        boolean isAscii = CharMatcher.ASCII.matchesAllOf(queueName);
        String asciiQueueName = isAscii ? queueName : queueName.replaceAll("\\P{InBasic_Latin}", "_");
        String disposition = String.format("attachment; filename=\"%s\"",
                                           getDispositionFileName(asciiQueueName, _internalMessage));
        if (!isAscii)
        {
            try
            {
                disposition += "; filename*=UTF-8''"
                               + URLEncoder.encode(getDispositionFileName(queueName, _internalMessage), "UTF-8");
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("JVM does not support UTF8", e);
            }
        }
        return disposition;
    }

    private String getDispositionFileName(String queueName, ServerMessage message)
    {
        return String.format("%s.message.%s", queueName, message.getMessageNumber())
               + (message.getMessageHeader().getMimeType().startsWith("text") ? ".txt" : "");
    }

    private Object copyObject(final Object source) throws IOException
    {
        if (_remaining > 0)
        {
            if (source == null)
            {
                return null;
            }
            else if (source instanceof String || source instanceof Character)
            {
                return copyString(String.valueOf(source));
            }
            else if (source instanceof Number || source instanceof Boolean || source.getClass().isPrimitive())
            {
                String value = _objectMapper.writeValueAsString(source);
                _remaining = _remaining - value.length();
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
        return null;
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
            _remaining = 0;
            return source.substring(0, limit);
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
        for (Object key : source)
        {
            if (_remaining > 0)
            {
                Object copiedKey = copyObject(key);
                if (copiedKey == null)
                {
                    _remaining = _remaining - NULL_LENGTH;
                }
                copy.add(copiedKey);
                if (copy.size() > 0 && _remaining > 0)
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
                Object copiedValue = copyObject(source.get(key));
                if (copiedValue != null && _remaining > 0)
                {
                    Object copiedKey = copyObject(key);
                    copy.put(copiedKey, copiedValue);
                    _remaining = _remaining - 1; // colon character
                    if (copy.size() > 0 && _remaining > 0)
                    {
                        _remaining = _remaining - 1; // comma character
                    }
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
            Object value = Array.get(source, i);
            copy.add(copyObject(value));
        }
        return copyCollection(copy);
    }

}
