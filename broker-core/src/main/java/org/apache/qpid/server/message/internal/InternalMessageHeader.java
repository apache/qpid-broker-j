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
package org.apache.qpid.server.message.internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.message.AMQMessageHeader;

public final class InternalMessageHeader implements AMQMessageHeader, Serializable
{
    private static final long serialVersionUID = 7219183903302678948L;

    private final LinkedHashMap<String, Object> _headers;
    private final String _correlationId;
    private final long _expiration;
    private final String _userId;
    private final String _appId;
    private final String _messageId;
    private final String _mimeType;
    private final String _encoding;
    private final byte _priority;
    private final long _timestamp;
    private final long _notValidBefore;
    private final String _type;
    private final String _replyTo;
    private long _arrivalTime;

    public InternalMessageHeader(final Map<String, Object> headers,
                                 final String correlationId,
                                 final long expiration,
                                 final String userId,
                                 final String appId,
                                 final String messageId,
                                 final String mimeType,
                                 final String encoding,
                                 final byte priority,
                                 final long timestamp,
                                 final long notValidBefore,
                                 final String type,
                                 final String replyTo,
                                 final long arrivalTime)
    {
        _headers = headers == null ? new LinkedHashMap<>() : new LinkedHashMap<>(headers);

        _correlationId = correlationId;
        _expiration = expiration;
        _userId = userId;
        _appId = appId;
        _messageId = messageId;
        _mimeType = mimeType;
        _encoding = encoding;
        _priority = priority;
        _timestamp = timestamp > 0 ? timestamp : arrivalTime;
        _notValidBefore = notValidBefore;
        _type = type;
        _replyTo = replyTo;
        _arrivalTime = arrivalTime;
    }

    public InternalMessageHeader(final AMQMessageHeader header)
    {
        this(header, System.currentTimeMillis());
    }

    public InternalMessageHeader(final AMQMessageHeader header, long arrivalTime)
    {
        this(buildHeaders(header),
             header.getCorrelationId(),
             header.getExpiration(),
             header.getUserId(),
             header.getAppId(),
             header.getMessageId(),
             header.getMimeType(),
             header.getEncoding(),
             header.getPriority(),
             header.getTimestamp(),
             header.getNotValidBefore(),
             header.getType(),
             header.getReplyTo(),
             arrivalTime);
    }

    @Override
    public String getCorrelationId()
    {
        return _correlationId;
    }

    @Override
    public long getExpiration()
    {
        return _expiration;
    }

    @Override
    public String getUserId()
    {
        return _userId;
    }

    @Override
    public String getAppId()
    {
        return _appId;
    }

    @Override
    public String getGroupId()
    {
        final Object jmsxGroupId = _headers.get("JMSXGroupID");
        return jmsxGroupId == null ? null : String.valueOf(jmsxGroupId);
    }

    @Override
    public String getMessageId()
    {
        return _messageId;
    }

    @Override
    public String getMimeType()
    {
        return _mimeType;
    }

    @Override
    public String getEncoding()
    {
        return _encoding;
    }

    @Override
    public byte getPriority()
    {
        return _priority;
    }

    @Override
    public long getTimestamp()
    {
        return _timestamp;
    }

    @Override
    public long getNotValidBefore()
    {
        return _notValidBefore;
    }

    @Override
    public String getType()
    {
        return _type;
    }

    @Override
    public String getReplyTo()
    {
        return _replyTo;
    }

    @Override
    public Object getHeader(final String name)
    {
        return _headers.get(name);
    }

    @Override
    public boolean containsHeaders(final Set<String> names)
    {
        return _headers.keySet().containsAll(names);
    }

    @Override
    public boolean containsHeader(final String name)
    {
        return _headers.keySet().contains(name);
    }

    @Override
    public Collection<String> getHeaderNames()
    {
        return Collections.unmodifiableCollection(_headers.keySet());
    }

    long getArrivalTime()
    {
        return _arrivalTime;
    }

    public Map<String,Object> getHeaderMap()
    {
        return Collections.unmodifiableMap(new LinkedHashMap<>(_headers));
    }

    private static Map<String, Object> buildHeaders(final AMQMessageHeader header)
    {
        Map<String, Object> map = new LinkedHashMap<>();
        for (String s : header.getHeaderNames())
        {
            if (map.put(s, header.getHeader(s)) != null)
            {
                throw new IllegalStateException("Duplicate key");
            }
        }
        return map;
    }
}
