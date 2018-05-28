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
package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;

class MessageTransferHeader implements AMQMessageHeader
{


    public static final String JMS_TYPE = "x-jms-type";

    private final DeliveryProperties _deliveryProps;
    private final MessageProperties _messageProps;
    private final long _arrivalTime;

    public MessageTransferHeader(DeliveryProperties deliveryProps,
                                 MessageProperties messageProps,
                                 final long arrivalTime)
    {
        _deliveryProps = deliveryProps;
        _messageProps = messageProps;
        _arrivalTime = arrivalTime;
    }

    @Override
    public String getCorrelationId()
    {
        if (_messageProps != null && _messageProps.getCorrelationId() != null)
        {
            return new String(_messageProps.getCorrelationId(), UTF_8);
        }
        else
        {
            return null;
        }
    }

    @Override
    public long getExpiration()
    {
        long expiration = 0L;
        if (_deliveryProps != null)
        {
            // The AMQP 0-x client wrongly sets ttl to 0 when it means "no ttl".
            if (_deliveryProps.hasTtl() && _deliveryProps.getTtl() != 0L)
            {
                expiration = _arrivalTime + _deliveryProps.getTtl();
            }
            else if (_deliveryProps.hasExpiration())
            {
                expiration = _deliveryProps.getExpiration();
            }
        }
        return expiration;
    }

    @Override
    public String getUserId()
    {
        byte[] userIdBytes = _messageProps == null ? null : _messageProps.getUserId();
        return userIdBytes == null ? null : new String(userIdBytes, UTF_8);
    }

    @Override
    public String getAppId()
    {
        byte[] appIdBytes = _messageProps == null ? null : _messageProps.getAppId();
        return appIdBytes == null ? null : new String(appIdBytes, UTF_8);
    }

    @Override
    public String getGroupId()
    {
        Object jmsXGroupId = getHeader("JMSXGroupID");
        return jmsXGroupId == null ? null : String.valueOf(jmsXGroupId);
    }

    @Override
    public String getMessageId()
    {
        UUID id = _messageProps == null ? null : _messageProps.getMessageId();

        return id == null ? null : "ID:"+String.valueOf(id);
    }

    @Override
    public String getMimeType()
    {
        return _messageProps == null ? null : _messageProps.getContentType();
    }

    @Override
    public String getEncoding()
    {
        return _messageProps == null ? null : _messageProps.getContentEncoding();
    }

    @Override
    public byte getPriority()
    {
        MessageDeliveryPriority priority = _deliveryProps == null || !_deliveryProps.hasPriority()
                                           ? MessageDeliveryPriority.MEDIUM
                                           : _deliveryProps.getPriority();
        return (byte) priority.getValue();
    }

    @Override
    public long getTimestamp()
    {
        return _deliveryProps == null ? 0L : _deliveryProps.getTimestamp();
    }


    @Override
    public long getNotValidBefore()
    {
        Object header = getHeader("x-qpid-not-valid-before");
        return header instanceof Number ? ((Number)header).longValue() : 0L;
    }

    @Override
    public String getType()
    {
        Object type = getHeader(JMS_TYPE);
        return type instanceof String ? (String) type : null;
    }

    @Override
    public String getReplyTo()
    {
        if (_messageProps != null && _messageProps.getReplyTo() != null)
        {
            return _messageProps.getReplyTo().toString();
        }
        else
        {
            return null;
        }
    }

    public String getReplyToExchange()
    {
        if (_messageProps != null && _messageProps.getReplyTo() != null)
        {
            return _messageProps.getReplyTo().getExchange();
        }
        else
        {
            return null;
        }
    }

    public String getReplyToRoutingKey()
    {
        if (_messageProps != null && _messageProps.getReplyTo() != null)
        {
            return _messageProps.getReplyTo().getRoutingKey();
        }
        else
        {
            return null;
        }
    }

    @Override
    public Object getHeader(String name)
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders == null ? null : appHeaders.get(name);
    }

    @Override
    public boolean containsHeaders(Set<String> names)
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders != null && appHeaders.keySet().containsAll(names);

    }

    @Override
    public Collection<String> getHeaderNames()
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders != null ? Collections.unmodifiableCollection(appHeaders.keySet()) : Collections.EMPTY_SET ;

    }

    @Override
    public boolean containsHeader(String name)
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders != null && appHeaders.containsKey(name);
    }

    long getArrivalTime()
    {
        return _arrivalTime;
    }
}
