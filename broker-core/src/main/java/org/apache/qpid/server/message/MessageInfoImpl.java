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
package org.apache.qpid.server.message;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.model.Consumer;

public class MessageInfoImpl implements MessageInfo
{
    private final String _deliveredTo;
    private final Date _arrivalTime;
    private final boolean _persistent;
    private final String _messageId;
    private final Date _expirationTime;
    private final String _applicationId;
    private final String _correlationId;
    private final String _encoding;
    private final String _mimeType;
    private final byte _priority;
    private final String _replyTo;
    private final Date _timestamp;
    private final String _type;
    private final String _userId;
    private final String _state;
    private final int _deliveryCount;
    private final long _size;
    private final long _headerSize;
    private final long _id;
    private final Map<String, Object> _headers;
    private final String _initialRoutingAddress;
    private final String _to;
    private final Date _notValidBefore;
    private final String _messageType;
    private final String _groupId;
    private final String _deliveredToConsumerId;

    public MessageInfoImpl(final MessageInstance instance, final boolean includeHeaders)
    {
        final ServerMessage message = instance.getMessage();
        final AMQMessageHeader messageHeader = message.getMessageHeader();

        MessageInstanceConsumer<?> acquiringConsumer = instance.getAcquiringConsumer();
        _deliveredTo = acquiringConsumer == null ? null : String.valueOf(acquiringConsumer.getIdentifier());
        _deliveredToConsumerId = (acquiringConsumer instanceof Consumer) ?  String.valueOf(((Consumer<?,?>)acquiringConsumer).getId()) : null;
        _arrivalTime = message.getArrivalTime() == 0L ? null : new Date(message.getArrivalTime());
        _messageType = message.getMessageType();
        _persistent = message.isPersistent();
        _messageId = messageHeader.getMessageId();
        _expirationTime = messageHeader.getExpiration() == 0L ? null : new Date(messageHeader.getExpiration());
        _applicationId = messageHeader.getAppId();
        _correlationId = messageHeader.getCorrelationId();
        _encoding = messageHeader.getEncoding();
        _mimeType = messageHeader.getMimeType();
        _priority = messageHeader.getPriority();
        _replyTo = messageHeader.getReplyTo();
        _timestamp = messageHeader.getTimestamp() == 0L ? null : new Date(messageHeader.getTimestamp());
        _type = messageHeader.getType();
        _userId = messageHeader.getUserId();
        _groupId = messageHeader.getGroupId();
        if (instance.isAvailable())
        {
            _state = instance.isHeld() ? "Held" : "Available";
        }
        else
        {
            _state = instance.isAcquired() ? "Acquired" : "";
        }
        _deliveryCount = instance.getDeliveryCount();
        _size = message.getSize();
        _headerSize = message.getSizeIncludingHeader() - message.getSize();
        _id = message.getMessageNumber();
        _initialRoutingAddress = message.getInitialRoutingAddress();
        _to = message.getTo();
        _notValidBefore = messageHeader.getNotValidBefore() == 0L ? null : new Date(messageHeader.getNotValidBefore());

        if(includeHeaders)
        {
            Map<String,Object> headers = new LinkedHashMap<>();
            for(String headerName : messageHeader.getHeaderNames())
            {
                headers.put(headerName, messageHeader.getHeader(headerName));
            }
            _headers = Collections.unmodifiableMap(headers);
        }
        else
        {
            _headers = null;
        }
    }


    @Override
    public long getId()
    {
        return _id;
    }

    @Override
    public long getSize()
    {
        return _size;
    }

    @Override
    public long getHeaderSize()
    {
        return _headerSize;
    }

    @Override
    public String getMessageType()
    {
        return _messageType;
    }

    @Override
    public int getDeliveryCount()
    {
        return _deliveryCount;
    }

    @Override
    public String getState()
    {
        return _state;
    }

    @Override
    public String getDeliveredTo()
    {
        return _deliveredTo;
    }

    @Override
    public String getDeliveredToConsumerId()
    {
        return _deliveredToConsumerId;
    }

    @Override
    public Date getArrivalTime()
    {
        return _arrivalTime == null ? null : new Date(_arrivalTime.getTime());
    }

    @Override
    public boolean isPersistent()
    {
        return _persistent;
    }

    @Override
    public String getMessageId()
    {
        return _messageId;
    }

    @Override
    public Date getExpirationTime()
    {
        return _expirationTime == null ? null : new Date(_expirationTime.getTime());
    }

    @Override
    public String getApplicationId()
    {
        return _applicationId;
    }

    @Override
    public String getCorrelationId()
    {
        return _correlationId;
    }

    @Override
    public String getEncoding()
    {
        return _encoding;
    }

    @Override
    public String getMimeType()
    {
        return _mimeType;
    }

    @Override
    public int getPriority()
    {
        return _priority;
    }

    @Override
    public String getReplyTo()
    {
        return _replyTo;
    }

    @Override
    public Date getTimestamp()
    {
        return _timestamp == null ? null : new Date(_timestamp.getTime());
    }

    @Override
    public String getType()
    {
        return _type;
    }

    @Override
    public String getUserId()
    {
        return _userId;
    }

    @Override
    public String getGroupId()
    {
        return _groupId;
    }

    @Override
    public Map<String, Object> getHeaders()
    {
        return _headers;
    }

    @Override
    public Date getNotValidBefore()
    {
        return _notValidBefore == null ? null : new Date(_notValidBefore.getTime());
    }

    @Override
    public String getInitialRoutingAddress()
    {
        return _initialRoutingAddress;
    }

    @Override
    public String getTo()
    {
        return _to;
    }
}
