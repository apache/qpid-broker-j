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
package org.apache.qpid.server.protocol.v0_8;

import java.nio.BufferUnderflowException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolVersionException;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

/**
 * Encapsulates a publish body and a content header. In the context of the message store these are treated as a
 * single unit.
 */
public class MessageMetaData implements StorableMessageMetaData
{
    private final MessagePublishInfo _messagePublishInfo;

    private final ContentHeaderBody _contentHeaderBody;


    private final long _arrivalTime;
    private static final byte MANDATORY_FLAG = 1;
    private static final byte IMMEDIATE_FLAG = 2;
    public static final MessageMetaDataType.Factory<MessageMetaData> FACTORY = new MetaDataFactory();
    private static final MessageMetaDataType_0_8 TYPE = new MessageMetaDataType_0_8();

    public MessageMetaData(MessagePublishInfo publishBody, ContentHeaderBody contentHeaderBody)
    {
        this(publishBody,contentHeaderBody, System.currentTimeMillis());
    }

    public MessageMetaData(MessagePublishInfo publishBody,
                           ContentHeaderBody contentHeaderBody,
                           long arrivalTime)
    {
        _contentHeaderBody = contentHeaderBody;
        _messagePublishInfo = publishBody;
        _arrivalTime = arrivalTime;
    }


    public ContentHeaderBody getContentHeaderBody()
    {
        return _contentHeaderBody;
    }

    public MessagePublishInfo getMessagePublishInfo()
    {
        return _messagePublishInfo;
    }

    public long getArrivalTime()
    {
        return _arrivalTime;
    }

    @Override
    public MessageMetaDataType getType()
    {
        return TYPE;
    }

    @Override
    public int getStorableSize()
    {
        int size = _contentHeaderBody.getSize();
        size += 4;
        size += EncodingUtils.encodedShortStringLength(_messagePublishInfo.getExchange());
        size += EncodingUtils.encodedShortStringLength(_messagePublishInfo.getRoutingKey());
        size += 1; // flags for immediate/mandatory
        size += EncodingUtils.encodedLongLength();

        return size;
    }


    @Override
    public void writeToBuffer(final QpidByteBuffer dest)
    {
        dest.putInt(_contentHeaderBody.getSize());
        _contentHeaderBody.writePayload(dest);

        EncodingUtils.writeShortStringBytes(dest, _messagePublishInfo.getExchange());
        EncodingUtils.writeShortStringBytes(dest, _messagePublishInfo.getRoutingKey());
        byte flags = 0;
        if(_messagePublishInfo.isMandatory())
        {
            flags |= MANDATORY_FLAG;
        }
        if(_messagePublishInfo.isImmediate())
        {
            flags |= IMMEDIATE_FLAG;
        }
        dest.put(flags);
        dest.putLong(_arrivalTime);

    }

    @Override
    public int getContentSize()
    {
        return (int) _contentHeaderBody.getBodySize();
    }

    @Override
    public boolean isPersistent()
    {
        return _contentHeaderBody.getProperties().getDeliveryMode() ==  BasicContentHeaderProperties.PERSISTENT;
    }

    @Override
    public synchronized void dispose()
    {
        _contentHeaderBody.dispose();
    }

    @Override
    public synchronized void clearEncodedForm()
    {
        _contentHeaderBody.clearEncodedForm();
    }

    @Override
    public synchronized void reallocate()
    {
        _contentHeaderBody.reallocate();
    }

    public synchronized void validate()
    {
        _contentHeaderBody.getProperties().validate();
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MessageMetaData>
    {

        @Override
        public MessageMetaData createMetaData(QpidByteBuffer buf)
        {
            try
            {
                final int size = buf.getInt();

                ContentHeaderBody chb = ContentHeaderBody.createFromBuffer(buf, size);
                final AMQShortString exchange = AMQShortString.readAMQShortString(buf);
                final AMQShortString routingKey = AMQShortString.readAMQShortString(buf);
                final byte flags = buf.get();
                long arrivalTime = buf.getLong();

                MessagePublishInfo publishBody =
                        new MessagePublishInfo(exchange,
                                               (flags & IMMEDIATE_FLAG) != 0,
                                               (flags & MANDATORY_FLAG) != 0,
                                               routingKey);

                return new MessageMetaData(publishBody, chb, arrivalTime);
            }
            catch (AMQFrameDecodingException | AMQProtocolVersionException | AMQPInvalidClassException
                    | IllegalArgumentException | IllegalStateException | BufferUnderflowException  e)
            {
                throw new ConnectionScopedRuntimeException(e);
            }

        }
    }

    public AMQMessageHeader getMessageHeader()
    {
        return new MessageHeaderAdapter();
    }

    private final class MessageHeaderAdapter implements AMQMessageHeader
    {
        private BasicContentHeaderProperties getProperties()
        {
            return getContentHeaderBody().getProperties();
        }

        @Override
        public String getUserId()
        {
            return getProperties().getUserIdAsString();
        }

        @Override
        public String getAppId()
        {
            return getProperties().getAppIdAsString();
        }

        @Override
        public String getGroupId()
        {
            return Objects.toString(getHeader("JMSXGroupID"), null);
        }

        @Override
        public String getCorrelationId()
        {
            return getProperties().getCorrelationIdAsString();
        }

        @Override
        public long getExpiration()
        {
            return getProperties().getExpiration();
        }

        @Override
        public String getMessageId()
        {
            return getProperties().getMessageIdAsString();
        }

        @Override
        public String getMimeType()
        {
            return getProperties().getContentTypeAsString();
        }

        @Override
        public String getEncoding()
        {
            return getProperties().getEncodingAsString();
        }

        @Override
        public byte getPriority()
        {
            return getProperties().getPriority();
        }

        @Override
        public long getTimestamp()
        {
            return getProperties().getTimestamp();
        }

        @Override
        public String getType()
        {
            return getProperties().getTypeAsString();
        }

        @Override
        public String getReplyTo()
        {
            return getProperties().getReplyToAsString();
        }

        @Override
        public long getNotValidBefore()
        {
            Object header = getHeader("x-qpid-not-valid-before");
            return header instanceof Number ? ((Number)header).longValue() : 0L;
        }

        @Override
        public Object getHeader(String name)
        {
            return getProperties().getHeader(name);
        }

        @Override
        public boolean containsHeaders(Set<String> names)
        {
            final BasicContentHeaderProperties properties = getProperties();
            for(String name : names)
            {
                if(!properties.containsHeader(name))
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            return getProperties().getHeaderNames();
        }

        @Override
        public boolean containsHeader(String name)
        {
            return getProperties().containsHeader(name);
        }



    }
}
