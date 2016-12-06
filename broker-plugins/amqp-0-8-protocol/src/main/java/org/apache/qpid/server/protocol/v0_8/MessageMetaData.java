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

import java.util.Collection;
import java.util.Set;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQFrameDecodingException;
import org.apache.qpid.framing.AMQProtocolVersionException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.EncodingUtils;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.MessagePublishInfo;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.plugin.MessageMetaDataType;
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


    private long _arrivalTime;
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


    public synchronized ContentHeaderBody getContentHeaderBody()
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

    public MessageMetaDataType getType()
    {
        return TYPE;
    }

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

    public int getContentSize()
    {
        return (int) _contentHeaderBody.getBodySize();
    }

    public boolean isPersistent()
    {
        return _contentHeaderBody.getProperties().getDeliveryMode() ==  BasicContentHeaderProperties.PERSISTENT;
    }

    @Override
    public synchronized void dispose()
    {
        _contentHeaderBody.dispose();
    }

    public synchronized void clearEncodedForm()
    {
        _contentHeaderBody.clearEncodedForm();
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MessageMetaData>
    {


        public MessageMetaData createMetaData(QpidByteBuffer buf)
        {
            try
            {
                int size = buf.getInt();
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
            catch (AMQFrameDecodingException | AMQProtocolVersionException e)
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

        public String getUserId()
        {
            return getProperties().getUserIdAsString();
        }

        public String getAppId()
        {
            return getProperties().getAppIdAsString();
        }

        public String getCorrelationId()
        {
            return getProperties().getCorrelationIdAsString();
        }

        public long getExpiration()
        {
            return getProperties().getExpiration();
        }

        public String getMessageId()
        {
            return getProperties().getMessageIdAsString();
        }

        public String getMimeType()
        {
            return getProperties().getContentTypeAsString();
        }

        public String getEncoding()
        {
            return getProperties().getEncodingAsString();
        }

        public byte getPriority()
        {
            return getProperties().getPriority();
        }

        public long getTimestamp()
        {
            return getProperties().getTimestamp();
        }

        public String getType()
        {
            return getProperties().getTypeAsString();
        }

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

        public Object getHeader(String name)
        {
            FieldTable ft = getProperties().getHeaders();
            return ft.get(name);
        }

        public boolean containsHeaders(Set<String> names)
        {
            FieldTable ft = getProperties().getHeaders();
            for(String name : names)
            {
                if(!ft.containsKey(name))
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            return getProperties().getHeaders().keys();
        }

        public boolean containsHeader(String name)
        {
            FieldTable ft = getProperties().getHeaders();
            return ft.containsKey(name);
        }



    }
}
