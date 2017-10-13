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
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

@PluggableService
public class MessageFormat_0_9_1 implements MessageFormat<AMQMessage>
{

    public static final int AMQP_MESSAGE_FORMAT_0_9_1 = 91;
    private static final byte MANDATORY_MASK = (byte)1;
    private static final byte IMMEDIATE_MASK = (byte)2;


    @Override
    public String getType()
    {
        return "AMQP_0_9_1";
    }

    @Override
    public int getSupportedFormat()
    {
        return AMQP_MESSAGE_FORMAT_0_9_1;
    }

    @Override
    public Class<AMQMessage> getMessageClass()
    {
        return AMQMessage.class;
    }

    @Override
    public QpidByteBuffer convertToMessageFormat(final AMQMessage message)
    {
        final MessagePublishInfo messagePublishInfo = message.getMessagePublishInfo();
        final ContentHeaderBody contentHeaderBody = message.getContentHeaderBody();
        AMQShortString exchange = messagePublishInfo.getExchange();
        AMQShortString routingKey = messagePublishInfo.getRoutingKey();
        int length =  contentHeaderBody.getSize() + (exchange == null ? 0 : exchange.length()) + (routingKey == null ? 0 : routingKey.length()) + 3;
        try (QpidByteBuffer headerBuf = QpidByteBuffer.allocateDirect(length);
             QpidByteBuffer content = message.getContent())
        {
            EncodingUtils.writeShortStringBytes(headerBuf, exchange);
            EncodingUtils.writeShortStringBytes(headerBuf, routingKey);
            byte flags = messagePublishInfo.isMandatory() ? (byte) 0 : MANDATORY_MASK;
            if (messagePublishInfo.isImmediate())
            {
                flags |= IMMEDIATE_MASK;
            }
            headerBuf.put(flags);
            headerBuf.flip();

            contentHeaderBody.writePayload(headerBuf);

            return QpidByteBuffer.concatenate(headerBuf, content);
        }
    }

    @Override
    public AMQMessage createMessage(final QpidByteBuffer payload,
                                    final MessageStore store,
                                    final Object connectionReference)
    {
        try
        {
            AMQShortString exchange = readShortString(payload);
            AMQShortString routingKey = readShortString(payload);
            byte flags = payload.get();
            final MessagePublishInfo publishBody = new MessagePublishInfo(exchange,
                                                                          (flags & IMMEDIATE_MASK) != 0,
                                                                          (flags & MANDATORY_MASK) != 0,
                                                                          routingKey);
            final ContentHeaderBody contentHeaderBody = readContentBody(payload);
            MessageMetaData mmd = new MessageMetaData(publishBody, contentHeaderBody);

            final MessageHandle<MessageMetaData> handle = store.addMessage(mmd);
            handle.addContent(payload);
            final StoredMessage<MessageMetaData> storedMessage = handle.allContentAdded();

            return new AMQMessage(storedMessage, connectionReference);
        }
        catch (AMQFrameDecodingException | BufferUnderflowException e )
        {
            throw new ConnectionScopedRuntimeException("Error parsing AMQP 0-9-1 message format", e);
        }
    }

    private ContentHeaderBody readContentBody(final QpidByteBuffer buf) throws AMQFrameDecodingException
    {
        long size = buf.getUnsignedInt();
        try (QpidByteBuffer buffer = readByteBuffer(buf, size))
        {
            final long newPosition = buf.position() + size;
            if (newPosition > Integer.MAX_VALUE)
            {
                throw new IllegalStateException(String.format("trying to advance QBB to %d which is larger than MAX_INT",
                                                              newPosition));
            }
            buf.position((int) newPosition);
            return new ContentHeaderBody(buffer, size);
        }
    }

    private QpidByteBuffer readByteBuffer(final QpidByteBuffer data, final long size)
    {
        return data.view(0, (int)size);
    }

    private int readInt(final List<QpidByteBuffer> data)
    {
        int required = 4;
        int result = 0;
        for(QpidByteBuffer buf : data)
        {
            if(required == 4 && buf.remaining() >= 4)
            {
                return buf.getInt();
            }
            else
            {
                while(buf.remaining() > 0)
                {
                    result <<= 8;
                    result |= ((int)buf.get()) & 0xff;
                    if(--required == 0)
                    {
                        return result;
                    }
                }
            }
        }
        throw new BufferUnderflowException();
    }

    private AMQShortString readShortString(final QpidByteBuffer data)
    {
        return AMQShortString.readAMQShortString(data);
    }
}
