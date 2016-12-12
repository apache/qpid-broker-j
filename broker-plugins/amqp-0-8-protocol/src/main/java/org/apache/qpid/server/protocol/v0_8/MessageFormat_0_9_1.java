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
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQFrameDecodingException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.EncodingUtils;
import org.apache.qpid.framing.MessagePublishInfo;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

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
    public List<QpidByteBuffer> convertToMessageFormat(final AMQMessage message)
    {
        final MessagePublishInfo messagePublishInfo = message.getMessagePublishInfo();
        final ContentHeaderBody contentHeaderBody = message.getContentHeaderBody();
        AMQShortString exchange = messagePublishInfo.getExchange();
        AMQShortString routingKey = messagePublishInfo.getRoutingKey();
        int length =  contentHeaderBody.getSize() + (exchange == null ? 0 : exchange.length()) + (routingKey == null ? 0 : routingKey.length()) + 3;
        QpidByteBuffer headerBuf = QpidByteBuffer.allocateDirect(length);
        EncodingUtils.writeShortStringBytes(headerBuf, exchange);
        EncodingUtils.writeShortStringBytes(headerBuf, routingKey);
        byte flags = messagePublishInfo.isMandatory() ? (byte)0 : MANDATORY_MASK;
        if(messagePublishInfo.isImmediate())
        {
            flags |= IMMEDIATE_MASK;
        }
        headerBuf.put(flags);
        headerBuf.flip();

        contentHeaderBody.writePayload(headerBuf);
        List<QpidByteBuffer> bufs = new ArrayList<>();
        headerBuf.flip();
        bufs.add(headerBuf);
        bufs.addAll(message.getContent(0, (int) contentHeaderBody.getBodySize()));

        return bufs;
    }

    @Override
    public AMQMessage createMessage(final List<QpidByteBuffer> buf,
                                    final MessageStore store,
                                    final Object connectionReference)
    {
        try
        {
            AMQShortString exchange = readShortString(buf);
            AMQShortString routingKey = readShortString(buf);
            byte flags = readByte(buf);
            final MessagePublishInfo publishBody = new MessagePublishInfo(exchange,
                                                                          (flags & IMMEDIATE_MASK) != 0,
                                                                          (flags & MANDATORY_MASK) != 0,
                                                                          routingKey);
            final ContentHeaderBody contentHeaderBody = readContentBody(buf);
            MessageMetaData mmd = new MessageMetaData(publishBody, contentHeaderBody);

            final MessageHandle<MessageMetaData> handle = store.addMessage(mmd);
            for (QpidByteBuffer content : buf)
            {
                if (content.hasRemaining())
                {
                    handle.addContent(content);
                }
            }
            final StoredMessage<MessageMetaData> storedMessage = handle.allContentAdded();

            return new AMQMessage(storedMessage, connectionReference);
        }
        catch (AMQFrameDecodingException | BufferUnderflowException e )
        {
            throw new ConnectionScopedRuntimeException("Error parsing AMQP 0-9-1 message format", e);
        }
    }

    private ContentHeaderBody readContentBody(final List<QpidByteBuffer> buf) throws AMQFrameDecodingException
    {
        long size = ((long) readInt(buf)) & 0xffffffffL;
        final QpidByteBuffer buffer = readByteBuffer(buf, size);
        final ContentHeaderBody contentHeaderBody = new ContentHeaderBody(buffer, size);
        buffer.dispose();
        return contentHeaderBody;
    }

    private QpidByteBuffer readByteBuffer(final List<QpidByteBuffer> data, final long size)
    {
        QpidByteBuffer result = null;
        for(QpidByteBuffer buf : data)
        {
            if(result == null && buf.remaining()>= size)
            {
                return buf.view(0, (int)size);
            }
            else if(buf.hasRemaining())
            {
                if(result == null)
                {
                    result = QpidByteBuffer.allocateDirect((int)size);
                }
                if(buf.remaining()>result.remaining())
                {
                    QpidByteBuffer dup = buf.view(0, result.remaining());
                    result.put(dup);
                    dup.dispose();
                }
                else
                {
                    result.put(buf);
                }
                if(!result.hasRemaining())
                {
                    result.flip();
                    return result;
                }
            }
        }
        throw new BufferUnderflowException();
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

    private byte readByte(final List<QpidByteBuffer> data)
    {
        for(QpidByteBuffer buf : data)
        {
            if(buf.hasRemaining())
            {
                return buf.get();
            }
        }
        throw new BufferUnderflowException();
    }

    private AMQShortString readShortString(final List<QpidByteBuffer> data)
    {
        for(QpidByteBuffer buf : data)
        {
            if(buf.hasRemaining())
            {
                int length = ((int)buf.get(buf.position())) & 0xff;
                if(buf.remaining()>length)
                {
                    return AMQShortString.readAMQShortString(buf);
                }
            }
        }
        int length = ((int)readByte(data)) & 0xff;
        byte[] octets = new byte[length];
        readByteArray(octets, data);
        return new AMQShortString(octets);
    }

    private void readByteArray(final byte[] octets, final List<QpidByteBuffer> data)
    {
        int offset = 0;
        for(QpidByteBuffer buf : data)
        {
            final int remaining = buf.remaining();
            if(remaining >= octets.length-offset)
            {
                buf.get(octets, offset, octets.length-offset);
                return;
            }
            else if(remaining > 0)
            {
                buf.get(octets, offset, remaining);
                offset+=remaining;
            }
        }
        throw new BufferUnderflowException();
    }

    @Override
    public String getRoutingAddress(final AMQMessage message, final String destinationAddress)
    {
        String initialRoutingAddress = message.getInitialRoutingAddress();
        if(initialRoutingAddress != null && destinationAddress != null && initialRoutingAddress.startsWith(destinationAddress+"/"))
        {
            initialRoutingAddress = initialRoutingAddress.substring(destinationAddress.length() + 1);
        }
        return initialRoutingAddress;
    }
}
