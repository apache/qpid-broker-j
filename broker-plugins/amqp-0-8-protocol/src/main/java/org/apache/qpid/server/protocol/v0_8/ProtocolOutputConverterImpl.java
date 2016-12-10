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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQBody;
import org.apache.qpid.framing.AMQDataBlock;
import org.apache.qpid.framing.AMQFrame;
import org.apache.qpid.framing.AMQMethodBody;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicCancelOkBody;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.MessagePublishInfo;
import org.apache.qpid.protocol.AMQVersionAwareProtocolSession;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageContentSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.util.GZIPUtils;

public class ProtocolOutputConverterImpl implements ProtocolOutputConverter
{
    private static final int BASIC_CLASS_ID = 60;
    private final AMQPConnection_0_8Impl _connection;
    private static final AMQShortString GZIP_ENCODING = AMQShortString.valueOf(GZIPUtils.GZIP_CONTENT_ENCODING);

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolOutputConverterImpl.class);

    public ProtocolOutputConverterImpl(AMQPConnection_0_8Impl connection)
    {
        _connection = connection;
    }


    public long writeDeliver(final ServerMessage m,
                             final InstanceProperties props, int channelId,
                             long deliveryTag,
                             AMQShortString consumerTag)
    {
        MessageConverter<ServerMessage, AMQMessage> messageConverter = null;
        final AMQMessage msg;
        if(m instanceof AMQMessage)
        {
            msg = (AMQMessage) m;
        }
        else
        {
            messageConverter = getMessageConverter(m);
            msg = messageConverter.convert(m, _connection.getAddressSpace());
        }
        final boolean isRedelivered = Boolean.TRUE.equals(props.getProperty(InstanceProperties.Property.REDELIVERED));
        AMQBody deliverBody = createEncodedDeliverBody(msg, isRedelivered, deliveryTag, consumerTag);
        final long result = writeMessageDelivery(msg, channelId, deliverBody);
        if(messageConverter != null)
        {
            messageConverter.dispose(msg);
        }
        return result;
    }

    private <M extends ServerMessage> MessageConverter<M, AMQMessage> getMessageConverter(M message)
    {
        Class<M> clazz = (Class<M>) message.getClass();
        return MessageConverterRegistry.getConverter(clazz, AMQMessage.class);
    }

    private long writeMessageDelivery(AMQMessage message, int channelId, AMQBody deliverBody)
    {
        return writeMessageDelivery(message, message.getContentHeaderBody(), channelId, deliverBody);
    }

    interface DisposableMessageContentSource extends MessageContentSource
    {
        void dispose();
    }

    private long writeMessageDelivery(MessageContentSource message, ContentHeaderBody contentHeaderBody, int channelId, AMQBody deliverBody)
    {

        int bodySize = (int) message.getSize();
        boolean msgCompressed = isCompressed(contentHeaderBody);
        DisposableMessageContentSource modifiedContent = null;

        boolean compressionSupported = _connection.isCompressionSupported();


        long length;
        if(msgCompressed
           && !compressionSupported
           && (modifiedContent = inflateIfPossible(message)) != null)
        {
            BasicContentHeaderProperties modifiedProps =
                    new BasicContentHeaderProperties(contentHeaderBody.getProperties());
            modifiedProps.setEncoding((String)null);

            length = writeMessageDeliveryModified(modifiedContent, channelId, deliverBody, modifiedProps);
       }
        else if(!msgCompressed
                && compressionSupported
                && contentHeaderBody.getProperties().getEncoding()==null
                && bodySize > _connection.getMessageCompressionThreshold()
                && (modifiedContent = deflateIfPossible(message)) != null)
        {
            BasicContentHeaderProperties modifiedProps =
                    new BasicContentHeaderProperties(contentHeaderBody.getProperties());
            modifiedProps.setEncoding(GZIP_ENCODING);

            length = writeMessageDeliveryModified(modifiedContent, channelId, deliverBody, modifiedProps);
        }
        else
        {
            writeMessageDeliveryUnchanged(message, channelId, deliverBody, contentHeaderBody, bodySize);

            length = bodySize;
        }

        if (modifiedContent != null)
        {
            modifiedContent.dispose();
        }

        return length;
    }

    private DisposableMessageContentSource deflateIfPossible(MessageContentSource source)
    {
        Collection<QpidByteBuffer> contentBuffers = source.getContent(0, (int) source.getSize());
        try
        {
            return new ModifiedContentSource(QpidByteBuffer.deflate(contentBuffers));
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to compress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
        finally
        {
            for (QpidByteBuffer contentBuffer : contentBuffers)
            {
                contentBuffer.dispose();
            }
        }
    }


    private DisposableMessageContentSource inflateIfPossible(MessageContentSource source)
    {
        Collection<QpidByteBuffer> contentBuffers = source.getContent(0, (int) source.getSize());
        try
        {
            return new ModifiedContentSource(QpidByteBuffer.inflate(contentBuffers));
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to decompress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
        finally
        {
            for (QpidByteBuffer contentBuffer : contentBuffers)
            {
                contentBuffer.dispose();
            }
        }
    }


    private int writeMessageDeliveryModified(final MessageContentSource content, final int channelId,
                                             final AMQBody deliverBody,
                                             final BasicContentHeaderProperties modifiedProps)
    {
        final int bodySize = (int) content.getSize();
        ContentHeaderBody modifiedHeaderBody = new ContentHeaderBody(modifiedProps, bodySize);
        writeMessageDeliveryUnchanged(content, channelId, deliverBody, modifiedHeaderBody, bodySize);
        return bodySize;
    }


    private void writeMessageDeliveryUnchanged(MessageContentSource content,
                                               int channelId, AMQBody deliverBody, ContentHeaderBody contentHeaderBody,
                                               int bodySize)
    {
        if (bodySize == 0)
        {
            SmallCompositeAMQBodyBlock compositeBlock = new SmallCompositeAMQBodyBlock(channelId, deliverBody,
                                                                                       contentHeaderBody);

            writeFrame(compositeBlock);
        }
        else
        {
            int maxBodySize = (int) _connection.getMaxFrameSize() - AMQFrame.getFrameOverhead();


            int capacity = bodySize > maxBodySize ? maxBodySize : bodySize;

            int writtenSize = capacity;

            AMQBody firstContentBody = new MessageContentSourceBody(content, 0, capacity);

            CompositeAMQBodyBlock
                    compositeBlock =
                    new CompositeAMQBodyBlock(channelId, deliverBody, contentHeaderBody, firstContentBody);
            writeFrame(compositeBlock);

            while (writtenSize < bodySize)
            {
                capacity = bodySize - writtenSize > maxBodySize ? maxBodySize : bodySize - writtenSize;
                AMQBody body = new MessageContentSourceBody(content, writtenSize, capacity);
                writtenSize += capacity;

                writeFrame(new AMQFrame(channelId, body));
            }
        }
    }

    private boolean isCompressed(final ContentHeaderBody contentHeaderBody)
    {
        return GZIP_ENCODING.equals(contentHeaderBody.getProperties().getEncoding());
    }

    private class MessageContentSourceBody implements AMQBody
    {
        public static final byte TYPE = 3;
        private final int _length;
        private final MessageContentSource _content;
        private final int _offset;

        public MessageContentSourceBody(MessageContentSource content, int offset, int length)
        {
            _content = content;
            _offset = offset;
            _length = length;
        }

        public byte getFrameType()
        {
            return TYPE;
        }

        public int getSize()
        {
            return _length;
        }

        @Override
        public long writePayload(final ByteBufferSender sender)
        {
            long size = 0L;
            for(QpidByteBuffer buf : _content.getContent(_offset, _length))
            {
                size += buf.remaining();

                sender.send(buf);
                buf.dispose();
            }
            return size;
        }

        public void handle(int channelId, AMQVersionAwareProtocolSession amqProtocolSession) throws QpidException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "[" + getClass().getSimpleName() + " offset: " + _offset + ", length: " + _length + "]";
        }

    }

    public long writeGetOk(final ServerMessage msg,
                           final InstanceProperties props,
                           int channelId,
                           long deliveryTag,
                           int queueSize)
    {
        final AMQMessage amqMessage;
        MessageConverter<ServerMessage, AMQMessage> messageConverter = null;
        if(msg instanceof AMQMessage)
        {
            amqMessage = (AMQMessage) msg;
        }
        else
        {
            messageConverter = getMessageConverter(msg);
            amqMessage = messageConverter.convert(msg, _connection.getAddressSpace());
        }
        AMQBody deliver = createEncodedGetOkBody(amqMessage, props, deliveryTag, queueSize);
        final long result = writeMessageDelivery(amqMessage, channelId, deliver);
        if(messageConverter != null)
        {
            messageConverter.dispose(amqMessage);
        }
        return result;
    }


    private AMQBody createEncodedDeliverBody(AMQMessage message,
                                             boolean isRedelivered,
                                             final long deliveryTag,
                                             final AMQShortString consumerTag)
    {

        final AMQShortString exchangeName;
        final AMQShortString routingKey;

        final MessagePublishInfo pb = message.getMessagePublishInfo();
        exchangeName = pb.getExchange();
        routingKey = pb.getRoutingKey();

        return new EncodedDeliveryBody(deliveryTag, routingKey, exchangeName, consumerTag, isRedelivered);
    }

    private class EncodedDeliveryBody implements AMQBody
    {
        private final long _deliveryTag;
        private final AMQShortString _routingKey;
        private final AMQShortString _exchangeName;
        private final AMQShortString _consumerTag;
        private final boolean _isRedelivered;
        private AMQBody _underlyingBody;

        private EncodedDeliveryBody(long deliveryTag, AMQShortString routingKey, AMQShortString exchangeName, AMQShortString consumerTag, boolean isRedelivered)
        {
            _deliveryTag = deliveryTag;
            _routingKey = routingKey;
            _exchangeName = exchangeName;
            _consumerTag = consumerTag;
            _isRedelivered = isRedelivered;
        }

        public AMQBody createAMQBody()
        {
            return _connection.getMethodRegistry().createBasicDeliverBody(_consumerTag,
                                                                               _deliveryTag,
                                                                               _isRedelivered,
                                                                               _exchangeName,
                                                                               _routingKey);
        }

        public byte getFrameType()
        {
            return AMQMethodBody.TYPE;
        }

        public int getSize()
        {
            if(_underlyingBody == null)
            {
                _underlyingBody = createAMQBody();
            }
            return _underlyingBody.getSize();
        }

        public long writePayload(ByteBufferSender sender)
        {
            if(_underlyingBody == null)
            {
                _underlyingBody = createAMQBody();
            }
            return _underlyingBody.writePayload(sender);
        }

        public void handle(final int channelId, final AMQVersionAwareProtocolSession amqProtocolSession)
            throws QpidException
        {
            throw new QpidException("This block should never be dispatched!");
        }

        @Override
        public String toString()
        {
            return "[" + getClass().getSimpleName() + " underlyingBody: " + String.valueOf(_underlyingBody) + "]";
        }
    }

    private AMQBody createEncodedGetOkBody(AMQMessage message, InstanceProperties props, long deliveryTag, int queueSize)
    {
        final AMQShortString exchangeName;
        final AMQShortString routingKey;

        final MessagePublishInfo pb = message.getMessagePublishInfo();
        exchangeName = pb.getExchange();
        routingKey = pb.getRoutingKey();

        final boolean isRedelivered = Boolean.TRUE.equals(props.getProperty(InstanceProperties.Property.REDELIVERED));

        return _connection.getMethodRegistry().createBasicGetOkBody(deliveryTag,
                                                            isRedelivered,
                                                            exchangeName,
                                                            routingKey,
                                                            queueSize);
    }

    private AMQBody createEncodedReturnFrame(MessagePublishInfo messagePublishInfo,
                                             int replyCode,
                                             AMQShortString replyText)
    {


        return _connection.getMethodRegistry().createBasicReturnBody(replyCode,
                                                             replyText,
                                                             messagePublishInfo.getExchange(),
                                                             messagePublishInfo.getRoutingKey());
    }

    public void writeReturn(MessagePublishInfo messagePublishInfo, ContentHeaderBody header, MessageContentSource message, int channelId, int replyCode, AMQShortString replyText)
    {

        AMQBody returnFrame = createEncodedReturnFrame(messagePublishInfo, replyCode, replyText);

        writeMessageDelivery(message, header, channelId, returnFrame);
    }


    public void writeFrame(AMQDataBlock block)
    {
        _connection.writeFrame(block);
    }


    public void confirmConsumerAutoClose(int channelId, AMQShortString consumerTag)
    {

        BasicCancelOkBody basicCancelOkBody = _connection.getMethodRegistry().createBasicCancelOkBody(consumerTag);
        writeFrame(basicCancelOkBody.generateFrame(channelId));

    }


    public static final class CompositeAMQBodyBlock extends AMQDataBlock
    {
        public static final int OVERHEAD = 3 * AMQFrame.getFrameOverhead();

        private final AMQBody _methodBody;
        private final AMQBody _headerBody;
        private final AMQBody _contentBody;
        private final int _channel;


        public CompositeAMQBodyBlock(int channel, AMQBody methodBody, AMQBody headerBody, AMQBody contentBody)
        {
            _channel = channel;
            _methodBody = methodBody;
            _headerBody = headerBody;
            _contentBody = contentBody;
        }

        public long getSize()
        {
            return OVERHEAD + (long)_methodBody.getSize() + (long)_headerBody.getSize() + (long)_contentBody.getSize();
        }

        @Override
        public long writePayload(final ByteBufferSender sender)
        {
            long size = (new AMQFrame(_channel, _methodBody)).writePayload(sender);

            size += (new AMQFrame(_channel, _headerBody)).writePayload(sender);

            size += (new AMQFrame(_channel, _contentBody)).writePayload(sender);

            return size;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("[").append(getClass().getSimpleName())
                .append(" methodBody=").append(_methodBody)
                .append(", headerBody=").append(_headerBody)
                .append(", contentBody=").append(_contentBody)
                .append(", channel=").append(_channel).append("]");
            return builder.toString();
        }

    }

    public static final class SmallCompositeAMQBodyBlock extends AMQDataBlock
    {
        public static final int OVERHEAD = 2 * AMQFrame.getFrameOverhead();

        private final AMQBody _methodBody;
        private final AMQBody _headerBody;
        private final int _channel;


        public SmallCompositeAMQBodyBlock(int channel, AMQBody methodBody, AMQBody headerBody)
        {
            _channel = channel;
            _methodBody = methodBody;
            _headerBody = headerBody;

        }

        public long getSize()
        {
            return OVERHEAD + (long)_methodBody.getSize() + (long)_headerBody.getSize() ;
        }

        @Override
        public long writePayload(final ByteBufferSender sender)
        {
            long size = (new AMQFrame(_channel, _methodBody)).writePayload(sender);
            size += (new AMQFrame(_channel, _headerBody)).writePayload(sender);
            return size;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append(getClass().getSimpleName())
                .append("methodBody=").append(_methodBody)
                .append(", headerBody=").append(_headerBody)
                .append(", channel=").append(_channel).append("]");
            return builder.toString();
        }
    }

    private static class ModifiedContentSource implements DisposableMessageContentSource
    {
        private final Collection<QpidByteBuffer> _buffers;
        private final int _size;

        public ModifiedContentSource(final Collection<QpidByteBuffer> buffers)
        {
            _buffers = buffers;
            int size = 0;
            for(QpidByteBuffer buf : buffers)
            {
                size += buf.remaining();
            }
            _size = size;
        }

        @Override
        public void dispose()
        {
            for(QpidByteBuffer buffer : _buffers)
            {
                buffer.dispose();
            }
        }

        @Override
        public Collection<QpidByteBuffer> getContent(final int offset, int length)
        {
            Collection<QpidByteBuffer> content = new ArrayList<>(_buffers.size());
            int pos = 0;
            for (QpidByteBuffer buf : _buffers)
            {
                if (length > 0)
                {
                    int bufRemaining = buf.remaining();
                    if (pos + bufRemaining <= offset)
                    {
                        pos += bufRemaining;
                    }
                    else if (pos >= offset)
                    {
                        buf = buf.duplicate();
                        if (bufRemaining <= length)
                        {
                            length -= bufRemaining;
                        }
                        else
                        {
                            buf.limit(length);
                            length = 0;
                        }
                        content.add(buf);
                        pos+=buf.remaining();

                    }
                    else
                    {
                        int offsetInBuf = offset - pos;
                        int limit = length < bufRemaining - offsetInBuf ? length : bufRemaining - offsetInBuf;
                        final QpidByteBuffer bufView = buf.view(offsetInBuf, limit);
                        content.add(bufView);
                        length -= limit;
                        pos+=limit+offsetInBuf;
                    }
                }

            }
            return content;

        }

        @Override
        public long getSize()
        {
            return _size;
        }
    }
}
