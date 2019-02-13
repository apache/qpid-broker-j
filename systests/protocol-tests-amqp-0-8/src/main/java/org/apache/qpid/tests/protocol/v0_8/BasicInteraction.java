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
package org.apache.qpid.tests.protocol.v0_8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.CompositeAMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;

public class BasicInteraction
{
    private final Interaction _interaction;
    private String _publishExchange;
    private String _publishRoutingKey;
    private boolean _publishMandatory;
    private boolean _publishImmediate;
    private byte[] _content;
    private FieldTable _contentHeaderPropertiesHeaders;
    private String _contentHeaderPropertiesContentType;
    private byte _contentHeaderPropertiesDeliveryMode;
    private byte _contentHeaderPropertiesPriority;
    private int _qosPrefetchCount;
    private long _qosPrefetchSize;
    private boolean _qosGlobal;
    private String _consumeQueueName;
    private String _consumeConsumerTag;
    private boolean _consumeNoLocal;
    private boolean _consumeNoAck;
    private boolean _consumeExclusive;
    private boolean _consumeNoWait;
    private Map<String, Object> _consumeArguments = new HashMap<>();
    private long _ackDeliveryTag;
    private boolean _ackMultiple;

    private String _consumeCancelTag;
    private boolean _consumeCancelNoWait;

    private String _getQueueName;
    private boolean _getNoAck;

    private boolean _confirmSelectNoWait;

    public BasicInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }

    public Interaction publish() throws Exception
    {
        return _interaction.sendPerformative(new BasicPublishBody(0,
                                                                  AMQShortString.valueOf(_publishExchange),
                                                                  AMQShortString.valueOf(_publishRoutingKey),
                                                                  _publishMandatory,
                                                                  _publishImmediate));
    }

    public BasicInteraction content(final String content)
    {
        _content = content.getBytes(StandardCharsets.UTF_8);
        return this;
    }

    public BasicInteraction content(final byte[] content)
    {
        _content = content;
        return this;
    }

    public BasicInteraction contentHeaderPropertiesHeaders(final Map<String, Object> messageHeaders)
    {
        _contentHeaderPropertiesHeaders = FieldTable.convertToFieldTable(messageHeaders);
        return this;
    }

    public BasicInteraction contentHeaderPropertiesHeaders(final FieldTable messageHeaders)
    {
        _contentHeaderPropertiesHeaders = messageHeaders;
        return this;
    }

    public BasicInteraction contentHeaderPropertiesContentType(final String messageContentType)
    {
        _contentHeaderPropertiesContentType = messageContentType;
        return this;
    }

    public BasicInteraction contentHeaderPropertiesPriority(final byte priority)
    {
        _contentHeaderPropertiesPriority = priority;
        return this;
    }

    public BasicInteraction contentHeaderPropertiesDeliveryMode(final byte deliveryMode)
    {
        _contentHeaderPropertiesDeliveryMode = deliveryMode;
        return this;
    }

    public Interaction contentHeader(int contentSize) throws Exception
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(_contentHeaderPropertiesHeaders);
        basicContentHeaderProperties.setContentType(_contentHeaderPropertiesContentType);
        basicContentHeaderProperties.setDeliveryMode(_contentHeaderPropertiesDeliveryMode);
        basicContentHeaderProperties.setPriority(_contentHeaderPropertiesPriority);
        ContentHeaderBody contentHeaderBody = new ContentHeaderBody(basicContentHeaderProperties, contentSize);
        return _interaction.sendPerformative(contentHeaderBody);
    }

    public Interaction contentBody(final byte[] bytes) throws Exception
    {
        try (QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            final ContentBody contentBody = new ContentBody(buf);
            return _interaction.sendPerformative(contentBody);
        }
    }

    public Interaction publishMessage() throws Exception
    {
        List<AMQFrame> frames = new ArrayList<>();
        BasicPublishBody publishFrame = new BasicPublishBody(0,
                                                             AMQShortString.valueOf(_publishExchange),
                                                             AMQShortString.valueOf(_publishRoutingKey),
                                                             _publishMandatory,
                                                             _publishImmediate);
        frames.add(new AMQFrame(_interaction.getChannelId(), publishFrame));
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(_contentHeaderPropertiesHeaders);
        basicContentHeaderProperties.setContentType(_contentHeaderPropertiesContentType);
        basicContentHeaderProperties.setDeliveryMode(_contentHeaderPropertiesDeliveryMode);
        basicContentHeaderProperties.setPriority(_contentHeaderPropertiesPriority);
        final int contentSize = _content == null ? 0 : _content.length;
        ContentHeaderBody contentHeaderBody = new ContentHeaderBody(basicContentHeaderProperties, contentSize);
        frames.add(new AMQFrame(_interaction.getChannelId(), contentHeaderBody));
        if (contentSize > 0)
        {
            final byte[] contentCopy = new byte[contentSize];
            System.arraycopy(_content, 0, contentCopy, 0, contentSize);
            final int framePayloadMax = _interaction.getMaximumFrameSize() - 8;
            int offset = 0;
            do
            {
                int contentToCopyLength = Math.min(framePayloadMax, contentSize - offset);
                ContentBody contentBody = new ContentBody(ByteBuffer.wrap(contentCopy, offset,
                                                                          contentToCopyLength));
                frames.add(new AMQFrame(_interaction.getChannelId(), contentBody));
                offset += contentToCopyLength;
            }
            while (offset < contentSize);
        }

        CompositeAMQDataBlock frame = new CompositeAMQDataBlock(frames.toArray(new AMQFrame[frames.size()]));

        return _interaction.sendPerformative(frame);
    }

    public BasicInteraction publishExchange(final String exchangeName)
    {
        _publishExchange = exchangeName;
        return this;
    }

    public BasicInteraction publishRoutingKey(final String queueName)
    {
        _publishRoutingKey = queueName;
        return this;
    }

    public BasicInteraction publishMandatory(final boolean mandatory)
    {
        _publishMandatory = mandatory;
        return this;
    }

    public BasicInteraction publishImmediate(final boolean immediate)
    {
        _publishImmediate = immediate;
        return this;
    }

    public BasicInteraction qosPrefetchCount(final int prefetchCount)
    {
        _qosPrefetchCount = prefetchCount;
        return this;
    }

    public BasicInteraction qosPrefetchSize(final int prefetchSize)
    {
        _qosPrefetchSize = prefetchSize;
        return this;
    }

    public Interaction qos() throws Exception
    {
        return _interaction.sendPerformative(new BasicQosBody(_qosPrefetchSize,
                                                              _qosPrefetchCount,
                                                              _qosGlobal));
    }

    public Interaction consume() throws Exception
    {
        return _interaction.sendPerformative(new BasicConsumeBody(0,
                                                                  AMQShortString.valueOf(_consumeQueueName),
                                                                  AMQShortString.valueOf(_consumeConsumerTag),
                                                                  _consumeNoLocal,
                                                                  _consumeNoAck,
                                                                  _consumeExclusive,
                                                                  _consumeNoWait,
                                                                  FieldTable.convertToFieldTable(_consumeArguments)));
    }

    public BasicInteraction consumeConsumerTag(final String consumerTag)
    {
        _consumeConsumerTag = consumerTag;
        return this;
    }

    public BasicInteraction consumeQueue(final String queueName)
    {
        _consumeQueueName = queueName;
        return this;
    }

    public BasicInteraction consumeNoAck(final boolean noAck)
    {
        _consumeNoAck = noAck;
        return this;
    }

    public Interaction ack() throws Exception
    {
        return _interaction.sendPerformative(new BasicAckBody(_ackDeliveryTag, _ackMultiple));
    }

    public BasicInteraction ackMultiple(final boolean multiple)
    {
        _ackMultiple = multiple;
        return this;
    }

    public BasicInteraction ackDeliveryTag(final long deliveryTag)
    {
        _ackDeliveryTag = deliveryTag;
        return this;
    }

    public Interaction cancel() throws Exception
    {
        return _interaction.sendPerformative(new BasicCancelBody(AMQShortString.valueOf(_consumeCancelTag),
                                                                 _consumeCancelNoWait));
    }

    public BasicInteraction consumeCancelTag(final String consumeCancelTag)
    {
        _consumeCancelTag = consumeCancelTag;
        return this;
    }

    public Interaction get() throws Exception
    {
        return _interaction.sendPerformative(new BasicGetBody(0,
                                                              AMQShortString.valueOf(_getQueueName),
                                                              _getNoAck));
    }

    public BasicInteraction getQueueName(final String queueName)
    {
        _getQueueName = queueName;
        return this;
    }

    public BasicInteraction getNoAck(final boolean noAck)
    {
        _getNoAck = noAck;
        return this;
    }

    public Interaction confirmSelect() throws Exception
    {
        return _interaction.sendPerformative(new ConfirmSelectBody(_confirmSelectNoWait));
    }
}
