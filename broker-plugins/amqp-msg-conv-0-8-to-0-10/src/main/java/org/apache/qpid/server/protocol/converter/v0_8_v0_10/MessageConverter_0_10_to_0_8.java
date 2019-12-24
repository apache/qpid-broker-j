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
package org.apache.qpid.server.protocol.converter.v0_8_v0_10;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQPInvalidClassException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;

@PluggableService
public class MessageConverter_0_10_to_0_8 implements MessageConverter<MessageTransferMessage, AMQMessage>
{
    private static final int BASIC_CLASS_ID = 60;

    public static BasicContentHeaderProperties convertContentHeaderProperties(MessageTransferMessage messageTransferMessage,
                                                                              NamedAddressSpace addressSpace)
    {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();

        Header header = messageTransferMessage.getHeader();
        DeliveryProperties deliveryProps = header.getDeliveryProperties();
        MessageProperties messageProps = header.getMessageProperties();

        if(deliveryProps != null)
        {
            if(deliveryProps.hasDeliveryMode())
            {
                props.setDeliveryMode((deliveryProps.getDeliveryMode() == MessageDeliveryMode.PERSISTENT
                                              ? BasicContentHeaderProperties.PERSISTENT
                                              : BasicContentHeaderProperties.NON_PERSISTENT));
            }

            if (deliveryProps.hasTtl())
            {
                props.setExpiration(messageTransferMessage.getArrivalTime() + deliveryProps.getTtl());
            }
            else if(deliveryProps.hasExpiration())
            {
                props.setExpiration(deliveryProps.getExpiration());
            }

            if(deliveryProps.hasPriority())
            {
                props.setPriority((byte) deliveryProps.getPriority().getValue());
            }

            if(deliveryProps.hasTimestamp())
            {
                props.setTimestamp(deliveryProps.getTimestamp());
            }
            else
            {
                props.setTimestamp(messageTransferMessage.getArrivalTime());
            }
        }
        if(messageProps != null)
        {
            if(messageProps.hasAppId())
            {
                try
                {
                    props.setAppId(AMQShortString.createAMQShortString(messageProps.getAppId()));
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }
            }
            if(messageProps.hasContentType())
            {
                props.setContentType(messageProps.getContentType());
            }
            if(messageProps.hasCorrelationId())
            {
                try
                {
                    props.setCorrelationId(AMQShortString.createAMQShortString(messageProps.getCorrelationId()));
                }
                catch (IllegalArgumentException e)
                {
                    throw new MessageConversionException("Could not convert message from 0-10 to 0-8 because conversion of 'correlationId' failed.", e);
                }
            }
            if(messageProps.hasContentEncoding())
            {
                props.setEncoding(messageProps.getContentEncoding());
            }
            if(messageProps.hasMessageId())
            {
                // Add prefix 'ID:' to workaround broken 0-8..0-9-1 Qpid JMS client
                props.setMessageId("ID:" + messageProps.getMessageId().toString());
            }
            if(messageProps.hasReplyTo())
            {
                ReplyTo replyTo = messageProps.getReplyTo();
                String exchangeName = replyTo.getExchange();
                String routingKey = replyTo.getRoutingKey();
                if(exchangeName == null)
                {
                    exchangeName = "";
                }

                if (!"".equals(exchangeName) || (routingKey != null && !"".equals(routingKey)))
                {
                    MessageDestination destination = addressSpace.getAttainedMessageDestination(exchangeName, false);
                    Exchange<?> exchange = destination instanceof Exchange ? (Exchange<?>) destination : null;

                    String exchangeClass = exchange == null
                            ? ExchangeDefaults.DIRECT_EXCHANGE_CLASS
                            : exchange.getType();
                    String routingKeyOption = routingKey == null ? "" : "?routingkey='" + routingKey + "'";
                    final String replyToBindingUrl =
                            String.format("%s://%s//%s", exchangeClass, exchangeName, routingKeyOption);
                    try
                    {
                        props.setReplyTo(replyToBindingUrl);
                    }
                    catch (IllegalArgumentException e)
                    {
                        throw new MessageConversionException("Could not convert message from 0-10 to 0-8 because conversion of 'reply-to' failed.", e);
                    }
                }
            }
            if(messageProps.hasUserId())
            {
                try
                {
                    props.setUserId(AMQShortString.createAMQShortString(messageProps.getUserId()));
                }
                catch (IllegalArgumentException e)
                {
                    // ignore
                }
            }

            if(messageProps.hasApplicationHeaders())
            {
                Map<String, Object> appHeaders = new HashMap<String, Object>(messageProps.getApplicationHeaders());
                if(messageProps.getApplicationHeaders().containsKey("x-jms-type"))
                {
                    String jmsType = String.valueOf(appHeaders.remove("x-jms-type"));
                    try
                    {
                        props.setType(jmsType);
                    }
                    catch (IllegalArgumentException e)
                    {
                        throw new MessageConversionException("Could not convert message from 0-10 to 0-8 because x-jms-type conversion failed.", e);
                    }
                }

                FieldTable ft;
                try
                {
                    ft = FieldTableFactory.createFieldTable(appHeaders);
                }
                catch (IllegalArgumentException | AMQPInvalidClassException e)
                {
                    throw new MessageConversionException("Could not convert message from 0-10 to 0-8 because conversion of application headers failed.", e);
                }
                props.setHeaders(ft);
            }
        }

        return props;
    }

    @Override
    public Class<MessageTransferMessage> getInputClass()
    {
        return MessageTransferMessage.class;
    }

    @Override
    public Class<AMQMessage> getOutputClass()
    {
        return AMQMessage.class;
    }

    @Override
    public AMQMessage convert(MessageTransferMessage message, NamedAddressSpace addressSpace)
    {
        return new AMQMessage(convertToStoredMessage(message, addressSpace));
    }

    @Override
    public void dispose(final AMQMessage message)
    {

    }

    private StoredMessage<MessageMetaData> convertToStoredMessage(final MessageTransferMessage message,
                                                                  NamedAddressSpace addressSpace)
    {
        final MessageMetaData metaData = convertMetaData(message, addressSpace);
        final int metadataSize = metaData.getStorableSize();
        return new StoredMessage<org.apache.qpid.server.protocol.v0_8.MessageMetaData>()
        {
            @Override
            public MessageMetaData getMetaData()
            {
                return metaData;
            }

            @Override
            public long getMessageNumber()
            {
                return message.getMessageNumber();
            }

            @Override
            public QpidByteBuffer getContent(final int offset, final int length)
            {
                return message.getContent(offset, length);
            }

            @Override
            public int getContentSize()
            {
                return metaData.getContentSize();
            }

            @Override
            public int getMetadataSize()
            {
                return metadataSize;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isInContentInMemory()
            {
                return true;
            }

            @Override
            public long getInMemorySize()
            {
                return getMetadataSize() + getContentSize();
            }

            @Override
            public boolean flowToDisk()
            {
                return false;
            }

            @Override
            public void reallocate()
            {

            }
        };
    }

    private MessageMetaData convertMetaData(MessageTransferMessage message, NamedAddressSpace addressSpace)
    {
        return new MessageMetaData(convertPublishBody(message),
                convertContentHeaderBody(message, addressSpace),
                message.getArrivalTime());
    }

    private ContentHeaderBody convertContentHeaderBody(MessageTransferMessage message, NamedAddressSpace addressSpace)
    {
        BasicContentHeaderProperties props = convertContentHeaderProperties(message, addressSpace);
        ContentHeaderBody chb = new ContentHeaderBody(props);
        chb.setBodySize(message.getSize());
        return chb;
    }

    private MessagePublishInfo convertPublishBody(MessageTransferMessage message)
    {
        DeliveryProperties delvProps = message.getHeader().getDeliveryProperties();
        final AMQShortString exchangeName = (delvProps == null || delvProps.getExchange() == null)
                                            ? null
                                            : AMQShortString.createAMQShortString(delvProps.getExchange());
        final AMQShortString routingKey = (delvProps == null || delvProps.getRoutingKey() == null)
                                          ? null
                                          : AMQShortString.createAMQShortString(delvProps.getRoutingKey());
        final boolean immediate = delvProps != null && delvProps.getImmediate();
        final boolean mandatory = delvProps != null && !delvProps.getDiscardUnroutable();

        return new MessagePublishInfo(exchangeName, immediate, mandatory, routingKey);
    }

    @Override
    public String getType()
    {
        return "v0-10 to v0-8";
    }
}
