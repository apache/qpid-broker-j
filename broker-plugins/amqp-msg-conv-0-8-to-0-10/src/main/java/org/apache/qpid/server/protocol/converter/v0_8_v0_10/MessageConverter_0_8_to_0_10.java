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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.url.AMQBindingURL;

@PluggableService
public class MessageConverter_0_8_to_0_10  implements MessageConverter<AMQMessage, MessageTransferMessage>
{
    @Override
    public Class<AMQMessage> getInputClass()
    {
        return AMQMessage.class;
    }

    @Override
    public Class<MessageTransferMessage> getOutputClass()
    {
        return MessageTransferMessage.class;
    }

    @Override
    public MessageTransferMessage convert(AMQMessage message_0_8, NamedAddressSpace addressSpace)
    {
        return new MessageTransferMessage(convertToStoredMessage(message_0_8), null);
    }

    @Override
    public void dispose(final MessageTransferMessage message)
    {

    }

    private StoredMessage<MessageMetaData_0_10> convertToStoredMessage(final AMQMessage message_0_8)
    {
        final MessageMetaData_0_10 messageMetaData_0_10 = convertMetaData(message_0_8);
        final int metadataSize = messageMetaData_0_10.getStorableSize();
        return new StoredMessage<MessageMetaData_0_10>()
        {
            @Override
            public MessageMetaData_0_10 getMetaData()
            {
                return messageMetaData_0_10;
            }

            @Override
            public long getMessageNumber()
            {
                return message_0_8.getMessageNumber();
            }

            @Override
            public QpidByteBuffer getContent(final int offset, final int length)
            {
                return message_0_8.getContent(offset, length);
            }

            @Override
            public int getContentSize()
            {
                return messageMetaData_0_10.getContentSize();
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
                return getContentSize() + getMetadataSize();
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

    private MessageMetaData_0_10 convertMetaData(AMQMessage message_0_8)
    {
        DeliveryProperties deliveryProps = new DeliveryProperties();
        MessageProperties messageProps = new MessageProperties();

        int size = (int) message_0_8.getSize();

        BasicContentHeaderProperties properties =
                  message_0_8.getContentHeaderBody().getProperties();

        final AMQShortString exchange = message_0_8.getMessagePublishInfo().getExchange();
        if(exchange != null)
        {
            deliveryProps.setExchange(exchange.toString());
        }

        deliveryProps.setExpiration(message_0_8.getExpiration());
        if (message_0_8.getExpiration() != 0)
        {
            deliveryProps.setTtl(message_0_8.getExpiration() - message_0_8.getArrivalTime());
        }
        deliveryProps.setImmediate(message_0_8.isImmediate());
        deliveryProps.setDiscardUnroutable(!message_0_8.isMandatory());
        deliveryProps.setPriority(MessageDeliveryPriority.get(properties.getPriority()));
        deliveryProps.setRoutingKey(message_0_8.getInitialRoutingAddress());
        deliveryProps.setTimestamp(properties.getTimestamp());
        if (properties.getDeliveryMode() == BasicContentHeaderProperties.PERSISTENT)
        {
            deliveryProps.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        }
        else
        {
            deliveryProps.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
        }

        messageProps.setContentEncoding(properties.getEncodingAsString());
        messageProps.setContentLength(size);
        if(properties.getAppId() != null)
        {
            messageProps.setAppId(properties.getAppId().getBytes());
        }
        messageProps.setContentType(properties.getContentTypeAsString());
        if(properties.getCorrelationId() != null)
        {
            messageProps.setCorrelationId(properties.getCorrelationId().getBytes());
        }

        if(properties.getReplyTo() != null && properties.getReplyTo().length() != 0)
        {
            String origReplyToString = properties.getReplyTo().toString();
            ReplyTo replyTo = new ReplyTo();
            // if the string looks like a binding URL, then attempt to parse it...
            try
            {
                AMQBindingURL burl = new AMQBindingURL(origReplyToString);
                String routingKey = burl.getRoutingKey();
                if(routingKey != null)
                {
                    replyTo.setRoutingKey(routingKey);
                }

                String exchangeName = burl.getExchangeName();
                if(exchangeName != null && !"".equals(exchangeName))
                {
                    replyTo.setExchange(exchangeName);
                }
            }
            catch (URISyntaxException e)
            {
                replyTo.setRoutingKey(origReplyToString);
            }
            messageProps.setReplyTo(replyTo);

        }

        if(properties.getMessageId() != null)
        {
            UUID uuid;
            String messageIdAsString = properties.getMessageIdAsString();
            if(messageIdAsString.startsWith("ID:"))
            {
                messageIdAsString = messageIdAsString.substring(3);
            }

            try
            {
                uuid = UUID.fromString(messageIdAsString);
            }
            catch(IllegalArgumentException e)
            {
                uuid = UUID.nameUUIDFromBytes(messageIdAsString.getBytes(UTF_8));
            }
            messageProps.setMessageId(uuid);
        }



        if(properties.getUserId() != null)
        {
            messageProps.setUserId(properties.getUserId().getBytes());
        }

        final Map<String, Object> appHeaders = new LinkedHashMap<>(properties.getHeadersAsMap());

        if(properties.getType() != null)
        {
            appHeaders.put("x-jms-type", properties.getTypeAsString());
        }


        messageProps.setApplicationHeaders(appHeaders);

        Header header = new Header(deliveryProps, messageProps, null);


        return new MessageMetaData_0_10(header, size, message_0_8.getArrivalTime());
    }

    @Override
    public String getType()
    {
        return "v0-8 to v0-10";
    }
}
