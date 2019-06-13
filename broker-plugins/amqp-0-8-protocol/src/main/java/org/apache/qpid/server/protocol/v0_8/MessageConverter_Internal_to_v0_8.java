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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;

@PluggableService
public class MessageConverter_Internal_to_v0_8 implements MessageConverter<InternalMessage, AMQMessage>
{


    @Override
    public Class<InternalMessage> getInputClass()
    {
        return InternalMessage.class;
    }

    @Override
    public Class<AMQMessage> getOutputClass()
    {
        return AMQMessage.class;
    }

    @Override
    public AMQMessage convert(InternalMessage serverMsg, NamedAddressSpace addressSpace)
    {
        return new AMQMessage(convertToStoredMessage(serverMsg, addressSpace), null);
    }

    @Override
    public void dispose(final AMQMessage message)
    {

    }

    private StoredMessage<MessageMetaData> convertToStoredMessage(final InternalMessage serverMsg,
                                                                  final NamedAddressSpace addressSpace)
    {
        Object messageBody = serverMsg.getMessageBody();
        ObjectToMimeContentConverter converter = MimeContentConverterRegistry.getBestFitObjectToMimeContentConverter(messageBody);
        final byte[] messageContent;
        try
        {
            messageContent = converter == null ? new byte[] {} : converter.toMimeContent(messageBody);
        }
        catch (IllegalArgumentException e)
        {
            throw new MessageConversionException("Could not convert message from Internal to 0-8 because"
                                                 + " conversion of message content failed.", e);
        }
        String mimeType = converter == null ? null  : converter.getMimeType();

        mimeType = improveMimeType(serverMsg, mimeType);

        final MessageMetaData messageMetaData_0_8 =
                convertMetaData(serverMsg, addressSpace, mimeType, messageContent.length);
        final int metadataSize = messageMetaData_0_8.getStorableSize();

        return new StoredMessage<MessageMetaData>()
        {
            @Override
            public MessageMetaData getMetaData()
            {
                return messageMetaData_0_8;
            }

            @Override
            public long getMessageNumber()
            {
                return serverMsg.getMessageNumber();
            }

            @Override
            public QpidByteBuffer getContent(final int offset, final int length)
            {
                return QpidByteBuffer.wrap(messageContent, offset, length);
            }

            @Override
            public int getContentSize()
            {
                return messageContent.length;
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

    private String improveMimeType(final InternalMessage serverMsg, String mimeType)
    {
        if (serverMsg.getMessageHeader() != null && serverMsg.getMessageHeader().getMimeType() != null)
        {
            if ("text/plain".equals(mimeType) && serverMsg.getMessageHeader().getMimeType().startsWith("text/"))
            {
                mimeType = serverMsg.getMessageHeader().getMimeType();
            }
            else if ("application/octet-stream".equals(mimeType))
            {
                mimeType = serverMsg.getMessageHeader().getMimeType();
            }
        }
        return mimeType;
    }

    private MessageMetaData convertMetaData(final InternalMessage serverMsg,
                                            final NamedAddressSpace addressSpace,
                                            final String bodyMimeType,
                                            final int size)
    {

        MessagePublishInfo publishInfo = createMessagePublishInfo(serverMsg, addressSpace);

        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setAppId(serverMsg.getMessageHeader().getAppId());
        props.setContentType(bodyMimeType);
        props.setCorrelationId(convertToShortStringForProperty("correlation-id", serverMsg.getMessageHeader().getCorrelationId()));
        props.setDeliveryMode(serverMsg.isPersistent() ? BasicContentHeaderProperties.PERSISTENT : BasicContentHeaderProperties.NON_PERSISTENT);
        props.setExpiration(serverMsg.getExpiration());
        props.setMessageId(convertToOptionalAMQPShortString(serverMsg.getMessageHeader().getMessageId()));
        props.setPriority(serverMsg.getMessageHeader().getPriority());
        props.setReplyTo(convertToShortStringForProperty("reply-to", getReplyTo(serverMsg, addressSpace)));
        props.setTimestamp(serverMsg.getMessageHeader().getTimestamp());

        props.setUserId(convertToOptionalAMQPShortString(serverMsg.getMessageHeader().getUserId()));

        props.setEncoding(convertToShortStringForProperty("encoding", serverMsg.getMessageHeader().getEncoding()));

        Map<String,Object> headerProps = new LinkedHashMap<String, Object>();

        for(String headerName : serverMsg.getMessageHeader().getHeaderNames())
        {
            headerProps.put(headerName, serverMsg.getMessageHeader().getHeader(headerName));
        }

        try
        {
            props.setHeaders(FieldTable.convertToFieldTable(headerProps));
        }
        catch (IllegalArgumentException | AMQPInvalidClassException e)
        {
            throw new MessageConversionException("Could not convert message from internal to 0-8 because headers conversion failed.", e);
        }

        final ContentHeaderBody chb = new ContentHeaderBody(props);
        chb.setBodySize(size);
        return new MessageMetaData(publishInfo, chb, serverMsg.getArrivalTime());
    }

    private MessagePublishInfo createMessagePublishInfo(final InternalMessage serverMsg,
                                                        final NamedAddressSpace addressSpace)
    {
        String to = serverMsg.getTo();

        final String exchangeName;
        final String routingKey;
        if (to == null || "".equals(to))
        {
            to = serverMsg.getInitialRoutingAddress();
        }

        if (to != null && !"".equals(to))
        {
            DestinationAddress destinationAddress = new DestinationAddress(addressSpace, to);
            MessageDestination messageDestination = destinationAddress.getMessageDestination();
            if (messageDestination instanceof Queue)
            {
                exchangeName = "";
                routingKey = messageDestination.getName();
            }
            else if (messageDestination instanceof Exchange)
            {
                exchangeName = messageDestination.getName();
                routingKey = destinationAddress.getRoutingKey();
            }
            else
            {
                exchangeName = "";
                routingKey = to;
            }
        }
        else
        {
            exchangeName = "";
            routingKey = "";
        }

        return new MessagePublishInfo(convertToShortStringForProperty("to", exchangeName),
                                      false,
                                      false,
                                      convertToShortStringForProperty("to' or 'subject",
                                                                      routingKey));
    }

    private String getReplyTo(final InternalMessage serverMsg, final NamedAddressSpace addressSpace)
    {
        String replyTo = serverMsg.getMessageHeader().getReplyTo();

        if (replyTo != null)
        {
            DestinationAddress destinationAddress = new DestinationAddress(addressSpace, replyTo);
            MessageDestination messageDestination = destinationAddress.getMessageDestination();

            final String replyToBindingUrl;
            if (messageDestination instanceof Exchange)
            {
                Exchange<?> exchange = (Exchange<?>) messageDestination;

                final String routingKeyOption = "".equals(destinationAddress.getRoutingKey())
                        ? ""
                        : String.format("?routingkey='%s'", destinationAddress.getRoutingKey());
                replyToBindingUrl = String.format("%s://%s//%s",
                                                  exchange.getType(),
                                                  exchange.getName(),
                                                  routingKeyOption);
            }
            else if (messageDestination instanceof Queue)
            {
                replyToBindingUrl = String.format("%s:////%s",
                                                  ExchangeDefaults.DIRECT_EXCHANGE_CLASS,
                                                  messageDestination.getName());
            }
            else
            {
                replyToBindingUrl = String.format("%s:////?routingkey='%s'",
                                                  ExchangeDefaults.DIRECT_EXCHANGE_CLASS,
                                                  destinationAddress.getRoutingKey());
            }

            return replyToBindingUrl;
        }
        return null;
    }

    private AMQShortString convertToOptionalAMQPShortString(final String stringValue)
    {
        AMQShortString result;
        try
        {
            result = AMQShortString.valueOf(stringValue);
        }
        catch (IllegalArgumentException e)
        {
            result = null;
        }
        return result;
    }

    private AMQShortString convertToShortStringForProperty(String propertyName, String s)
    {
        try
        {
            return AMQShortString.valueOf(s);
        }
        catch (IllegalArgumentException e)
        {
            throw new MessageConversionException(String.format(
                    "Could not convert message from internal to 0-8 because conversion of '%s' failed.", propertyName), e);
        }
    }


    @Override
    public String getType()
    {
        return "Internal to v0-8";
    }
}
