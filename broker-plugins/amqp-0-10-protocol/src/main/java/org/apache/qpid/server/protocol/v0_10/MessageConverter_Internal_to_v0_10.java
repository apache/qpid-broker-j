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
package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.EncoderUtils;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.store.StoredMessage;

@PluggableService
public class MessageConverter_Internal_to_v0_10 implements MessageConverter<InternalMessage, MessageTransferMessage>
{
    private static final int MAX_VBIN16_LENGTH = 0xFFFF;
    private static final int MAX_STR8_LENGTH = 0xFF;

    @Override
    public Class<InternalMessage> getInputClass()
    {
        return InternalMessage.class;
    }

    @Override
    public Class<MessageTransferMessage> getOutputClass()
    {
        return MessageTransferMessage.class;
    }

    @Override
    public MessageTransferMessage convert(InternalMessage serverMsg, NamedAddressSpace addressSpace)
    {
        return new MessageTransferMessage(convertToStoredMessage(serverMsg, addressSpace), null);
    }

    @Override
    public void dispose(final MessageTransferMessage message)
    {

    }

    private StoredMessage<MessageMetaData_0_10> convertToStoredMessage(final InternalMessage serverMsg,
                                                                       final NamedAddressSpace addressSpace)
    {
        Object messageBody = serverMsg.getMessageBody();
        ObjectToMimeContentConverter converter = MimeContentConverterRegistry.getBestFitObjectToMimeContentConverter(messageBody);
        final byte[] messageContent = converter == null ? new byte[] {} : converter.toMimeContent(messageBody);
        String mimeType = converter == null ? null  : converter.getMimeType();

        mimeType = improveMimeType(serverMsg, mimeType);

        final MessageMetaData_0_10 messageMetaData_0_10 =
                convertMetaData(serverMsg, addressSpace, mimeType, messageContent.length);
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
            if ("text/plain".equals(mimeType) &&
                serverMsg.getMessageHeader().getMimeType().startsWith("text/"))
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

    private MessageMetaData_0_10 convertMetaData(final InternalMessage serverMsg,
                                                 final NamedAddressSpace addressSpace,
                                                 final String bodyMimeType,
                                                 final int size)
    {
        DeliveryProperties deliveryProps = new DeliveryProperties();
        MessageProperties messageProps = new MessageProperties();

        deliveryProps.setDeliveryMode(serverMsg.isPersistent()
                                              ? MessageDeliveryMode.PERSISTENT
                                              : MessageDeliveryMode.NON_PERSISTENT);
        long expiration = serverMsg.getExpiration();
        if (expiration > 0)
        {
            deliveryProps.setExpiration(expiration);
            deliveryProps.setTtl(Math.max(0, expiration - serverMsg.getArrivalTime()));
        }
        InternalMessageHeader messageHeader = serverMsg.getMessageHeader();
        deliveryProps.setPriority(MessageDeliveryPriority.get(messageHeader.getPriority()));
        convertToAndInitialRoutingKey(serverMsg, deliveryProps, addressSpace);
        deliveryProps.setTimestamp(messageHeader.getTimestamp());

        messageProps.setContentEncoding(ensureStr8("content-encoding", messageHeader.getEncoding()));
        messageProps.setContentLength(size);
        messageProps.setContentType(bodyMimeType);
        if (messageHeader.getCorrelationId() != null)
        {
            messageProps.setCorrelationId(ensureVBin16("correlation-id", messageHeader
                    .getCorrelationId().getBytes(UTF_8)));
        }

        validateValue(messageHeader.getHeaderMap(), "application-headers");

        messageProps.setApplicationHeaders(messageHeader.getHeaderMap());
        String messageIdAsString = messageHeader.getMessageId();
        if (messageIdAsString != null)
        {
            try
            {
                if (messageIdAsString.startsWith("ID:"))
                {
                    messageIdAsString = messageIdAsString.substring(3);
                }
                messageProps.setMessageId(UUID.fromString(messageIdAsString));
            }
            catch (IllegalArgumentException iae)
            {
                // ignore message id is not a UUID
            }
        }
        String userId = messageHeader.getUserId();
        if (userId != null)
        {
            byte[] bytes = userId.getBytes(UTF_8);
            if (bytes.length <= MAX_VBIN16_LENGTH)
            {
                messageProps.setUserId(bytes);
            }
        }

        final String origReplyTo = messageHeader.getReplyTo();
        if (origReplyTo != null && !origReplyTo.equals(""))
        {
            messageProps.setReplyTo(getReplyTo(addressSpace, origReplyTo));
        }

        Header header = new Header(deliveryProps, messageProps, null);
        return new MessageMetaData_0_10(header, size, serverMsg.getArrivalTime());
    }

    private void convertToAndInitialRoutingKey(final InternalMessage serverMsg,
                                               final DeliveryProperties deliveryProps,
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

        deliveryProps.setRoutingKey(ensureStr8("to' or 'initialRoutingAddress", routingKey));
        deliveryProps.setExchange(ensureStr8("to' or 'initialRoutingAddress", exchangeName));
    }

    private ReplyTo getReplyTo(final NamedAddressSpace addressSpace, final String origReplyTo)
    {
        DestinationAddress destinationAddress = new DestinationAddress(addressSpace, origReplyTo);
        MessageDestination messageDestination = destinationAddress.getMessageDestination();
        final String exchange = ensureStr8("reply-to[\"exchange\"]", messageDestination instanceof Exchange
                ? messageDestination.getName() : "");
        final String routingKey = ensureStr8("reply-to[\"routing-key\"]", messageDestination instanceof Queue
                ? messageDestination.getName() : destinationAddress.getRoutingKey());
        return new ReplyTo(exchange, routingKey);
    }

    private void validateValue(final Object value, final String path)
    {
        if (!EncoderUtils.isEncodable(value))
        {
            throw new MessageConversionException(String.format(
                    "Could not convert message from internal to 0-10 because conversion of %s failed. Unsupported type is used.", path));
        }
    }


    @Override
    public String getType()
    {
        return "Internal to v0-10";
    }

    private byte[] ensureVBin16(final String propertyName, final byte[] result)
    {
        if (result != null && result.length > MAX_VBIN16_LENGTH)
        {
            throw new MessageConversionException(String.format(
                    "Could not convert message from internal to 0-10 because conversion of '%s' failed."
                    + " The array length exceeds allowed maximum.",
                    propertyName));
        }
        return result;
    }

    private String ensureStr8(final String propertyName, String string)
    {
        if (string != null && string.length() > MAX_STR8_LENGTH)
        {
            throw new MessageConversionException(String.format(
                    "Could not convert message from internal to 0-10 because conversion of '%s' failed."
                    + " The string length exceeds allowed maximum.",
                    propertyName));
        }
        return string;
    }

}
