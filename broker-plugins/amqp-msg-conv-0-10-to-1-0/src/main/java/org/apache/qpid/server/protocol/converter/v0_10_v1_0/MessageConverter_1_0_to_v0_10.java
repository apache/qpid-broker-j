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
package org.apache.qpid.server.protocol.converter.v0_10_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.convertValue;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getAbsoluteExpiryTime;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getAmqp0xConvertedContentAndMimeType;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getCorrelationId;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getCreationTime;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getGroupId;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getGroupSequence;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getMessageId;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getTtl;
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getUserId;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.ConvertedContentAndMimeType;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.store.StoredMessage;

@PluggableService
public class MessageConverter_1_0_to_v0_10 implements MessageConverter<Message_1_0, MessageTransferMessage>
{
    private static final int MAX_STR8_LENGTH = 0xFF;
    private static final int MAX_VBIN16_LENGTH = 0xFFFF;

    @Override
    public Class<Message_1_0> getInputClass()
    {
        return Message_1_0.class;
    }

    @Override
    public Class<MessageTransferMessage> getOutputClass()
    {
        return MessageTransferMessage.class;
    }

    @Override
    public MessageTransferMessage convert(Message_1_0 serverMsg, NamedAddressSpace addressSpace)
    {
        return new MessageTransferMessage(convertToStoredMessage(serverMsg, addressSpace), null);
    }

    @Override
    public void dispose(final MessageTransferMessage message)
    {

    }

    @Override
    public String getType()
    {
        return "v1-0 to v0-10";
    }

    private StoredMessage<MessageMetaData_0_10> convertToStoredMessage(final Message_1_0 serverMsg,
                                                                       final NamedAddressSpace addressSpace)
    {
        final ConvertedContentAndMimeType convertedContentAndMimeType = getAmqp0xConvertedContentAndMimeType(serverMsg);
        final byte[] convertedContent = convertedContentAndMimeType.getContent();
        final MessageMetaData_0_10 messageMetaData_0_10 = convertMetaData(serverMsg,
                                                                          addressSpace,
                                                                          convertedContentAndMimeType.getMimeType(),
                                                                          convertedContent.length);
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
                return QpidByteBuffer.wrap(convertedContent, offset, length);
            }

            @Override
            public int getContentSize()
            {
                return convertedContent.length;
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

    private MessageMetaData_0_10 convertMetaData(Message_1_0 serverMsg,
                                                 final NamedAddressSpace addressSpace,
                                                 final String bodyMimeType,
                                                 final int size)
    {
        DeliveryProperties deliveryProps = new DeliveryProperties();
        MessageProperties messageProps = new MessageProperties();

        final MessageMetaData_1_0.MessageHeader_1_0 origHeader = serverMsg.getMessageHeader();

        setExchangeAndRoutingKeyOnDeliveryProperties(deliveryProps, origHeader, addressSpace);

        deliveryProps.setDeliveryMode(serverMsg.isPersistent()
                                              ? MessageDeliveryMode.PERSISTENT
                                              : MessageDeliveryMode.NON_PERSISTENT);

        deliveryProps.setExpiration(serverMsg.getExpiration());
        deliveryProps.setPriority(MessageDeliveryPriority.get(origHeader.getPriority()));

        Date creationTime = getCreationTime(serverMsg);
        final long arrivalTime = serverMsg.getArrivalTime();
        if (creationTime != null)
        {
            deliveryProps.setTimestamp(creationTime.getTime());
        }
        else
        {
            deliveryProps.setTimestamp(arrivalTime);
        }

        Long ttl = getTtl(serverMsg);
        Date absoluteExpiryTime = getAbsoluteExpiryTime(serverMsg);
        if (ttl != null)
        {
            deliveryProps.setTtl(ttl);
            deliveryProps.setExpiration(arrivalTime + ttl);
        }
        else if (absoluteExpiryTime != null)
        {
            final long time = absoluteExpiryTime.getTime();
            deliveryProps.setTtl(Math.max(0, time - arrivalTime));
            deliveryProps.setExpiration(time);
        }

        UUID messageId = getMessageIdAsUUID(serverMsg);
        if (messageId != null)
        {
            messageProps.setMessageId(messageId);
        }

        byte[] correlationId = getCorrelationIdAsBytes(serverMsg);
        if (correlationId != null)
        {
            messageProps.setCorrelationId(correlationId);
        }

        final String encoding = origHeader.getEncoding();
        if (encoding != null)
        {
            messageProps.setContentEncoding(ensureStr8("content-encoding", encoding));
        }

        messageProps.setContentLength(size);
        messageProps.setContentType(bodyMimeType);
        byte[] userId = getUserIdAsBytes(serverMsg);
        if (userId != null && userId.length <= MAX_VBIN16_LENGTH)
        {
            messageProps.setUserId(ensureVBin16("user-id", userId));
        }

        final String origReplyTo = origHeader.getReplyTo();
        if (origReplyTo != null && !origReplyTo.equals(""))
        {
            messageProps.setReplyTo(getReplyTo(addressSpace, origReplyTo));
        }

        Map<String, Object> appHeaders = getApplicationHeaders(serverMsg);

        messageProps.setApplicationHeaders(appHeaders);

        Header header = new Header(deliveryProps, messageProps, null);
        return new MessageMetaData_0_10(header, size, arrivalTime);
    }

    private Map<String, Object> getApplicationHeaders(final Message_1_0 serverMsg)
    {
        final MessageMetaData_1_0.MessageHeader_1_0 origHeader = serverMsg.getMessageHeader();
        final Map<String, Object> applicationProperties = serverMsg.getMessageHeader().getHeadersAsMap();
        for(String key: applicationProperties.keySet())
        {
            ensureStr8("application-properties[\"" + key + "\"]", key);
        }

        Map<String, Object> appHeaders =
                new LinkedHashMap((Map<String, Object>) convertValue(applicationProperties));
        if (origHeader.getSubject() != null)
        {
            if (!appHeaders.containsKey("qpid.subject"))
            {
                appHeaders.put("qpid.subject", origHeader.getSubject());
            }

            if (!appHeaders.containsKey("x-jms-type"))
            {
                appHeaders.put("x-jms-type", origHeader.getSubject());
            }
        }

        String groupId = getGroupId(serverMsg);
        if (groupId != null && !appHeaders.containsKey("JMSXGroupID"))
        {
            appHeaders.put("JMSXGroupID", groupId);
        }

        UnsignedInteger groupSequence = getGroupSequence(serverMsg);
        if (groupSequence != null && !appHeaders.containsKey("JMSXGroupSeq"))
        {
            appHeaders.put("JMSXGroupSeq", groupSequence.intValue());
        }

        return appHeaders;
    }

    private ReplyTo getReplyTo(final NamedAddressSpace addressSpace, final String origReplyTo)
    {
        DestinationAddress destinationAddress = new DestinationAddress(addressSpace, origReplyTo);
        MessageDestination messageDestination = destinationAddress.getMessageDestination();
        return new ReplyTo(ensureStr8("reply-to[\"exchange\"]",
                                      messageDestination instanceof Exchange ? messageDestination.getName() : ""),
                           ensureStr8("reply-to[\"routing-key\"]",
                                      messageDestination instanceof Queue
                                              ? messageDestination.getName()
                                              : destinationAddress.getRoutingKey()));
    }

    private void setExchangeAndRoutingKeyOnDeliveryProperties(final DeliveryProperties deliveryProps,
                                                              final MessageMetaData_1_0.MessageHeader_1_0 origHeader,
                                                              final NamedAddressSpace addressSpace)
    {
        final String to = origHeader.getTo();
        final String subject = origHeader.getSubject() == null ? "" : origHeader.getSubject();

        final String exchangeName;
        final String routingKey;

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
                routingKey = "".equals(destinationAddress.getRoutingKey()) ? subject : destinationAddress.getRoutingKey();
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
            routingKey = subject;
        }

        deliveryProps.setRoutingKey(ensureStr8("to' or 'subject", routingKey));
        deliveryProps.setExchange(ensureStr8("to", exchangeName));
    }

    private byte[] getUserIdAsBytes(final Message_1_0 serverMsg)
    {
        Binary userId = getUserId(serverMsg);
        if (userId != null)
        {
            return userId.getArray();
        }
        return null;
    }

    private UUID getMessageIdAsUUID(final Message_1_0 serverMsg)
    {
        Object messageId = getMessageId(serverMsg);
        if (messageId == null)
        {
            return null;
        }
        else if (messageId instanceof UUID)
        {
            return (UUID)messageId;
        }
        else if (messageId instanceof String)
        {
            String messageIdString = (String) messageId;
            try
            {
                if (messageIdString.startsWith("ID:"))
                {
                    messageIdString = messageIdString.substring(3);
                }
                return UUID.fromString(messageIdString);
            }
            catch (IllegalArgumentException e)
            {
                return UUID.nameUUIDFromBytes(messageIdString.getBytes(UTF_8));
            }
        }
        else if (messageId instanceof Binary)
        {
            return UUID.nameUUIDFromBytes(((Binary) messageId).getArray());
        }
        else if (messageId instanceof byte[])
        {
            return UUID.nameUUIDFromBytes((byte[]) messageId);
        }
        else if (messageId instanceof UnsignedLong)
        {
            return UUID.nameUUIDFromBytes(longToBytes(((UnsignedLong) messageId).longValue()));
        }
        else
        {
            return UUID.nameUUIDFromBytes(String.valueOf(messageId).getBytes(UTF_8));
        }
    }

    private byte[] getCorrelationIdAsBytes(final Message_1_0 serverMsg)
    {
        final Object correlationIdObject = getCorrelationId(serverMsg);
        final byte[] correlationId;
        if (correlationIdObject == null)
        {
            correlationId = null;
        }
        else if (correlationIdObject instanceof Binary)
        {
            correlationId = ((Binary) correlationIdObject).getArray();
        }
        else if (correlationIdObject instanceof byte[])
        {
            correlationId = (byte[]) correlationIdObject;
        }
        else if (correlationIdObject instanceof UUID)
        {
            UUID uuid = (UUID)correlationIdObject;
            correlationId = longToBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        }
        else if (correlationIdObject instanceof UnsignedLong)
        {
            correlationId = longToBytes(((UnsignedLong) correlationIdObject).longValue());
        }
        else
        {
            correlationId = String.valueOf(correlationIdObject).getBytes(UTF_8);
        }
        return ensureVBin16("correlation-id", correlationId);
    }

    private String ensureStr8(final String propertyName, String string)
    {
        if (string != null && string.length() > MAX_STR8_LENGTH)
        {
            throw new MessageConversionException(String.format(
                    "Could not convert message from 1.0 to 0-10 because conversion of '%s' failed."
                    + " The string length exceeds allowed maximum.",
                    propertyName));
        }
        return string;
    }

    private byte[] ensureVBin16(final String propertyName, final byte[] result)
    {
        if (result != null && result.length > MAX_VBIN16_LENGTH)
        {
            throw new MessageConversionException(String.format(
                    "Could not convert message from 1.0 to 0-10 because conversion of '%s' failed."
                    + " The array length exceeds allowed maximum.",
                    propertyName));
        }
        return result;
    }

    private byte[] longToBytes(long... x)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * x.length);
        for (long l : x)
        {
            buffer.putLong(l);
        }
        return buffer.array();
    }
}
