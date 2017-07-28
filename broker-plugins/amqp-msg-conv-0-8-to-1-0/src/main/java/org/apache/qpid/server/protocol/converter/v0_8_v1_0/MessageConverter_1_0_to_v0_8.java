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
package org.apache.qpid.server.protocol.converter.v0_8_v1_0;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.StoredMessage;

@PluggableService
public class MessageConverter_1_0_to_v0_8 implements MessageConverter<Message_1_0, AMQMessage>
{
    private static final int BASIC_CLASS_ID = 60;


    @Override
    public Class<Message_1_0> getInputClass()
    {
        return Message_1_0.class;
    }

    @Override
    public Class<AMQMessage> getOutputClass()
    {
        return AMQMessage.class;
    }

    @Override
    public AMQMessage convert(Message_1_0 serverMsg, NamedAddressSpace addressSpace)
    {
        return new AMQMessage(convertToStoredMessage(serverMsg, addressSpace), null);
    }

    @Override
    public void dispose(final AMQMessage message)
    {

    }

    private StoredMessage<MessageMetaData> convertToStoredMessage(final Message_1_0 serverMsg,
                                                                  final NamedAddressSpace addressSpace)
    {
        Object bodyObject = MessageConverter_from_1_0.convertBodyToObject(serverMsg);

        final ObjectToMimeContentConverter converter = MimeContentConverterRegistry.getBestFitObjectToMimeContentConverter(bodyObject);

        final byte[] messageContent = converter == null ? new byte[] {} : converter.toMimeContent(bodyObject);
        final String mimeType = converter == null ? null  : converter.getMimeType();
        final MessageMetaData messageMetaData_0_8 = convertMetaData(serverMsg,
                                                                    mimeType,
                                                                    messageContent.length,
                                                                    addressSpace);
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
            public Collection<QpidByteBuffer> getContent(final int offset, final int length)
            {
                return Collections.singleton(QpidByteBuffer.wrap(messageContent, offset, length));
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
            public boolean isInMemory()
            {
                return true;
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

    private MessageMetaData convertMetaData(final Message_1_0 serverMsg,
                                            final String bodyMimeType,
                                            final int size,
                                            final NamedAddressSpace addressSpace)
    {


        final MessageMetaData_1_0.MessageHeader_1_0 header = serverMsg.getMessageHeader();
        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setAppId(serverMsg.getMessageHeader().getAppId());
        props.setContentType(bodyMimeType);
        props.setEncoding(convertToShortStringForProperty("content-encoding",
                                                          serverMsg.getMessageHeader().getEncoding()));
        props.setCorrelationId(getCorrelationId(serverMsg));
        props.setDeliveryMode(serverMsg.isPersistent() ? BasicContentHeaderProperties.PERSISTENT : BasicContentHeaderProperties.NON_PERSISTENT);


        final Date absoluteExpiryTime = getAbsoluteExpiryTime(serverMsg);
        if (absoluteExpiryTime != null)
        {
            props.setExpiration(absoluteExpiryTime.getTime());
        }
        else
        {
            Long ttl = getTtl(serverMsg);
            if (ttl != null)
            {
                props.setExpiration(ttl + serverMsg.getArrivalTime());
            }
        }

        props.setMessageId(getMessageId(serverMsg));
        props.setPriority(serverMsg.getMessageHeader().getPriority());
        props.setReplyTo(getReplyTo(serverMsg, addressSpace));
        final long timestamp = serverMsg.getMessageHeader().getTimestamp();
        if (timestamp > 0)
        {
            props.setTimestamp(timestamp);
        }
        else
        {
            props.setTimestamp(serverMsg.getArrivalTime());
        }
        props.setUserId(getUserId(serverMsg));

        Map<String,Object> headerProps = new LinkedHashMap<String, Object>();

        if(header.getSubject() != null)
        {
            headerProps.put("qpid.subject", header.getSubject());
            props.setType(convertToShortStringForProperty("subject", header.getSubject()));
        }

        String groupId = getGroupId(serverMsg);
        if (groupId != null)
        {
            headerProps.put("JMSXGroupID", groupId);
        }

        UnsignedInteger groupSequence = getGroupSequence(serverMsg);
        if (groupSequence != null)
        {
            headerProps.put("JMSXGroupSeq", groupSequence.intValue());
        }

        for (String headerName : serverMsg.getMessageHeader().getHeaderNames())
        {
            headerProps.put(headerName,
                            MessageConverter_from_1_0.convertValue(serverMsg.getMessageHeader().getHeader(headerName)));
        }

        final FieldTable headers;
        try
        {
            headers = FieldTable.convertToFieldTable(headerProps);
        }
        catch (IllegalArgumentException e)
        {
            throw new MessageConversionException(
                    "Could not convert message from 1.0 to 0-8 because conversion of 'application-properties' failed.",
                    e);
        }
        props.setHeaders(headers);

        final ContentHeaderBody chb = new ContentHeaderBody(props);
        chb.setBodySize(size);

        MessagePublishInfo publishInfo = createMessagePublishInfo(header, addressSpace);
        return new MessageMetaData(publishInfo, chb, serverMsg.getArrivalTime());
    }

    private MessagePublishInfo createMessagePublishInfo(final MessageMetaData_1_0.MessageHeader_1_0 header,
                                                        final NamedAddressSpace addressSpace)
    {
        final String to = header.getTo();
        final String subject = header.getSubject() == null ? "" : header.getSubject();

        final String exchangeName;
        final String routingKey;

        if (to != null)
        {
            if (to.startsWith("/"))
            {
                //TODO: get local address from global
                throw new MessageConversionException("Could not convert message from 1.0 to 0-8 because conversion of 'to' failed. Global addresses cannot be converted.");
            }

            int separatorPosition = to.indexOf('/');
            if (separatorPosition != -1)
            {
                exchangeName = to.substring(0, separatorPosition);
                routingKey = to.substring(separatorPosition + 1);
            }
            else
            {
                MessageDestination destination = addressSpace.getAttainedMessageDestination(to);
                if (destination instanceof Queue)
                {
                    exchangeName = "";
                    routingKey = to;
                }
                else
                {
                    exchangeName = to;
                    routingKey = subject;
                }
            }
        }
        else
        {
            exchangeName = "";
            routingKey = subject;
        }

        return new MessagePublishInfo(convertToShortStringForProperty("to", exchangeName),
                                                                false,
                                                                false,
                                                                convertToShortStringForProperty("to' or 'subject",
                                                                                                routingKey));
    }

    private UnsignedInteger getGroupSequence(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                return properties.getGroupSequence();
            }
        }
        return null;
    }

    private String getGroupId(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                return properties.getGroupId();
            }
        }
        return null;
    }

    private AMQShortString getUserId(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                Binary userId = properties.getUserId();
                if (userId != null)
                {
                    try
                    {
                        return new AMQShortString(userId.getArray());
                    }
                    catch (IllegalArgumentException e)
                    {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    private AMQShortString getMessageId(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                Object messageId = properties.getMessageId();
                try
                {
                    if (messageId instanceof Binary)
                    {
                        return new AMQShortString(((Binary) messageId).getArray());
                    }
                    else if (messageId instanceof byte[])
                    {
                        return new AMQShortString(((byte[]) messageId));
                    }
                    else
                    {
                        return AMQShortString.valueOf(messageId);
                    }
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }
            }
        }
        return null;

    }

    private Date getAbsoluteExpiryTime(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                return properties.getAbsoluteExpiryTime();
            }
        }
        return null;
    }

    private Long getTtl(final Message_1_0 serverMsg)
    {
        HeaderSection headerSection = serverMsg.getHeaderSection();
        if (headerSection != null)
        {
            Header header = headerSection.getValue();
            if (header != null)
            {
                UnsignedInteger ttl = header.getTtl();
                if (ttl != null)
                {
                    return ttl.longValue();
                }
            }
        }
        return null;
    }

    private AMQShortString getReplyTo(final Message_1_0 serverMsg, final NamedAddressSpace addressSpace)
    {
        // TODO : QPID-7602 - we probably need to look up the replyTo object and construct the correct BURL based on that
        final String replyTo = serverMsg.getMessageHeader().getReplyTo();
        try
        {
            return AMQShortString.valueOf(replyTo);
        }
        catch (IllegalArgumentException e)
        {
            throw new MessageConversionException("Could not convert message from 1.0 to 0-8 because conversion of 'reply-to' failed.", e);
        }
    }

    private AMQShortString getCorrelationId(final Message_1_0 serverMsg)
    {
        AMQShortString correlationId = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            if (properties != null)
            {
                final Object correlationIdObject = properties.getCorrelationId();
                try
                {
                    if (correlationIdObject instanceof Binary)
                    {
                        correlationId = new AMQShortString(((Binary) correlationIdObject).getArray());
                    }
                    else if (correlationIdObject instanceof byte[])
                    {
                        correlationId = new AMQShortString(((byte[]) correlationIdObject));
                    }
                    else
                    {
                        correlationId = AMQShortString.valueOf(correlationIdObject);
                    }
                }
                catch (IllegalArgumentException e)
                {
                    throw new MessageConversionException("Could not convert message from 1.0 to 0-8 because conversion of 'correlation-id' failed.", e);
                }
            }
        }
        return correlationId;
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
                    "Could not convert message from 1.0 to 0-8 because conversion of '%s' failed.", propertyName), e);
        }
    }

    @Override
    public String getType()
    {
        return "v1-0 to v0-8";
    }
}
