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

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v1_0.MessageConverter_to_1_0;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.url.AMQBindingURL;
import org.apache.qpid.server.util.GZIPUtils;

@PluggableService
public class MessageConverter_0_8_to_1_0 extends MessageConverter_to_1_0<AMQMessage>
{
    @Override
    public Class<AMQMessage> getInputClass()
    {
        return AMQMessage.class;
    }

    @Override
    protected MessageMetaData_1_0 convertMetaData(final AMQMessage serverMessage,
                                                  final EncodingRetainingSection<?> bodySection,
                                                  SectionEncoder sectionEncoder)
    {
        Header header = new Header();
        Properties props = new Properties();
        header.setDurable(serverMessage.isPersistent());

        BasicContentHeaderProperties contentHeader = serverMessage.getContentHeaderBody().getProperties();

        header.setPriority(UnsignedByte.valueOf(contentHeader.getPriority()));

        if(contentHeader.hasExpiration())
        {
            final long expiration = serverMessage.getExpiration();
            final long arrivalTime = serverMessage.getArrivalTime();
            header.setTtl(UnsignedInteger.valueOf(Math.max(0, expiration - arrivalTime)));
        }

        if(!GZIPUtils.GZIP_CONTENT_ENCODING.equals(contentHeader.getEncodingAsString()) && bodySection instanceof DataSection)
        {
            props.setContentEncoding(Symbol.valueOf(contentHeader.getEncodingAsString()));
        }

        Symbol contentType = getContentType(contentHeader.getContentTypeAsString());
        props.setContentType(contentType);

        final AMQShortString correlationId = contentHeader.getCorrelationId();
        if(correlationId != null)
        {
            final byte[] correlationIdAsBytes = correlationId.getBytes();
            final String correlationIdAsString = contentHeader.getCorrelationIdAsString();
            if (Arrays.equals(correlationIdAsBytes, correlationIdAsString.getBytes(StandardCharsets.UTF_8)))
            {
                props.setCorrelationId(convertMessageId(correlationId));
            }
            else
            {
                props.setCorrelationId(correlationIdAsBytes);
            }
        }

        final AMQShortString messageId = contentHeader.getMessageId();
        if(messageId != null)
        {
            props.setMessageId(convertMessageId(messageId));
        }

        if (contentHeader.getReplyTo() != null)
        {
            props.setReplyTo(convertReplyTo(contentHeader.getReplyTo()));
        }

        if(contentHeader.getUserId() != null)
        {
            props.setUserId(new Binary(contentHeader.getUserId().getBytes()));
        }

        if (contentHeader.hasTimestamp())
        {
            props.setCreationTime(new Date(contentHeader.getTimestamp()));
        }
        else
        {
            props.setCreationTime(new Date(serverMessage.getArrivalTime()));
        }

        if (contentHeader.getType() != null)
        {
            props.setSubject(contentHeader.getType().toString());
        }

        Map<String, Object> applicationPropertiesMap = new LinkedHashMap<>(contentHeader.getHeadersAsMap());

        if(applicationPropertiesMap.containsKey("qpid.subject"))
        {
            props.setSubject(String.valueOf(applicationPropertiesMap.get("qpid.subject")));
            applicationPropertiesMap.remove("qpid.subject");
        }

        if(applicationPropertiesMap.containsKey("JMSXGroupID"))
        {
            props.setGroupId(String.valueOf(applicationPropertiesMap.get("JMSXGroupID")));
            applicationPropertiesMap.remove("JMSXGroupID");
        }

        if(applicationPropertiesMap.containsKey("JMSXGroupSeq"))
        {
            Object jmsxGroupSeq = applicationPropertiesMap.get("JMSXGroupSeq");
            if (jmsxGroupSeq instanceof Integer)
            {
                props.setGroupSequence(UnsignedInteger.valueOf((Integer)jmsxGroupSeq));
                applicationPropertiesMap.remove("JMSXGroupSeq");
            }
        }

        MessagePublishInfo messagePublishInfo = serverMessage.getMessagePublishInfo();
        String to = AMQShortString.toString(messagePublishInfo.getExchange());
        if (messagePublishInfo.getRoutingKey() != null)
        {
            String routingKey = AMQShortString.toString(messagePublishInfo.getRoutingKey());
            if (to != null && !"".equals(to))
            {
                to += "/" + routingKey;
            }
            else
            {
                to = routingKey;
            }
        }
        props.setTo(to);


        final ApplicationProperties applicationProperties;
        try
        {
            applicationProperties = new ApplicationProperties(applicationPropertiesMap);
        }
        catch (IllegalArgumentException e)
        {
            throw new MessageConversionException("Could not convert message from 0-8 to 1.0 because headers conversion failed.", e);
        }

        MessageAnnotations messageAnnotations = createMessageAnnotation(bodySection,
                                                                        contentHeader.getContentTypeAsString());

        return new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                                       null,
                                       messageAnnotations == null ? null : messageAnnotations.createEncodingRetainingSection(),
                                       props.createEncodingRetainingSection(),
                                       applicationProperties.createEncodingRetainingSection(),
                                       null,
                                       serverMessage.getArrivalTime(),
                                       bodySection.getEncodedSize());
    }

    private Object convertMessageId(final AMQShortString messageId)
    {
        try
        {
            String messageIdAsString = messageId.toString();
            if(messageIdAsString.startsWith("ID:"))
            {
                messageIdAsString = messageIdAsString.substring(3);
            }
            return UUID.fromString(messageIdAsString);
        }
        catch (IllegalArgumentException e)
        {
            return messageId.toString();
        }
    }

    private String convertReplyTo(final AMQShortString replyTo)
    {
        String convertedReplyTo;
        final String originalReplyTo = String.valueOf(replyTo);
        try
        {
            AMQBindingURL burl = new AMQBindingURL(originalReplyTo);

            if (burl.getExchangeName() != null && !burl.getExchangeName().equals(""))
            {
                convertedReplyTo = burl.getExchangeName();

                if (burl.getRoutingKey() != null)
                {
                    convertedReplyTo += "/" + burl.getRoutingKey();
                }
            }
            else if (burl.getQueueName() != null && !burl.getQueueName().equals(""))
            {
                convertedReplyTo = burl.getQueueName();
            }
            else if (burl.getRoutingKey() != null)
            {
                convertedReplyTo = burl.getRoutingKey();
            }
            else
            {
                convertedReplyTo = originalReplyTo;
            }
        }
        catch (URISyntaxException e)
        {
            convertedReplyTo = originalReplyTo;
        }
        return convertedReplyTo;
    }

    @Override
    public String getType()
    {
        return "v0-8 to v1-0";
    }
}
