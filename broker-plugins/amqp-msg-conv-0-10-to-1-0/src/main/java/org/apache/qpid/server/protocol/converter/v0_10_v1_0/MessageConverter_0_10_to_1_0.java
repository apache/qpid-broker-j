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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
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
import org.apache.qpid.server.util.GZIPUtils;

@PluggableService
public class MessageConverter_0_10_to_1_0  extends MessageConverter_to_1_0<MessageTransferMessage>
{
    @Override
    public Class<MessageTransferMessage> getInputClass()
    {
        return MessageTransferMessage.class;
    }


    @Override
    protected MessageMetaData_1_0 convertMetaData(MessageTransferMessage serverMessage,
                                                  final EncodingRetainingSection<?> bodySection,
                                                  SectionEncoder sectionEncoder)
    {
        Properties props = new Properties();
        props.setCreationTime(new Date(serverMessage.getArrivalTime()));

        final MessageProperties msgProps = serverMessage.getHeader().getMessageProperties();
        final DeliveryProperties deliveryProps = serverMessage.getHeader().getDeliveryProperties();
        Header header = new Header();
        if(deliveryProps != null)
        {
            header.setDurable(deliveryProps.hasDeliveryMode()
                              && deliveryProps.getDeliveryMode() == MessageDeliveryMode.PERSISTENT);
            if (deliveryProps.hasPriority())
            {
                header.setPriority(UnsignedByte.valueOf((byte) deliveryProps.getPriority().getValue()));
            }
            if (deliveryProps.hasTtl())
            {
                header.setTtl(UnsignedInteger.valueOf(deliveryProps.getTtl()));
            }
            else if (deliveryProps.hasExpiration())
            {
                long ttl = Math.max(0, deliveryProps.getExpiration() - serverMessage.getArrivalTime());
                header.setTtl(UnsignedInteger.valueOf(ttl));
            }

            if(deliveryProps.hasTimestamp())
            {
                props.setCreationTime(new Date(deliveryProps.getTimestamp()));
            }

            String to = deliveryProps.getExchange();
            if (deliveryProps.getRoutingKey() != null)
            {
                String routingKey = deliveryProps.getRoutingKey();
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

        }


        ApplicationProperties applicationProperties = null;
        String originalContentMimeType = null;
        if(msgProps != null)
        {
            if(msgProps.hasContentEncoding()
               && !GZIPUtils.GZIP_CONTENT_ENCODING.equals(msgProps.getContentEncoding())
               && bodySection instanceof DataSection)
            {
                props.setContentEncoding(Symbol.valueOf(msgProps.getContentEncoding()));
            }

            if(msgProps.hasCorrelationId())
            {
                CharsetDecoder charsetDecoder = StandardCharsets.UTF_8.newDecoder()
                                                                      .onMalformedInput(CodingErrorAction.REPORT)
                                                                      .onUnmappableCharacter(CodingErrorAction.REPORT);
                try
                {
                    String correlationIdAsString = charsetDecoder.decode(ByteBuffer.wrap(msgProps.getCorrelationId())).toString();
                    props.setCorrelationId(correlationIdAsString);
                }
                catch (CharacterCodingException e)
                {
                    props.setCorrelationId(new Binary(msgProps.getCorrelationId()));
                }
            }

            if(msgProps.hasMessageId())
            {
                props.setMessageId(msgProps.getMessageId());
            }
            if(msgProps.hasReplyTo())
            {
                ReplyTo replyTo = msgProps.getReplyTo();
                String to = null;
                if (replyTo.hasExchange() && !"".equals(replyTo.getExchange()))
                {
                    to = replyTo.getExchange();
                }
                if (replyTo.hasRoutingKey())
                {
                    if (to != null)
                    {
                        to += "/" + replyTo.getRoutingKey();
                    }
                    else
                    {
                        to = replyTo.getRoutingKey();
                    }
                }
                props.setReplyTo(to);
            }

            if(msgProps.hasContentType())
            {
                originalContentMimeType = msgProps.getContentType();
                final Symbol contentType =
                        MessageConverter_to_1_0.getContentType(originalContentMimeType);
                props.setContentType(contentType);
            }

            if(msgProps.hasUserId())
            {
                props.setUserId(new Binary(msgProps.getUserId()));
            }

            Map<String, Object> applicationPropertiesMap = msgProps.getApplicationHeaders();
            if(applicationPropertiesMap != null)
            {
                applicationPropertiesMap = new LinkedHashMap<>(applicationPropertiesMap);
                if (applicationPropertiesMap.containsKey("x-jms-type"))
                {
                    props.setSubject(String.valueOf(applicationPropertiesMap.get("x-jms-type")));
                    applicationPropertiesMap.remove("x-jms-type");
                }

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

                try
                {
                    applicationProperties = new ApplicationProperties(applicationPropertiesMap);
                }
                catch (IllegalArgumentException e)
                {
                    throw new MessageConversionException("Could not convert message from 0-10 to 1.0 because application headers conversion failed.", e);
                }
            }
        }
        final MessageAnnotations messageAnnotation =
                MessageConverter_to_1_0.createMessageAnnotation(bodySection, originalContentMimeType);
        return new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                                       null,
                                       messageAnnotation == null ? null : messageAnnotation.createEncodingRetainingSection(),
                                       props.createEncodingRetainingSection(),
                                       applicationProperties == null ? null : applicationProperties.createEncodingRetainingSection(),
                                       null,
                                       serverMessage.getArrivalTime(),
                                       bodySection.getEncodedSize());
    }

    @Override
    public String getType()
    {
        return "v0-10 to v1-0";
    }
}
