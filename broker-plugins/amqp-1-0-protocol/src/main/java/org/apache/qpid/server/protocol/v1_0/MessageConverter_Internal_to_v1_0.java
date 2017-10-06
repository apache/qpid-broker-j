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
package org.apache.qpid.server.protocol.v1_0;

import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.BYTES_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.MAP_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.OBJECT_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.STREAM_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.TEXT_MESSAGE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;

import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequence;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NonEncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

@PluggableService
public class MessageConverter_Internal_to_v1_0 extends MessageConverter_to_1_0<InternalMessage>
{

    private static final Set<Class<?>> TYPES_EXPRESSIBLE_AS_AMQP_1_0_VALUE = Sets.newHashSet(String.class,
                                                                                             Character.class,
                                                                                             Boolean.class,
                                                                                             Number.class,
                                                                                             UUID.class,
                                                                                             Date.class);

    @Override
    public Class<InternalMessage> getInputClass()
    {
        return InternalMessage.class;
    }


    @Override
    protected MessageMetaData_1_0 convertMetaData(final InternalMessage serverMessage,
                                                  final EncodingRetainingSection<?> bodySection,
                                                  final SectionEncoder sectionEncoder)
    {
        Header header = new Header();

        header.setDurable(serverMessage.isPersistent());
        header.setPriority(UnsignedByte.valueOf(serverMessage.getMessageHeader().getPriority()));
        if(serverMessage.getExpiration() != 0l && serverMessage.getArrivalTime() !=0l && serverMessage.getExpiration() >= serverMessage.getArrivalTime())
        {
            header.setTtl(UnsignedInteger.valueOf(serverMessage.getExpiration()-serverMessage.getArrivalTime()));
        }

        Properties properties = new Properties();
        if (serverMessage.getMessageHeader().getEncoding() != null)
        {
            properties.setContentEncoding(Symbol.valueOf(serverMessage.getMessageHeader().getEncoding()));
        }
        properties.setCorrelationId(getCorrelationId(serverMessage));
        properties.setCreationTime(new Date(serverMessage.getMessageHeader().getTimestamp()));
        properties.setMessageId(getMessageId(serverMessage));
        Symbol contentType = getContentTypeSymbol(serverMessage.getMessageBody(), serverMessage.getMessageHeader().getMimeType());
        properties.setContentType(contentType);

        final String userId = serverMessage.getMessageHeader().getUserId();
        if(userId != null)
        {
            properties.setUserId(new Binary(userId.getBytes(StandardCharsets.UTF_8)));
        }
        properties.setReplyTo(serverMessage.getMessageHeader().getReplyTo());
        properties.setTo(serverMessage.getTo());

        ApplicationProperties applicationProperties = null;
        if(!serverMessage.getMessageHeader().getHeaderNames().isEmpty())
        {
            try
            {
                applicationProperties = new ApplicationProperties(serverMessage.getMessageHeader().getHeaderMap());
            }
            catch (IllegalArgumentException e)
            {
                throw new MessageConversionException("Could not convert message from internal to 1.0"
                                                     + " because conversion of 'application headers' failed.", e);
            }
        }

        final MessageAnnotations messageAnnotation = createMessageAnnotation(serverMessage.getMessageBody(),
                                                                             serverMessage.getMessageHeader()
                                                                                          .getMimeType(),
                                                                             bodySection);

        return new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                                       null,
                                       messageAnnotation == null ? null : messageAnnotation.createEncodingRetainingSection(),
                                       properties.createEncodingRetainingSection(),
                                       applicationProperties == null ? null : applicationProperties.createEncodingRetainingSection(),
                                       null,
                                       serverMessage.getArrivalTime(),
                                       bodySection.getEncodedSize());

    }

    private MessageAnnotations createMessageAnnotation(final Object originalMessageBody,
                                                       String mimeType,
                                                       final EncodingRetainingSection<?> convertedMessageBody)
    {
        final Byte contentTypeAnnotationValue;
        if (originalMessageBody instanceof String)
        {
            contentTypeAnnotationValue = TEXT_MESSAGE.getType();
        }
        else if (originalMessageBody instanceof List)
        {
            contentTypeAnnotationValue = isSectionValidForJmsList(convertedMessageBody) ? STREAM_MESSAGE.getType() : null;
        }
        else if (originalMessageBody instanceof byte[])
        {
            contentTypeAnnotationValue = BYTES_MESSAGE.getType();
        }
        else if (originalMessageBody instanceof Map)
        {
            contentTypeAnnotationValue = isSectionValidForJmsMap(convertedMessageBody) ? MAP_MESSAGE.getType() : null;
        }
        else if (originalMessageBody != null
                 && TYPES_EXPRESSIBLE_AS_AMQP_1_0_VALUE.stream().anyMatch(clazz -> clazz.isAssignableFrom(originalMessageBody.getClass())))
        {
            contentTypeAnnotationValue = null;
        }
        else if (originalMessageBody instanceof Serializable)
        {
            contentTypeAnnotationValue = OBJECT_MESSAGE.getType();
        }
        else if (originalMessageBody == null && mimeType == null)
        {
            contentTypeAnnotationValue = MESSAGE.getType();
        }
        else
        {
            contentTypeAnnotationValue = null;
        }

        if (contentTypeAnnotationValue != null)
        {
            return new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                   contentTypeAnnotationValue));
        }
        else
        {
            return null;
        }
    }

    private Symbol getContentTypeSymbol(final Object messageBody, final String mimeType)
    {
        String contentTypeAsString;
        if (messageBody instanceof String)
        {
            contentTypeAsString = mimeType == null ? "text/plain" : mimeType;
        }
        else if (messageBody instanceof List)
        {
            contentTypeAsString = null;
        }
        else if (messageBody instanceof byte[])
        {
            contentTypeAsString = mimeType == null ? "application/octet-stream" : mimeType;
        }
        else if (messageBody instanceof Map)
        {
            contentTypeAsString = null;
        }
        else if (messageBody != null
                 && TYPES_EXPRESSIBLE_AS_AMQP_1_0_VALUE.stream().anyMatch(clazz -> clazz.isAssignableFrom(messageBody.getClass())))
        {
            contentTypeAsString = mimeType;
        }
        else if (messageBody instanceof Serializable)
        {
            contentTypeAsString = "application/x-java-serialized-object";
        }
        else
        {
            contentTypeAsString = mimeType;
        }
        return Symbol.valueOf(contentTypeAsString);
    }

    private Object getMessageId(final InternalMessage serverMessage)
    {
        String messageIdAsString = serverMessage.getMessageHeader().getMessageId();
        return stringToMessageId(messageIdAsString);
    }

    private Object getCorrelationId(final InternalMessage serverMessage)
    {
        String correlationIdAsString = serverMessage.getMessageHeader().getCorrelationId();
        return stringToMessageId(correlationIdAsString);
    }

    private Object stringToMessageId(final String correlationIdAsString)
    {
        Object messageId = null;
        if (correlationIdAsString != null)
        {
            try
            {
                messageId = UUID.fromString(correlationIdAsString);
            }
            catch (IllegalArgumentException e)
            {
                try
                {
                    messageId = UnsignedLong.valueOf(correlationIdAsString);
                }
                catch (NumberFormatException nfe)
                {
                    messageId = correlationIdAsString;
                }
            }
        }
        return messageId;
    }

    @Override
    protected EncodingRetainingSection<?> getBodySection(final InternalMessage serverMessage,
                                                         final SectionEncoder encoder)
    {
        return convertToBody(serverMessage.getMessageBody()).createEncodingRetainingSection();
    }


    @Override
    public String getType()
    {
        return "Internal to v1-0";
    }


    public NonEncodingRetainingSection<?> convertToBody(Object object)
    {
        if (object == null
            || TYPES_EXPRESSIBLE_AS_AMQP_1_0_VALUE.stream().anyMatch(clazz -> clazz.isAssignableFrom(object.getClass())))
        {
            return new AmqpValue(object);
        }
        else if (object instanceof byte[])
        {
            return new Data(new Binary((byte[]) object));
        }
        else if (object instanceof Map)
        {
            return new AmqpValue(MessageConverter_to_1_0.fixMapValues((Map<String,Object>) object));
        }
        else if (object instanceof List)
        {
            return new AmqpSequence(MessageConverter_to_1_0.fixListValues((List<Object>) object));
        }
        else
        {
            try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                 ObjectOutputStream os = new ObjectOutputStream(bytesOut))
            {
                os.writeObject(object);
                return new Data(new Binary(bytesOut.toByteArray()));
            }
            catch (IOException e)
            {
                throw new ConnectionScopedRuntimeException(e);
            }
        }
    }

}
