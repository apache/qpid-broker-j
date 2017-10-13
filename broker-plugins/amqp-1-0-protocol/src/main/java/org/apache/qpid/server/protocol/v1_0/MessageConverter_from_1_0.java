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

import static org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry.getBestFitObjectToMimeContentConverter;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.mimecontentconverter.ConversionUtils;
import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequenceSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.codec.EncodingRetaining;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class MessageConverter_from_1_0
{

    private static final Set<Class> STANDARD_TYPES = new HashSet<>(Arrays.<Class>asList(Boolean.class,
                                                                                        Byte.class,
                                                                                        Short.class,
                                                                                        Integer.class,
                                                                                        Long.class,
                                                                                        Float.class,
                                                                                        Double.class,
                                                                                        Character.class,
                                                                                        String.class,
                                                                                        byte[].class,
                                                                                        UUID.class,
                                                                                        Date.class));

    static Object convertBodyToObject(final Message_1_0 serverMessage)
    {
        SectionDecoderImpl sectionDecoder = new SectionDecoderImpl(MessageConverter_v1_0_to_Internal.TYPE_REGISTRY.getSectionDecoderRegistry());

        Object bodyObject = null;
        List<EncodingRetainingSection<?>> sections = null;
        try
        {
            try (QpidByteBuffer allData = serverMessage.getContent())
            {
                sections = sectionDecoder.parseAll(allData);
            }

            List<EncodingRetainingSection<?>> bodySections = new ArrayList<>(sections.size());
            ListIterator<EncodingRetainingSection<?>> iterator = sections.listIterator();
            EncodingRetainingSection<?> previousSection = null;
            while(iterator.hasNext())
            {
                EncodingRetainingSection<?> section = iterator.next();
                if (section instanceof AmqpValueSection || section instanceof DataSection || section instanceof AmqpSequenceSection)
                {
                    if (previousSection != null && (previousSection.getClass() != section.getClass() || section instanceof AmqpValueSection))
                    {
                        throw new MessageConversionException("Message is badly formed and has multiple body section which are not all Data or not all AmqpSequence");
                    }
                    else
                    {
                        previousSection = section;
                    }
                    bodySections.add(section);
                }
            }

            // In 1.0 of the spec, it is illegal to have message with no body but AMQP-127 asks to have that restriction lifted
            if (!bodySections.isEmpty())
            {
                EncodingRetainingSection<?> firstBodySection = bodySections.get(0);
                if(firstBodySection instanceof AmqpValueSection)
                {
                    bodyObject = convertValue(firstBodySection.getValue());
                }
                else if(firstBodySection instanceof DataSection)
                {
                    int totalSize = 0;
                    for(EncodingRetainingSection<?> section : bodySections)
                    {
                        totalSize += ((DataSection)section).getValue().getArray().length;
                    }
                    byte[] bodyData = new byte[totalSize];
                    ByteBuffer buf = ByteBuffer.wrap(bodyData);
                    for(EncodingRetainingSection<?> section : bodySections)
                    {
                        buf.put(((DataSection) section).getValue().asByteBuffer());
                    }
                    bodyObject = bodyData;
                }
                else
                {
                    ArrayList<Object> totalSequence = new ArrayList<>();
                    for(EncodingRetainingSection<?> section : bodySections)
                    {
                        totalSequence.addAll(((AmqpSequenceSection)section).getValue());
                    }
                    bodyObject = convertValue(totalSequence);
                }
            }

        }
        catch (AmqpErrorException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
        finally
        {
            if (sections != null)
            {
                sections.forEach(EncodingRetaining::dispose);
            }
        }
        return bodyObject;
    }

    private static Map<Object, Object> convertMap(final Map<Object, Object> map)
    {
        Map<Object, Object> resultMap = new LinkedHashMap<>();
        for (final Map.Entry<Object, Object> entry :  map.entrySet())
        {
            resultMap.put(convertValue(entry.getKey()), convertValue(entry.getValue()));
        }
        return resultMap;
    }

    public static Object convertValue(final Object value)
    {
        if(value != null && !STANDARD_TYPES.contains(value.getClass()))
        {
            if(value instanceof Map)
            {
                return convertMap((Map<Object,Object>)value);
            }
            else if(value instanceof List)
            {
                return convertList((List<Object>)value);
            }
            else if(value instanceof UnsignedByte)
            {
                return ((UnsignedByte)value).shortValue();
            }
            else if(value instanceof UnsignedShort)
            {
                return ((UnsignedShort)value).intValue();
            }
            else if(value instanceof UnsignedInteger)
            {
                return ((UnsignedInteger)value).longValue();
            }
            else if(value instanceof UnsignedLong)
            {
                return ((UnsignedLong)value).longValue();
            }
            else if(value instanceof Symbol)
            {
                return value.toString();
            }
            else if(value instanceof Binary)
            {
                return ((Binary)value).getArray();
            }
            else
            {
                throw new MessageConversionException(String.format(
                        "Could not convert message from 1.0. Unsupported type '%s'.",
                        value.getClass().getSimpleName()));
            }
        }
        else
        {
            return value;
        }
    }

    private static List<Object> convertList(final List<Object> list)
    {
        List<Object> result = new ArrayList<>(list.size());
        for(Object entry : list)
        {
            result.add(convertValue(entry));
        }
        return result;
    }

    private static ContentHint getAmqp0xTypeHint(final Message_1_0 serverMsg)
    {
        Symbol contentType = getContentType(serverMsg);

        JmsMessageTypeAnnotation jmsMessageTypeAnnotation = getJmsMessageTypeAnnotation(serverMsg);

        String mimeTypeHint = null;
        Class<?> classHint = getContentTypeClassHint(jmsMessageTypeAnnotation);

        if (contentType != null)
        {
            Class<?> contentTypeClassHint = null;
            String type = contentType.toString();
            String supportedContentType = null;
            if (ConversionUtils.TEXT_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = String.class;
                // the AMQP 0-x client does not accept arbitrary "text/*" mimeTypes so use "text/plain"
                supportedContentType = "text/plain";
            }
            else if (ConversionUtils.MAP_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = Map.class;
                supportedContentType = contentType.toString();
            }
            else if (ConversionUtils.LIST_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = List.class;
                supportedContentType = contentType.toString();
            }
            else if (ConversionUtils.OBJECT_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = Serializable.class;
                // the AMQP 0-x client does not accept the "application/x-java-serialized-object" mimeTypes so use fall back
                supportedContentType = "application/java-object-stream";
            }
            else if (ConversionUtils.BYTES_MESSAGE_CONTENT_TYPES.matcher(type).matches())
            {
                contentTypeClassHint = byte[].class;
                supportedContentType = "application/octet-stream";
            }

            if (classHint == null || classHint == contentTypeClassHint)
            {
                classHint = contentTypeClassHint;
                mimeTypeHint = supportedContentType;
            }
        }

        return new ContentHint(classHint, mimeTypeHint);
    }

    static Class<?> getContentTypeClassHint(final JmsMessageTypeAnnotation jmsMessageTypeAnnotation)
    {
        Class<?> classHint = null;
        if (jmsMessageTypeAnnotation != null)
        {
            switch (jmsMessageTypeAnnotation)
            {
                case MESSAGE:
                    classHint = Void.class;
                    break;
                case MAP_MESSAGE:
                    classHint = Map.class;
                    break;
                case BYTES_MESSAGE:
                    classHint = byte[].class;
                    break;
                case OBJECT_MESSAGE:
                    classHint = Serializable.class;
                    break;
                case TEXT_MESSAGE:
                    classHint = String.class;
                    break;
                case STREAM_MESSAGE:
                    classHint = List.class;
                    break;
                default:
                    throw new ServerScopedRuntimeException(String.format(
                            "Unexpected jms message type annotation %s", jmsMessageTypeAnnotation));
            }
        }
        return classHint;
    }

    static JmsMessageTypeAnnotation getJmsMessageTypeAnnotation(final Message_1_0 serverMsg)
    {
        JmsMessageTypeAnnotation jmsMessageTypeAnnotation = null;
        MessageAnnotationsSection section = serverMsg.getMessageAnnotationsSection();
        if (section != null)
        {
            Map<Symbol, Object> annotations = section.getValue();
            section.dispose();
            if (annotations != null && annotations.containsKey(JmsMessageTypeAnnotation.ANNOTATION_KEY))
            {
                Object object = annotations.get(JmsMessageTypeAnnotation.ANNOTATION_KEY);
                if (object instanceof Byte)
                {
                    try
                    {
                        jmsMessageTypeAnnotation = JmsMessageTypeAnnotation.valueOf(((Byte) object));
                    }
                    catch (IllegalArgumentException e)
                    {
                        // ignore
                    }
                }
            }
        }
        return jmsMessageTypeAnnotation;
    }

    public static Symbol getContentType(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();

        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                return properties.getContentType();
            }
        }
        return null;
    }

    public static UnsignedInteger getGroupSequence(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                return properties.getGroupSequence();
            }
        }
        return null;
    }

    public static String getGroupId(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                return properties.getGroupId();
            }
        }
        return null;
    }

    public static Date getCreationTime(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                return properties.getCreationTime();
            }
        }
        return null;
    }

    public static Date getAbsoluteExpiryTime(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                return properties.getAbsoluteExpiryTime();
            }
        }
        return null;
    }

    public static Long getTtl(final Message_1_0 serverMsg)
    {
        HeaderSection headerSection = serverMsg.getHeaderSection();
        if (headerSection != null)
        {
            Header header = headerSection.getValue();
            headerSection.dispose();
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

    public static Binary getUserId(final Message_1_0 serverMsg)
    {
        Binary userId = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                userId = properties.getUserId();
            }
        }
        return userId;
    }

    public static String getReplyTo(final Message_1_0 serverMsg)
    {
        String replyTo = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                replyTo = properties.getReplyTo();
            }
        }
        return replyTo;
    }

    static Symbol getContentEncoding(final Message_1_0 serverMsg)
    {
        Symbol contentEncoding = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                contentEncoding = properties.getContentEncoding();
            }
        }
        return contentEncoding;
    }

    public static Object getCorrelationId(final Message_1_0 serverMsg)
    {
        Object correlationIdObject = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                correlationIdObject = properties.getCorrelationId();

            }
        }
        return correlationIdObject;
    }

    public static Object getMessageId(final Message_1_0 serverMsg)
    {
        Object messageId = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
            propertiesSection.dispose();
            if (properties != null)
            {
                messageId = properties.getMessageId();

            }
        }
        return messageId;
    }

    public static ConvertedContentAndMimeType getAmqp0xConvertedContentAndMimeType(final Message_1_0 serverMsg)
    {
        Object bodyObject = convertBodyToObject(serverMsg);
        ObjectToMimeContentConverter converter = getBestFitObjectToMimeContentConverter(bodyObject);

        ContentHint contentHint = getAmqp0xTypeHint(serverMsg);
        Class<?> typeHint = contentHint.getContentClass();
        if (typeHint == null && bodyObject == null)
        {
            typeHint = Void.class;
        }

        if (converter == null)
        {
            converter = getBestFitObjectToMimeContentConverter(bodyObject, typeHint);

            if (converter == null)
            {
                throw new MessageConversionException(String.format(
                        "Could not convert message from 1.0 to 0-x because conversion of content failed."
                        + " Could not find mime type converter for the content '%s'.",
                        bodyObject == null ? null : bodyObject.getClass().getSimpleName()));
            }
        }

        final byte[] messageContent = converter.toMimeContent(bodyObject);
        String mimeType = converter.getMimeType();
        if (bodyObject instanceof byte[])
        {
            if (Serializable.class == typeHint)
            {
                mimeType = "application/java-object-stream";
            }
            else if (String.class == typeHint)
            {
                mimeType = "text/plain";
            }
            else if ((Map.class == typeHint || List.class == typeHint) && contentHint.getContentType() != null)
            {
                mimeType = contentHint.getContentType();
            }
        }

        return new ConvertedContentAndMimeType(messageContent, mimeType);
    }

    public static class ContentHint
    {
        private final Class<?> _contentClass;
        private final String _contentType;

        ContentHint(final Class<?> contentClass, final String contentType)
        {
            _contentClass = contentClass;
            _contentType = contentType;
        }

        Class<?> getContentClass()
        {
            return _contentClass;
        }

        public String getContentType()
        {
            return _contentType;
        }
    }

    public static class ConvertedContentAndMimeType
    {
        private final byte[] _content;
        private final String _mimeType;

        private ConvertedContentAndMimeType(final byte[] content, final String mimeType)
        {
            _content = content;
            _mimeType = mimeType;
        }

        public byte[] getContent()
        {
            return _content;
        }

        public String getMimeType()
        {
            return _mimeType;
        }
    }
}
