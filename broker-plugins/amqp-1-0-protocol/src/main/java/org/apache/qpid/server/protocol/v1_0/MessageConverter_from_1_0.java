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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

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
                                                                                        UUID.class));

    public static Object convertBodyToObject(final Message_1_0 serverMessage)
    {
        final Collection<QpidByteBuffer> allData = serverMessage.getContent(0, (int) serverMessage.getSize());
        SectionDecoderImpl sectionDecoder = new SectionDecoderImpl(MessageConverter_v1_0_to_Internal.TYPE_REGISTRY.getSectionDecoderRegistry());

        Object bodyObject;
        try
        {
            List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(new ArrayList<>(allData));
            for(QpidByteBuffer buf : allData)
            {
                buf.dispose();
            }

            ListIterator<EncodingRetainingSection<?>> iterator = sections.listIterator();
            EncodingRetainingSection<?> previousSection = null;
            while(iterator.hasNext())
            {
                EncodingRetainingSection<?> section = iterator.next();
                if(!(section instanceof AmqpValueSection || section instanceof DataSection || section instanceof AmqpSequenceSection))
                {
                    iterator.remove();
                }
                else
                {
                    if(previousSection != null && (previousSection.getClass() != section.getClass() || section instanceof AmqpValueSection))
                    {
                        throw new ConnectionScopedRuntimeException("Message is badly formed and has multiple body section which are not all Data or not all AmqpSequence");
                    }
                    else
                    {
                        previousSection = section;
                    }
                }
            }


            if(sections.isEmpty())
            {
                // should actually be illegal
                bodyObject = new byte[0];
            }
            else
            {
                EncodingRetainingSection<?> firstBodySection = sections.get(0);
                if(firstBodySection instanceof AmqpValueSection)
                {
                    bodyObject = convertValue(firstBodySection.getValue());
                }
                else if(firstBodySection instanceof DataSection)
                {
                    int totalSize = 0;
                    for(EncodingRetainingSection<?> section : sections)
                    {
                        totalSize += ((DataSection)section).getValue().getArray().length;
                    }
                    byte[] bodyData = new byte[totalSize];
                    ByteBuffer buf = ByteBuffer.wrap(bodyData);
                    for(EncodingRetainingSection<?> section : sections)
                    {
                        buf.put(((DataSection) section).getValue().asByteBuffer());
                    }
                    bodyObject = bodyData;
                }
                else
                {
                    ArrayList<Object> totalSequence = new ArrayList<>();
                    for(EncodingRetainingSection<?> section : sections)
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
        return bodyObject;
    }

    private static Map convertMap(final Map map)
    {
        Map resultMap = new LinkedHashMap();
        Iterator<Map.Entry> iterator = map.entrySet().iterator();
        while(iterator.hasNext())
        {
            Map.Entry entry = iterator.next();
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
                return convertMap((Map)value);
            }
            else if(value instanceof List)
            {
                return convertList((List)value);
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
            else if(value instanceof Date)
            {
                return ((Date)value).getTime();
            }
            else if(value instanceof Binary)
            {
                return ((Binary)value).getArray();
            }
            else
            {
                // Throw exception instead?
                return value.toString();
            }
        }
        else
        {
            return value;
        }
    }

    private static List convertList(final List list)
    {
        List result = new ArrayList(list.size());
        for(Object entry : list)
        {
            result.add(convertValue(entry));
        }
        return result;
    }

    public static UnsignedInteger getGroupSequence(final Message_1_0 serverMsg)
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

    public static String getGroupId(final Message_1_0 serverMsg)
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

    public static Date getCreationTime(final Message_1_0 serverMsg)
    {
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
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
            if (properties != null)
            {
                userId = properties.getUserId();
            }
        }
        return userId;
    }

    public static Object getCorrelationId(final Message_1_0 serverMsg)
    {
        Object correlationIdObject = null;
        final PropertiesSection propertiesSection = serverMsg.getPropertiesSection();
        if (propertiesSection != null)
        {
            final Properties properties = propertiesSection.getValue();
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
            if (properties != null)
            {
                messageId = properties.getMessageId();

            }
        }
        return messageId;
    }
}
