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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.BYTES_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.LIST_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.MAP_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.OBJECT_MESSAGE_CONTENT_TYPES;
import static org.apache.qpid.server.message.mimecontentconverter.ConversionUtils.TEXT_CONTENT_TYPES;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.BYTES_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.MAP_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.OBJECT_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.STREAM_MESSAGE;
import static org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation.TEXT_MESSAGE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentConverterRegistry;
import org.apache.qpid.server.message.mimecontentconverter.MimeContentToObjectConverter;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequence;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequenceSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NonEncodingRetainingSection;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.GZIPUtils;

public abstract class MessageConverter_to_1_0<M extends ServerMessage> implements MessageConverter<M, Message_1_0>
{
    private static final byte[] SERIALIZED_NULL = getObjectBytes(null);
    private final AMQPDescribedTypeRegistry _typeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                     .registerTransportLayer()
                                                                                     .registerMessagingLayer()
                                                                                     .registerTransactionLayer()
                                                                                     .registerSecurityLayer();

    public static Symbol getContentType(final String contentMimeType)
    {
        Symbol contentType = null;
        if (contentMimeType != null)
        {
            if (TEXT_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                contentType = Symbol.valueOf(contentMimeType);
            }
            else if (BYTES_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                contentType = Symbol.valueOf("application/octet-stream");
            }
            else if (MAP_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                contentType = null;
            }
            else if (LIST_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                contentType = null;
            }
            else if (OBJECT_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                contentType = Symbol.valueOf("application/x-java-serialized-object");
            }
            else
            {
                contentType = Symbol.valueOf(contentMimeType);
            }
        }
        return contentType;
    }

    public static MessageAnnotations createMessageAnnotation(final EncodingRetainingSection<?> bodySection,
                                                             final String contentMimeType)
    {
        MessageAnnotations messageAnnotations = null;
        final Symbol key = Symbol.valueOf("x-opt-jms-msg-type");
        if (contentMimeType != null)
        {
            if (TEXT_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                messageAnnotations = new MessageAnnotations(Collections.singletonMap(key, TEXT_MESSAGE.getType()));
            }
            else if (BYTES_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                messageAnnotations = new MessageAnnotations(Collections.singletonMap(key, BYTES_MESSAGE.getType()));
            }
            else if (MAP_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                if (isSectionValidForJmsMap(bodySection))
                {
                    messageAnnotations = new MessageAnnotations(Collections.singletonMap(key, MAP_MESSAGE.getType()));
                }
            }
            else if (LIST_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                if (isSectionValidForJmsList(bodySection))
                {
                    messageAnnotations =
                            new MessageAnnotations(Collections.singletonMap(key, STREAM_MESSAGE.getType()));
                }
            }
            else if (OBJECT_MESSAGE_CONTENT_TYPES.matcher(contentMimeType).matches())
            {
                messageAnnotations = new MessageAnnotations(Collections.singletonMap(key, OBJECT_MESSAGE.getType()));
            }
        }
        else if (bodySection instanceof AmqpValueSection && bodySection.getValue() == null)
        {
            messageAnnotations = new MessageAnnotations(Collections.singletonMap(key, MESSAGE.getType()));
        }
        return messageAnnotations;
    }

    public static boolean isSectionValidForJmsList(final EncodingRetainingSection<?> section)
    {
        if (section instanceof AmqpSequenceSection)
        {
            final List<?> list = ((AmqpSequenceSection) section).getValue();
            for (Object entry: list)
            {
                if (!(entry == null
                      || entry instanceof Boolean
                      || entry instanceof Byte
                      || entry instanceof Short
                      || entry instanceof Integer
                      || entry instanceof Long
                      || entry instanceof Float
                      || entry instanceof Double
                      || entry instanceof Character
                      || entry instanceof String
                      || entry instanceof Binary))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean isSectionValidForJmsMap(final EncodingRetainingSection<?> section)
    {
        if (section instanceof AmqpValueSection)
        {
            final Object valueObject = ((AmqpValueSection) section).getValue();
            if (valueObject instanceof Map)
            {
                final Map<?, ?> map = (Map) valueObject;
                for (Map.Entry<?,?> entry: map.entrySet())
                {
                    if (!(entry.getKey() instanceof String))
                    {
                        return false;
                    }
                    Object value = entry.getValue();
                    if (!(value == null
                          || value instanceof Boolean
                          || value instanceof Byte
                          || value instanceof Short
                          || value instanceof Integer
                          || value instanceof Long
                          || value instanceof Float
                          || value instanceof Double
                          || value instanceof Character
                          || value instanceof String
                          || value instanceof Binary))
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public final Class<Message_1_0> getOutputClass()
    {
        return Message_1_0.class;
    }

    @Override
    public final Message_1_0 convert(M message, NamedAddressSpace addressSpace)
    {

        SectionEncoder sectionEncoder = new SectionEncoderImpl(_typeRegistry);
        return new Message_1_0(convertToStoredMessage(message, sectionEncoder));
    }

    @Override
    public void dispose(final Message_1_0 message)
    {
        if(message.getStoredMessage() instanceof ConvertedMessage)
        {
            ((ConvertedMessage<?>)message.getStoredMessage()).dispose();
        }
    }

    private ConvertedMessage<M> convertToStoredMessage(final M serverMessage, SectionEncoder sectionEncoder)
    {
        EncodingRetainingSection<?> bodySection = getBodySection(serverMessage, sectionEncoder);

        final MessageMetaData_1_0 metaData = convertMetaData(serverMessage, bodySection, sectionEncoder);
        return convertServerMessage(metaData, serverMessage, bodySection);
    }

    abstract protected MessageMetaData_1_0 convertMetaData(final M serverMessage,
                                                           final EncodingRetainingSection<?> bodySection,
                                                           SectionEncoder sectionEncoder);


    private static NonEncodingRetainingSection<?> convertMessageBody(String mimeType, byte[] data)
    {
        if (data != null && data.length != 0)
        {

            MimeContentToObjectConverter converter =
                    MimeContentConverterRegistry.getMimeContentToObjectConverter(mimeType);
            if (converter != null)
            {
                Object bodyObject = converter.toObject(data);

                if (bodyObject instanceof String)
                {
                    return new AmqpValue(bodyObject);
                }
                else if (bodyObject instanceof Map)
                {
                    return new AmqpValue(fixMapValues((Map<String, Object>) bodyObject));
                }
                else if (bodyObject instanceof List)
                {
                    return new AmqpSequence(fixListValues((List<Object>) bodyObject));
                }
            }
            else if (mimeType != null && TEXT_CONTENT_TYPES.matcher(mimeType).matches())
            {
                return new AmqpValue(new String(data, UTF_8));
            }
        }
        else if (mimeType == null)
        {
            return new AmqpValue(null);
        }
        else if (OBJECT_MESSAGE_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return new Data(new Binary(SERIALIZED_NULL));
        }
        else if (TEXT_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return new AmqpValue("");
        }
        else if (MAP_MESSAGE_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return new AmqpValue(Collections.emptyMap());
        }
        else if (LIST_MESSAGE_CONTENT_TYPES.matcher(mimeType).matches())
        {
            return new AmqpSequence(Collections.emptyList());
        }
        return new Data(new Binary(data));
    }

    static Map<String, Object> fixMapValues(Map<String, Object> map)
    {
        map = new LinkedHashMap<>(map);
        for(Map.Entry<String,Object> entry : map.entrySet())
        {
            entry.setValue(fixValue(entry.getValue()));
        }
        return map;
    }

    static Object fixValue(final Object value)
    {
        if(value instanceof byte[])
        {
            return new Binary((byte[])value);
        }
        else if(value instanceof Map)
        {
            return fixMapValues((Map)value);
        }
        else if(value instanceof List)
        {
            return fixListValues((List)value);
        }
        else
        {
            return value;
        }
    }

    static List<Object> fixListValues(List<Object> list)
    {
        list = new ArrayList<>(list);
        ListIterator<Object> iterator = list.listIterator();
        while(iterator.hasNext())
        {
            Object value = iterator.next();
            iterator.set(fixValue(value));

        }
        return list;
    }

    private ConvertedMessage<M> convertServerMessage(final MessageMetaData_1_0 metaData,
                                                  final M serverMessage,
                                                  final EncodingRetainingSection<?> section)
    {

        return new ConvertedMessage<>(metaData, serverMessage, section);
    }

    protected EncodingRetainingSection<?> getBodySection(final M serverMessage, final SectionEncoder encoder)
    {
        final String mimeType = serverMessage.getMessageHeader().getMimeType();
        byte[] data = new byte[(int) serverMessage.getSize()];

        try (QpidByteBuffer content = serverMessage.getContent())
        {
            content.get(data);
        }

        byte[] uncompressed;
        if(GZIPUtils.GZIP_CONTENT_ENCODING.equals(serverMessage.getMessageHeader().getEncoding())
           && (uncompressed = GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(data))) != null)
        {
            data = uncompressed;
        }

        return convertMessageBody(mimeType, data).createEncodingRetainingSection();
    }

    private static byte[] getObjectBytes(final Object object)
    {
        final byte[] expected;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(object);
            expected = baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
        return expected;
    }

    private static class ConvertedMessage<M extends ServerMessage> implements StoredMessage<MessageMetaData_1_0>
    {
        private final MessageMetaData_1_0 _metaData;
        private final M _serverMessage;
        private final EncodingRetainingSection<?> _section;

        public ConvertedMessage(final MessageMetaData_1_0 metaData,
                                final M serverMessage,
                                final EncodingRetainingSection<?> section)
        {
            _metaData = metaData;
            _serverMessage = serverMessage;
            _section = section;
        }

        @Override
        public MessageMetaData_1_0 getMetaData()
        {
            return _metaData;
        }

        @Override
        public long getMessageNumber()
        {
            return _serverMessage.getMessageNumber();
        }

        @Override
        public QpidByteBuffer getContent(int offset, int length)
        {
            try (QpidByteBuffer content = _section.getEncodedForm())
            {
                return content.view(offset, length);
            }
        }

        @Override
        public int getContentSize()
        {
            return _metaData.getContentSize();
        }

        @Override
        public int getMetadataSize()
        {
            return _metaData.getStorableSize();
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

        private void dispose()
        {
            _section.dispose();
            _metaData.dispose();
        }
    }
}
