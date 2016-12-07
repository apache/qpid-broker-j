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

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NonEncodingRetainingSection;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.transport.codec.BBDecoder;
import org.apache.qpid.typedmessage.TypedBytesContentReader;
import org.apache.qpid.typedmessage.TypedBytesFormatException;
import org.apache.qpid.util.GZIPUtils;

public abstract class MessageConverter_to_1_0<M extends ServerMessage> implements MessageConverter<M, Message_1_0>
{
    private final AMQPDescribedTypeRegistry _typeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                         .registerTransportLayer()
                                                                                         .registerMessagingLayer()
                                                                                         .registerTransactionLayer()
                                                                                         .registerSecurityLayer();

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


    private StoredMessage<MessageMetaData_1_0> convertToStoredMessage(final M serverMessage, SectionEncoder sectionEncoder)
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
        if("text/plain".equals(mimeType) || "text/xml".equals(mimeType))
        {
            String text = new String(data);
            return new AmqpValue(text);
        }
        else if("jms/map-message".equals(mimeType))
        {
            TypedBytesContentReader reader = new TypedBytesContentReader(ByteBuffer.wrap(data));

            LinkedHashMap map = new LinkedHashMap();
            final int entries = reader.readIntImpl();
            for (int i = 0; i < entries; i++)
            {
                try
                {
                    String propName = reader.readStringImpl();
                    Object value = reader.readObject();

                    map.put(propName, value);
                }
                catch (EOFException | TypedBytesFormatException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }

            return new AmqpValue(fixMapValues(map));

        }
        else if("amqp/map".equals(mimeType))
        {
            BBDecoder decoder = new BBDecoder();
            decoder.init(ByteBuffer.wrap(data));
            final Map<String,Object> map = decoder.readMap();

            return new AmqpValue(fixMapValues(map));

        }
        else if("amqp/list".equals(mimeType))
        {
            BBDecoder decoder = new BBDecoder();
            decoder.init(ByteBuffer.wrap(data));
            return new AmqpValue(fixListValues(decoder.readList()));
        }
        else if("jms/stream-message".equals(mimeType))
        {
            TypedBytesContentReader reader = new TypedBytesContentReader(ByteBuffer.wrap(data));

            List list = new ArrayList();
            while (reader.remaining() != 0)
            {
                try
                {
                    list.add(fixValue(reader.readObject()));
                }
                catch (TypedBytesFormatException | EOFException e)
                {
                    throw new ConnectionScopedRuntimeException(e);
                }
            }
            return new AmqpValue(list);
        }
        else
        {
            return new Data(new Binary(data));

        }
    }

    static Map fixMapValues(Map<String, Object> map)
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

    static List fixListValues(List list)
    {
        list = new ArrayList(list);
        ListIterator iterator = list.listIterator();
        while(iterator.hasNext())
        {
            Object value = iterator.next();
            iterator.set(fixValue(value));

        }
        return list;
    }

    private StoredMessage<MessageMetaData_1_0> convertServerMessage(final MessageMetaData_1_0 metaData,
                                                                    final M serverMessage,
                                                                    final EncodingRetainingSection<?> section)
    {

        return new StoredMessage<MessageMetaData_1_0>()
                    {
                        @Override
                        public MessageMetaData_1_0 getMetaData()
                        {
                            return metaData;
                        }

                        @Override
                        public long getMessageNumber()
                        {
                            return serverMessage.getMessageNumber();
                        }

                        @Override
                        public Collection<QpidByteBuffer> getContent(int offset, int length)
                        {
                            int position = 0;
                            List<QpidByteBuffer> content = new ArrayList<>();
                            for(QpidByteBuffer buf : section.getEncodedForm())
                            {
                                if(position < offset)
                                {
                                    if(offset - position < buf.remaining())
                                    {
                                        QpidByteBuffer view = buf.view(offset - position, Math.min(length, buf.remaining() - (offset-position)));
                                        content.add(view);
                                        position += view.remaining();
                                    }
                                    else
                                    {
                                        position += buf.remaining();
                                    }
                                }
                                else if(position <= offset+length)
                                {
                                    QpidByteBuffer view = buf.view(0, Math.min(length - (position-offset), buf.remaining()));
                                    content.add(view);
                                    position += view.remaining();
                                }

                                buf.dispose();
                            }
                            return content;
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
        };
    }

    protected EncodingRetainingSection<?> getBodySection(final M serverMessage, final SectionEncoder encoder)
    {
        final String mimeType = serverMessage.getMessageHeader().getMimeType();
        byte[] data = new byte[(int) serverMessage.getSize()];
        int total = 0;
        for(QpidByteBuffer b : serverMessage.getContent(0, (int) serverMessage.getSize()))
        {
            int len = b.remaining();
            b.get(data, total, len);
            b.dispose();
            total += len;
        }
        byte[] uncompressed;

        if(Symbol.valueOf(GZIPUtils.GZIP_CONTENT_ENCODING).equals(serverMessage.getMessageHeader().getEncoding())
           && (uncompressed = GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(data))) != null)
        {
            data = uncompressed;
        }

        return convertMessageBody(mimeType, data).createEncodingRetainingSection(encoder);
    }

}
