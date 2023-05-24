/*
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
import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.getContentType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.message.internal.InternalMessageMetaDataType;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequenceSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class MessageConverter_Internal_to_1_0Test extends UnitTestBase
{
    private static final MessageConverter_Internal_to_v1_0 CONVERTER = new MessageConverter_Internal_to_v1_0();
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer();

    private static final StoredMessage<InternalMessageMetaData> HANDLE = mock(StoredMessage.class);

    private static final AMQMessageHeader AMQP_HEADER = mock(AMQMessageHeader.class);

    @Test
    void stringMessage() throws Exception
    {
        final String content = "testContent";
        final String mimeType = "text/plain";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }

    @Test
    void stringMessageWithUnknownMimeType() throws Exception
    {
        final String content = "testContent";
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }

    @Test
    void stringMessageWithoutMimeType() throws Exception
    {
        final String content = "testContent";
        doTest(content,
               null,
               AmqpValueSection.class,
               content,
               Symbol.valueOf("text/plain"),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }

    @Test
    void listMessageWithMimeType() throws Exception
    {
        final ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42);
        doTest(content,
               "text/plain",
               AmqpSequenceSection.class,
               content,
               null,
               JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    void listMessageWithoutMimeType() throws Exception
    {
        final ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42);
        doTest(content,
               null,
               AmqpSequenceSection.class,
               content,
               null,
               JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    void listMessageWithoutMimeTypeWithNonJmsContent() throws Exception
    {
        final ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42, Lists.newArrayList());
        doTest(content,
               null,
               AmqpSequenceSection.class,
               content,
               null,
               null);
    }

    @Test
    void byteArrayMessageWithoutMimeType() throws Exception
    {
        final byte[] content = "testContent".getBytes(UTF_8);
        doTest(content,
               null,
               DataSection.class,
               content,
               Symbol.valueOf("application/octet-stream"),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    @Test
    void byteArrayMessageWithMimeType() throws Exception
    {
        final byte[] content = "testContent".getBytes(UTF_8);
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               DataSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    @Test
    void emptyByteArrayMessageWithMimeType() throws Exception
    {
        final byte[] content = new byte[0];
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               DataSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    @Test
    void mapMessageWithMimeType() throws Exception
    {
        final HashMap<Object, Object> content = new HashMap<>();
        content.put("key1", 37);
        content.put("key2", "foo");
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               null,
               JmsMessageTypeAnnotation.MAP_MESSAGE.getType());
    }

    @Test
    void mapMessageWithoutMimeType() throws Exception
    {
        final HashMap<Object, Object> content = new HashMap<>();
        content.put("key1", 37);
        content.put("key2", "foo");
        doTest(content,
               null,
               AmqpValueSection.class,
               content,
               null,
               JmsMessageTypeAnnotation.MAP_MESSAGE.getType());
    }

    @Test
    void mapMessageWithMimeTypeWithNonJmsContent() throws Exception
    {
        final HashMap<Object, Object> content = new HashMap<>();
        content.put(37, Map.of("foo", "bar"));
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               null,
               null);
    }

    @Test
    void serializableMessageWithMimeType() throws Exception
    {
        final Serializable content = new MySerializable();
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               DataSection.class,
               getObjectStreamMessageBytes(content),
               Symbol.valueOf("application/x-java-serialized-object"),
               JmsMessageTypeAnnotation.OBJECT_MESSAGE.getType());
    }

    @Test
    void serializableMessageWithoutMimeType() throws Exception
    {
        final Serializable content = new MySerializable();
        doTest(content,
               null,
               DataSection.class,
               getObjectStreamMessageBytes(content),
               Symbol.valueOf("application/x-java-serialized-object"),
               JmsMessageTypeAnnotation.OBJECT_MESSAGE.getType());
    }

    @Test
    void nullMessageWithoutMimeType() throws Exception
    {
        doTest(null,
               null,
               AmqpValueSection.class,
               null,
               null,
               JmsMessageTypeAnnotation.MESSAGE.getType());
    }

    @Test
    void uuidMessageWithMimeType() throws Exception
    {
        final UUID content = UUID.randomUUID();
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               Symbol.valueOf(mimeType),
               null);
    }

    private byte[] getObjectStreamMessageBytes(final Serializable o) throws Exception
    {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(o);
            return bos.toByteArray();
        }
    }

    private List<EncodingRetainingSection<?>> getEncodingRetainingSections(final QpidByteBuffer content,
                                                                           final int expectedNumberOfSections)
            throws Exception
    {
        final SectionDecoder sectionDecoder = new SectionDecoderImpl(TYPE_REGISTRY.getSectionDecoderRegistry());
        final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(content);
        assertEquals(expectedNumberOfSections, (long) sections.size(), "Unexpected number of sections");
        return sections;
    }

    protected InternalMessage getAmqMessage(final Serializable content, final String mimeType) throws Exception
    {
        final byte[] serializedContent = getObjectStreamMessageBytes(content);
        configureMessageContent(serializedContent);
        configureMessageHeader(mimeType);

        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(AMQP_HEADER);
        final int contentSize = serializedContent == null ? 0 : serializedContent.length;
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(false, internalMessageHeader, contentSize);
        when(HANDLE.getMetaData()).thenReturn(metaData);

        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(HANDLE));
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(AMQP_HEADER.getMimeType()).thenReturn(mimeType);
    }

    private void configureMessageContent(byte[] section)
    {
        if (section == null)
        {
            section = new byte[0];
        }
        final QpidByteBuffer combined = QpidByteBuffer.wrap(section);
        when(HANDLE.getContentSize()).thenReturn(section.length);
        final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);

        when(HANDLE.getContent(offsetCaptor.capture(), sizeCaptor.capture()))
                .then(invocation -> combined.view(offsetCaptor.getValue(), sizeCaptor.getValue()));
    }

    private Byte getJmsMessageTypeAnnotation(final Message_1_0 convertedMessage)
    {
        final MessageAnnotationsSection messageAnnotationsSection = convertedMessage.getMessageAnnotationsSection();
        if (messageAnnotationsSection != null)
        {
            final Map<Symbol, Object> messageAnnotations = messageAnnotationsSection.getValue();
            if (messageAnnotations != null)
            {
                final Object annotation = messageAnnotations.get(Symbol.valueOf("x-opt-jms-msg-type"));
                if (annotation instanceof Byte)
                {
                    return ((Byte) annotation);
                }
            }
        }
        return null;
    }

    private void doTest(final Serializable messageBytes,
                        final String mimeType,
                        final Class<? extends EncodingRetainingSection<?>> expectedBodySection,
                        final Object expectedContent,
                        final Symbol expectedContentType,
                        final Byte expectedJmsTypeAnnotation) throws Exception
    {
        final InternalMessage sourceMessage = getAmqMessage(messageBytes, mimeType);
        final Message_1_0 convertedMessage = CONVERTER.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent();

        final List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        final EncodingRetainingSection<?> encodingRetainingSection = sections.get(0);
        assertEquals(expectedBodySection, encodingRetainingSection.getClass(), "Unexpected section type");

        if (expectedContent instanceof byte[])
        {
            assertArrayEquals(((byte[]) expectedContent), ((Binary) encodingRetainingSection.getValue()).getArray(),
                    "Unexpected content");
        }
        else
        {
            assertEquals(expectedContent, encodingRetainingSection.getValue(), "Unexpected content");
        }

        final Symbol contentType = getContentType(convertedMessage);
        if (expectedContentType == null)
        {
            assertNull(contentType, "Content type should be null");
        }
        else
        {
            assertEquals(expectedContentType, contentType, "Unexpected content type");
        }

        final Byte jmsMessageTypeAnnotation = getJmsMessageTypeAnnotation(convertedMessage);
        if (expectedJmsTypeAnnotation == null)
        {
            assertNull(jmsMessageTypeAnnotation, "Unexpected annotation 'x-opt-jms-msg-type'");
        }
        else
        {
            assertEquals(expectedJmsTypeAnnotation, jmsMessageTypeAnnotation, "Unexpected annotation 'x-opt-jms-msg-type'");
        }
    }

    private static class MySerializable implements Serializable
    {
    }
}
