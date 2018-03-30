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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
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

public class MessageConverter_Internal_to_1_0Test extends UnitTestBase
{
    private final MessageConverter_Internal_to_v1_0 _converter = new MessageConverter_Internal_to_v1_0();
    private final AMQPDescribedTypeRegistry _typeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                     .registerTransportLayer()
                                                                                     .registerMessagingLayer()
                                                                                     .registerTransactionLayer()
                                                                                     .registerSecurityLayer();

    private final StoredMessage<InternalMessageMetaData> _handle = mock(StoredMessage.class);

    private final AMQMessageHeader _amqpHeader = mock(AMQMessageHeader.class);

    @Before
    public void setUp() throws Exception
    {
    }


    @Test
    public void testStringMessage() throws Exception
    {
        String content = "testContent";
        final String mimeType = "text/plain";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }

    @Test
    public void testStringMessageWithUnknownMimeType() throws Exception
    {
        String content = "testContent";
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }

    @Test
    public void testStringMessageWithoutMimeType() throws Exception
    {
        String content = "testContent";
        doTest(content,
               null,
               AmqpValueSection.class,
               content,
               Symbol.valueOf("text/plain"),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }

    @Test
    public void testListMessageWithMimeType() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42);
        doTest(content,
               "text/plain",
               AmqpSequenceSection.class,
               content,
               null,
               JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    public void testListMessageWithoutMimeType() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42);
        doTest(content,
               null,
               AmqpSequenceSection.class,
               content,
               null,
               JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    public void testListMessageWithoutMimeTypeWithNonJmsContent() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42, Lists.newArrayList());
        doTest(content,
               null,
               AmqpSequenceSection.class,
               content,
               null,
               null);
    }

    @Test
    public void testByteArrayMessageWithoutMimeType() throws Exception
    {
        byte[] content = "testContent".getBytes(UTF_8);
        doTest(content,
               null,
               DataSection.class,
               content,
               Symbol.valueOf("application/octet-stream"),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    @Test
    public void testByteArrayMessageWithMimeType() throws Exception
    {
        byte[] content = "testContent".getBytes(UTF_8);
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               DataSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    @Test
    public void testEmptyByteArrayMessageWithMimeType() throws Exception
    {
        byte[] content = new byte[0];
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               DataSection.class,
               content,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    @Test
    public void testMapMessageWithMimeType() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
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
    public void testMapMessageWithoutMimeType() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
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
    public void testMapMessageWithMimeTypeWithNonJmsContent() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
        content.put(37, Collections.singletonMap("foo", "bar"));
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               AmqpValueSection.class,
               content,
               null,
               null);
    }

    @Test
    public void testSerializableMessageWithMimeType() throws Exception
    {
        Serializable content = new MySerializable();
        final String mimeType = "foo/bar";
        doTest(content,
               mimeType,
               DataSection.class,
               getObjectStreamMessageBytes(content),
               Symbol.valueOf("application/x-java-serialized-object"),
               JmsMessageTypeAnnotation.OBJECT_MESSAGE.getType());
    }

    @Test
    public void testSerializableMessageWithoutMimeType() throws Exception
    {
        Serializable content = new MySerializable();
        doTest(content,
               null,
               DataSection.class,
               getObjectStreamMessageBytes(content),
               Symbol.valueOf("application/x-java-serialized-object"),
               JmsMessageTypeAnnotation.OBJECT_MESSAGE.getType());
    }

    @Test
    public void testNullMessageWithoutMimeType() throws Exception
    {
        doTest(null,
               null,
               AmqpValueSection.class,
               null,
               null,
               JmsMessageTypeAnnotation.MESSAGE.getType());
    }

    @Test
    public void testUuidMessageWithMimeType() throws Exception
    {
        UUID content = UUID.randomUUID();
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
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(o);
            return bos.toByteArray();
        }
    }

    private List<EncodingRetainingSection<?>> getEncodingRetainingSections(final QpidByteBuffer content,
                                                                           final int expectedNumberOfSections)
            throws Exception
    {
        SectionDecoder sectionDecoder = new SectionDecoderImpl(_typeRegistry.getSectionDecoderRegistry());
        final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(content);
        assertEquals("Unexpected number of sections", (long) expectedNumberOfSections, (long) sections.size());
        return sections;
    }


    protected InternalMessage getAmqMessage(final Serializable content, final String mimeType) throws Exception
    {
        final byte[] serializedContent = getObjectStreamMessageBytes(content);
        configureMessageContent(serializedContent);
        configureMessageHeader(mimeType);

        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(_amqpHeader);
        final int contentSize = serializedContent == null ? 0 : serializedContent.length;
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(false, internalMessageHeader, contentSize);
        when(_handle.getMetaData()).thenReturn(metaData);

        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(_handle));
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(_amqpHeader.getMimeType()).thenReturn(mimeType);
    }

    private void configureMessageContent(byte[] section)
    {
        if (section == null)
        {
            section = new byte[0];
        }
        final QpidByteBuffer combined = QpidByteBuffer.wrap(section);
        when(_handle.getContentSize()).thenReturn(section.length);
        final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);

        when(_handle.getContent(offsetCaptor.capture(),
                                sizeCaptor.capture())).then(invocation -> combined.view(offsetCaptor.getValue(),
                                                                                        sizeCaptor.getValue()));
    }

    private Byte getJmsMessageTypeAnnotation(final Message_1_0 convertedMessage)
    {
        MessageAnnotationsSection messageAnnotationsSection = convertedMessage.getMessageAnnotationsSection();
        if (messageAnnotationsSection != null)
        {
            Map<Symbol, Object> messageAnnotations = messageAnnotationsSection.getValue();
            if (messageAnnotations != null)
            {
                Object annotation = messageAnnotations.get(Symbol.valueOf("x-opt-jms-msg-type"));
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
        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent();

        List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        EncodingRetainingSection<?> encodingRetainingSection = sections.get(0);
        assertEquals("Unexpected section type", expectedBodySection, encodingRetainingSection.getClass());

        if (expectedContent instanceof byte[])
        {
            assertArrayEquals("Unexpected content",
                              ((byte[]) expectedContent),
                              ((Binary) encodingRetainingSection.getValue()).getArray());
        }
        else
        {
            assertEquals("Unexpected content", expectedContent, encodingRetainingSection.getValue());
        }

        Symbol contentType = getContentType(convertedMessage);
        if (expectedContentType == null)
        {
            assertNull("Content type should be null", contentType);
        }
        else
        {
            assertEquals("Unexpected content type", expectedContentType, contentType);
        }

        Byte jmsMessageTypeAnnotation = getJmsMessageTypeAnnotation(convertedMessage);
        if (expectedJmsTypeAnnotation == null)
        {
            assertEquals("Unexpected annotation 'x-opt-jms-msg-type'", null, jmsMessageTypeAnnotation);
        }
        else
        {
            assertEquals("Unexpected annotation 'x-opt-jms-msg-type'",
                                expectedJmsTypeAnnotation,
                                jmsMessageTypeAnnotation);
        }
    }

    private static class MySerializable implements Serializable
    {
    }
}
