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
package org.apache.qpid.server.protocol.converter.v0_10_v1_0;

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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.ListToAmqpListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.MapToAmqpMapConverter;
import org.apache.qpid.server.protocol.v1_0.JmsMessageTypeAnnotation;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
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
import org.apache.qpid.server.typedmessage.TypedBytesContentWriter;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"unchecked"})
class MessageConverter_0_10_to_1_0Test extends UnitTestBase
{
    private final MessageConverter_0_10_to_1_0 _converter = new MessageConverter_0_10_to_1_0();
    private final AMQPDescribedTypeRegistry _typeRegistry = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer();
    private final StoredMessage<MessageMetaData_0_10> _handle = mock(StoredMessage.class);
    private final MessageMetaData_0_10 _metaData = mock(MessageMetaData_0_10.class);
    private final AMQMessageHeader _amqpHeader = mock(AMQMessageHeader.class);
    private final Header _header = mock(Header.class);

    private MessageProperties _messageProperties;

    @BeforeAll
    void setUp()
    {
        _messageProperties = new MessageProperties();
        when(_handle.getMetaData()).thenReturn(_metaData);
        when(_header.getMessageProperties()).thenReturn(_messageProperties);
        when(_metaData.getHeader()).thenReturn(_header);
        when(_metaData.getMessageHeader()).thenReturn(_amqpHeader);
        when(_metaData.getMessageProperties()).thenReturn(_messageProperties);
    }

    @Test
    void convertStringMessageBody() throws Exception
    {
        doTestTextMessage("helloworld", "text/plain");
    }

    @Test
    void convertEmptyStringMessageBody() throws Exception
    {
        doTestTextMessage(null, "text/plain");
    }

    @Test
    void convertStringXmlMessageBody() throws Exception
    {
        doTestTextMessage("<helloworld></helloworld>", "text/xml");
    }

    @Test
    void convertEmptyStringXmlMessageBody() throws Exception
    {
        doTestTextMessage(null, "text/xml");
    }

    @Test
    void convertEmptyStringApplicationXmlMessageBody() throws Exception
    {
        doTestTextMessage(null, "application/xml");
    }

    @Test
    void convertStringWithContentTypeText() throws Exception
    {
        doTestTextMessage("foo","text/foobar");
    }

    @Test
    void convertStringWithContentTypeApplicationXml() throws Exception
    {
        doTestTextMessage("<helloworld></helloworld>","application/xml");
    }

    @Test
    void convertStringWithContentTypeApplicationXmlDtd() throws Exception
    {
        doTestTextMessage("<!DOCTYPE name []>","application/xml-dtd");
    }

    @Test
    void convertStringWithContentTypeApplicationFooXml() throws Exception
    {
        doTestTextMessage("<helloworld></helloworld>","application/foo+xml");
    }

    @Test
    void convertStringWithContentTypeApplicationJson() throws Exception
    {
        doTestTextMessage("[]","application/json");
    }

    @Test
    void convertStringWithContentTypeApplicationFooJson() throws Exception
    {
        doTestTextMessage("[]","application/foo+json");
    }

    @Test
    void convertStringWithContentTypeApplicationJavascript() throws Exception
    {
        doTestTextMessage("var foo","application/javascript");
    }

    @Test
    void convertStringWithContentTypeApplicationEcmascript() throws Exception
    {
        doTestTextMessage("var foo","application/ecmascript");
    }

    @Test
    void convertBytesMessageBody() throws Exception
    {
        doTestBytesMessage("helloworld".getBytes(), "application/octet-stream");
    }

    @Test
    void convertBytesMessageBodyNoContentType() throws Exception
    {
        final byte[] messageContent = "helloworld".getBytes();
        doTest(messageContent, null, DataSection.class, messageContent, null, null);
    }

    @Test
    void convertBytesMessageBodyUnknownContentType() throws Exception
    {
        final byte[] messageContent = "helloworld".getBytes();
        doTest(messageContent, "my/bytes", DataSection.class, messageContent, Symbol.valueOf("my/bytes"), null);
    }

    @Test
    void convertEmptyBytesMessageBody() throws Exception
    {
        doTestBytesMessage(new byte[0], "application/octet-stream");
    }

    @Test
    void convertJmsStreamMessageBody() throws Exception
    {
        final List<Object> expected = Lists.newArrayList("apple", 43, 31.42D);
        final byte[] messageBytes = getJmsStreamMessageBytes(expected);
        final String mimeType = "jms/stream-message";
        doTestStreamMessage(messageBytes, mimeType, expected, JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    void convertJmsStreamMessageEmptyBody() throws Exception
    {
        final List<Object> expected = List.of();
        doTestStreamMessage(null, "jms/stream-message", expected, JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    void convertAmqpListMessageBody() throws Exception
    {
        final List<Object> expected = Lists.newArrayList("apple", 43, 31.42D);
        final byte[] messageBytes = new ListToAmqpListConverter().toMimeContent(expected);

        final String mimeType = "amqp/list";
        doTestStreamMessage(messageBytes, mimeType, expected, JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @Test
    void convertAmqpListMessageBodyWithNonJmsContent() throws Exception
    {
        final List<Object> expected = Lists.newArrayList("apple", 43, 31.42D, Lists.newArrayList("nonJMSList"));
        final byte[] messageBytes = new ListToAmqpListConverter().toMimeContent(expected);
        final String mimeType = "amqp/list";
        doTestStreamMessage(messageBytes, mimeType, expected, null);
    }

    @Test
    void convertJmsMapMessageBody() throws Exception
    {
        final Map<String, Object> expected = Map.of("key", "value");
        final byte[] messageBytes = getJmsMapMessageBytes(expected);
        doTestMapMessage(messageBytes, "jms/map-message", expected, JmsMessageTypeAnnotation.MAP_MESSAGE.getType());
    }

    @Test
    void convertAmqpMapMessageBody() throws Exception
    {
        final Map<String, Object> expected = Map.of("key", "value");
        final byte[] messageBytes = new MapToAmqpMapConverter().toMimeContent(expected);
        doTestMapMessage(messageBytes, "amqp/map", expected, JmsMessageTypeAnnotation.MAP_MESSAGE.getType());
    }

    @Test
    void convertJmsMapMessageEmptyBody() throws Exception
    {
        final Map<String, Object> expected = Map.of();
        doTestMapMessage(null, "jms/map-message", expected, JmsMessageTypeAnnotation.MAP_MESSAGE.getType());
    }

    @Test
    void convertAmqpMapMessageBodyWithNonJmsContent() throws Exception
    {
        final Map<String, Object> expected = Map.of("key", List.of("nonJmsList"));
        final byte[] messageBytes = new MapToAmqpMapConverter().toMimeContent(expected);

        doTestMapMessage(messageBytes, "amqp/map", expected, null);
    }

    @Test
    void convertObjectStreamMessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());
        doTestObjectMessage(messageBytes, "application/java-object-stream", messageBytes);
    }

    @Test
    void convertObjectStream2MessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());
        doTestObjectMessage(messageBytes, "application/x-java-serialized-object", messageBytes);
    }

    @Test
    void convertEmptyObjectStreamMessageBody() throws Exception
    {
        final byte[] messageBytes = null;
        final byte[] expectedBytes = getObjectStreamMessageBytes(messageBytes);
        final String mimeType = "application/java-object-stream";
        doTestObjectMessage(messageBytes, mimeType, expectedBytes);
    }

    @Test
    void convertEmptyMessageWithoutContentType() throws Exception
    {
        doTest(null, null, AmqpValueSection.class, null, null, JmsMessageTypeAnnotation.MESSAGE.getType());
    }

    @Test
    void convertEmptyMessageWithUnknownContentType() throws Exception
    {
        doTest(null, "foo/bar", DataSection.class, new byte[0], Symbol.valueOf("foo/bar"), null);
    }

    @Test
    void convertMessageWithoutContentType() throws Exception
    {
        final byte[] expectedContent = "someContent".getBytes(UTF_8);
        doTest(expectedContent, null, DataSection.class, expectedContent, null, null);
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

    private byte[] getJmsStreamMessageBytes(final List<Object> objects) throws Exception
    {
        final TypedBytesContentWriter writer = new TypedBytesContentWriter();
        for (final Object object : objects)
        {
            writer.writeObject(object);
        }
        return getBytes(writer);
    }

    private byte[] getJmsMapMessageBytes(final Map<String, Object> map) throws Exception
    {
        final TypedBytesContentWriter writer = new TypedBytesContentWriter();
        writer.writeIntImpl(map.size());
        for (final Map.Entry<String, Object> entry : map.entrySet())
        {
            writer.writeNullTerminatedStringImpl(entry.getKey());
            writer.writeObject(entry.getValue());
        }
        return getBytes(writer);
    }

    private byte[] getBytes(final TypedBytesContentWriter writer)
    {
        final ByteBuffer buf = writer.getData();
        final byte[] expected = new byte[buf.remaining()];
        buf.get(expected);
        return expected;
    }

    private List<EncodingRetainingSection<?>> getEncodingRetainingSections(final QpidByteBuffer content,
                                                                           final int expectedNumberOfSections)
            throws Exception
    {
        final SectionDecoder sectionDecoder = new SectionDecoderImpl(_typeRegistry.getSectionDecoderRegistry());
        final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(content);
        assertEquals(expectedNumberOfSections, (long) sections.size(), "Unexpected number of sections");
        return sections;
    }

    private MessageTransferMessage getAmqMessage(final byte[] expected, final String mimeType)
    {
        configureMessageContent(expected);
        configureMessageHeader(mimeType);
        return new MessageTransferMessage(_handle, new Object());
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(_amqpHeader.getMimeType()).thenReturn(mimeType);
        _messageProperties.setContentType(mimeType);
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

        when(_handle.getContent(offsetCaptor.capture(), sizeCaptor.capture()))
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

    private void doTestTextMessage(final String originalContent, final String mimeType) throws Exception
    {
        final byte[] contentBytes = originalContent == null ? null : originalContent.getBytes(UTF_8);
        final String expectedContent = originalContent == null ? "" : originalContent;
        doTest(contentBytes,
               mimeType,
               AmqpValueSection.class,
               expectedContent,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.TEXT_MESSAGE.getType());
    }


    private void doTestMapMessage(final byte[] messageBytes,
                                  final String mimeType,
                                  final Map<String, Object> expected,
                                  final Byte expectedJmsTypeAnnotation) throws Exception
    {
        doTest(messageBytes, mimeType, AmqpValueSection.class, expected, null, expectedJmsTypeAnnotation);
    }

    private void doTestBytesMessage(final byte[] messageContent, final String mimeType) throws Exception
    {
        doTest(messageContent,
               mimeType,
               DataSection.class,
               messageContent,
               Symbol.valueOf(mimeType),
               JmsMessageTypeAnnotation.BYTES_MESSAGE.getType());
    }

    private void doTestStreamMessage(final byte[] messageBytes,
                                     final String mimeType,
                                     final List<Object> expected,
                                     final Byte expectedJmsTypAnnotation) throws Exception
    {
        doTest(messageBytes, mimeType, AmqpSequenceSection.class, expected, null, expectedJmsTypAnnotation);
    }

    private void doTestObjectMessage(final byte[] messageBytes, final String mimeType, final byte[] expectedBytes)
            throws Exception
    {
        doTest(messageBytes,
               mimeType,
               DataSection.class,
               expectedBytes,
               Symbol.valueOf("application/x-java-serialized-object"),
               JmsMessageTypeAnnotation.OBJECT_MESSAGE.getType());
    }

    private void doTest(final byte[] messageBytes,
                        final String mimeType,
                        final Class<? extends EncodingRetainingSection<?>> expectedBodySection,
                        final Object expectedContent,
                        final Symbol expectedContentType,
                        final Byte expectedJmsTypeAnnotation) throws Exception
    {
        final MessageTransferMessage sourceMessage = getAmqMessage(messageBytes, mimeType);
        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

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
}
