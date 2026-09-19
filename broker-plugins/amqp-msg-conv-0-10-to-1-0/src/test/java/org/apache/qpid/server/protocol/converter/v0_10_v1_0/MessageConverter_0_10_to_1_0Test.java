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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.AbstractDecoder;
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
import org.apache.qpid.server.util.GZIPUtils;
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
        final List<Object> expected = List.of("apple", 43, 31.42D);
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
        final List<Object> expected = List.of("apple", 43, 31.42D);
        final byte[] messageBytes = new ListToAmqpListConverter().toMimeContent(expected);

        final String mimeType = "amqp/list";
        doTestStreamMessage(messageBytes, mimeType, expected, JmsMessageTypeAnnotation.STREAM_MESSAGE.getType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"amqp/list", "amqp/map"})
    void testCompoundLimitRejectionUsesMessageConversionException(final String mimeType)
    {
        final List<Object> nested = List.of(List.of("value"));
        final byte[] content = "amqp/list".equals(mimeType) ?
                new ListToAmqpListConverter().toMimeContent(nested) :
                new MapToAmqpMapConverter().toMimeContent(Map.of("key", nested));
        final MessageTransferMessage sourceMessage = getAmqMessage(content, mimeType, 0, 1);
        assertTrue(sourceMessage.checkValid());

        final MessageConversionException exception = assertThrows(MessageConversionException.class, () ->
                _converter.convert(sourceMessage, mock(NamedAddressSpace.class)));

        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    }

    @Test
    void configuredLimitAcceptsDeeplyNestedAmqpListMessageBody() throws Exception
    {
        final int maxNestedObjects = AbstractDecoder.DEFAULT_MAX_NESTED_OBJECTS + 1;
        final List<Object> nested = createNestedList(maxNestedObjects);
        final byte[] messageBytes = new ListToAmqpListConverter().toMimeContent(nested);

        doTest(messageBytes, "amqp/list", AmqpSequenceSection.class, nested, null, null,
               AbstractDecoder.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, maxNestedObjects);
    }

    @Test
    void convertAmqpListMessageBodyWithNonJmsContent() throws Exception
    {
        final List<Object> expected = List.of("apple", 43, 31.42D, List.of("nonJMSList"));
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

    @Test
    void rejectGzipContentExceedingConnectionDecompressionLimit()
    {
        final byte[] data = new byte[8192];
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(data));
        final MessageTransferMessage sourceMessage = getAmqMessage(compressed, null);
        when(_amqpHeader.getEncoding()).thenReturn(GZIPUtils.GZIP_CONTENT_ENCODING);

        try
        {
            assertThrows(MessageConversionException.class, () ->
                    _converter.convert(sourceMessage, mock(NamedAddressSpace.class), 1024));
        }
        finally
        {
            when(_amqpHeader.getEncoding()).thenReturn(null);
        }
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

    private List<Object> createNestedList(final int depth)
    {
        List<Object> nested = List.of("leaf");
        for (int i = 1; i < depth; i++)
        {
            nested = List.of(nested);
        }
        return nested;
    }

    private List<EncodingRetainingSection<?>> getEncodingRetainingSections(final QpidByteBuffer content,
                                                                           final int expectedNumberOfSections,
                                                                           final int maxNestedObjects)
            throws Exception
    {
        final SectionDecoder sectionDecoder =
                new SectionDecoderImpl(_typeRegistry.getSectionDecoderRegistry(), maxNestedObjects);
        final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(content);
        assertEquals(expectedNumberOfSections, (long) sections.size(), "Unexpected number of sections");
        return sections;
    }

    private MessageTransferMessage getAmqMessage(final byte[] expected, final String mimeType)
    {
        return getAmqMessage(expected, mimeType, AbstractDecoder.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS,
                AbstractDecoder.DEFAULT_MAX_NESTED_OBJECTS);
    }

    private MessageTransferMessage getAmqMessage(final byte[] expected,
                                                 final String mimeType,
                                                 final int maxZeroWidthArrayElements,
                                                 final int maxNestedObjects)
    {
        configureMessageContent(expected);
        configureMessageHeader(mimeType);
        return new MessageTransferMessage(_handle, new Object(), maxZeroWidthArrayElements, maxNestedObjects);
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
        doTest(messageBytes,
               mimeType,
               expectedBodySection,
               expectedContent,
               expectedContentType,
               expectedJmsTypeAnnotation,
               AbstractDecoder.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS,
               AbstractDecoder.DEFAULT_MAX_NESTED_OBJECTS);
    }

    private void doTest(final byte[] messageBytes,
                        final String mimeType,
                        final Class<? extends EncodingRetainingSection<?>> expectedBodySection,
                        final Object expectedContent,
                        final Symbol expectedContentType,
                        final Byte expectedJmsTypeAnnotation,
                        final int maxZeroWidthArrayElements,
                        final int maxNestedObjects) throws Exception
    {
        final MessageTransferMessage sourceMessage =
                getAmqMessage(messageBytes, mimeType, maxZeroWidthArrayElements, maxNestedObjects);
        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        final List<EncodingRetainingSection<?>> sections =
                getEncodingRetainingSections(content, 1, maxNestedObjects);
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
