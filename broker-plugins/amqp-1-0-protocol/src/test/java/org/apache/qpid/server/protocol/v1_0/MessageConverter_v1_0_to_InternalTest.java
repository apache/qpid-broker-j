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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequence;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Footer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class MessageConverter_v1_0_to_InternalTest extends UnitTestBase
{
    private static final MessageAnnotations MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 0));
    private static final MessageAnnotations OBJECT_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 1));
    private static final MessageAnnotations MAP_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 2));
    private static final MessageAnnotations BYTE_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 3));
    private static final MessageAnnotations STREAM_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 4));
    private static final MessageAnnotations TEXT_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 5));
    private MessageConverter_v1_0_to_Internal _converter;

    @BeforeAll
    void setUp()
    {
        _converter = new MessageConverter_v1_0_to_Internal();
    }

    @Test
    void amqpValueWithNullWithTextMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
    }

    @Test
    void amqpValueWithNullWithMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNullWithObjectMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/x-java-serialized-object", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNullWithMapMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNullWithBytesMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/octet-stream", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNullWithStreamMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNullWithUnknownMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 11)),
                        amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNullWithContentType()
    {
        final Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithNull()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithString()
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertEquals(expected, convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithStringWithKnownTextualContentType()
    {
        final Properties properties = new Properties();
        final String mimeType = "text/foo";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = "content";
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpValueWithStringWithUnknownTextualContentType()
    {
        final Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = "content";
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, convertedMessage.getMessageBody(), "Unexpected content");
    }


    @Test
    void amqpValueWithMap()
    {
        final Map<Object, Object> originalMap = new LinkedHashMap<>();
        originalMap.put("binaryEntry", new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalMap.put("intEntry", 42);
        originalMap.put("uuidEntry", UUID.randomUUID());
        originalMap.put("nullEntry", null);
        originalMap.put(43, "nonstringkey");
        originalMap.put("mapEntry", Map.of("foo", "bar"));
        final AmqpValue amqpValue = new AmqpValue(originalMap);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");

        final Map<Object, Object> convertedMap = (Map<Object, Object>) convertedMessage.getMessageBody();

        assertEquals(originalMap.size(), (long) convertedMap.size(), "Unexpected size");
        assertArrayEquals(((Binary) originalMap.get("binaryEntry")).getArray(), (byte[]) convertedMap.get("binaryEntry"),
                "Unexpected binary entry");
        assertEquals(originalMap.get("intEntry"), convertedMap.get("intEntry"), "Unexpected int entry");
        assertEquals(originalMap.get("nullEntry"), convertedMap.get("nullEntry"), "Unexpected null entry");
        assertEquals(originalMap.get("uuidEntry"), convertedMap.get("uuidEntry"), "Unexpected uuid entry");
        assertEquals(originalMap.get(43), convertedMap.get(43), "Unexpected nonstringkey entry");
        assertEquals(new HashMap((Map) originalMap.get("mapEntry")), new HashMap((Map) convertedMap.get("mapEntry")),
                "Unexpected map entry");
    }

    @Test
    void amqpValueWithList()
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalList.add(42);
        originalList.add(null);
        originalList.add(Map.of("foo", "bar"));
        final AmqpValue amqpValue = new AmqpValue(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");

        final List<Object> convertedList = ((List<Object>) convertedMessage.getMessageBody());
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertArrayEquals(((Binary) originalList.get(0)).getArray(), (byte[]) convertedList.get(0),
                "Unexpected binary item");
        assertEquals(originalList.get(1), convertedList.get(1), "Unexpected int item");
        assertEquals(originalList.get(2), convertedList.get(2), "Unexpected null item");
        assertEquals(new HashMap((Map) originalList.get(3)), new HashMap((Map) convertedList.get(3)),
                "Unexpected map item");
    }

    @Test
    void amqpValueWithAmqpType()
    {
        final Date originalValue = new Date();
        final AmqpValue amqpValue = new AmqpValue(originalValue);
        final Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(originalValue, convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void amqpSequenceWithSimpleTypes()
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(37);
        originalList.add(42F);
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        final List<Object> convertedList = ((List<Object>) convertedMessage.getMessageBody());
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertEquals(originalList.get(0), convertedList.get(0), "Unexpected first item");
        assertEquals(originalList.get(1), convertedList.get(1), "Unexpected second item");
    }

    @Test
    void dataWithMessageAnnotation()
    {
        final byte[] data = "helloworld".getBytes(UTF_8);
        doTestDataWithAnnotation(data, MESSAGE_MESSAGE_ANNOTATION, null, "application/octet-stream");
    }

    @Test
    void dataWithMessageAnnotationWithContentType()
    {
        final byte[] data = "helloworld".getBytes(UTF_8);
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation(data, MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    void dataWithObjectMessageAnnotation() throws Exception
    {
        final byte[] bytes = "helloworld".getBytes(UTF_8);
        final byte[] expected = getObjectBytes(bytes);
        doTestDataWithAnnotation(expected, OBJECT_MESSAGE_MESSAGE_ANNOTATION, null, "application/x-java-serialized-object");
    }

    @Test
    void dataWithObjectMessageAnnotationWithContentType() throws Exception
    {
        final byte[] bytes = "helloworld".getBytes(UTF_8);
        final byte[] expected = getObjectBytes(bytes);
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation(expected, OBJECT_MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    void dataWithMapMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), MAP_MESSAGE_MESSAGE_ANNOTATION, null,
                "application/octet-stream");
    }

    @Test
    void dataWithMapMessageAnnotationWithContentType()
    {
        final String mimeType = "foor/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), MAP_MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    void dataWithBytesMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), BYTE_MESSAGE_MESSAGE_ANNOTATION, null,
                "application/octet-stream");
    }

    @Test
    void dataWithBytesMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), BYTE_MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    void dataWithStreamMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION, null,
                "application/octet-stream");
    }

    @Test
    void dataWithStreamMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    void dataWithTextMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, null,
                "application/octet-stream");
    }

    @Test
    void dataWithTextMessageAnnotationWithContentType()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, "foo/bar", "foo/bar");
    }

    @Test
    void dataWithUnsupportedMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 11)), null, "application/octet-stream");
    }

    @Test
    void dataWithUnsupportedMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 11)), mimeType, mimeType);
    }

    @Test
    void data() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        final Message_1_0 sourceMessage = createTestMessage(value.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/octet-stream", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertArrayEquals(expected, ((byte[]) convertedMessage.getMessageBody()), "Unexpected content");
    }

    @Test
    void noBodyWithMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(MESSAGE_MESSAGE_ANNOTATION, null);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithObjectMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/x-java-serialized-object", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");

        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithMapMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, null);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithBytesMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, null);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/octet-stream", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithStreamMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, null);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithTextMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, null);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithTextMessageAnnotationWithKnownTextualContentType()
    {
        final String mimeType = "text/foo";
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));

        final Message_1_0 sourceMessage = createTestMessage(properties, TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithTextMessageAnnotationWithUnknownTextualContentType()
    {
        final String mimeType = "foo/bar";
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        final Message_1_0 sourceMessage = createTestMessage(properties, TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }


    @Test
    void noBodyWithUnknownMessageAnnotation()
    {
        final Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY, (byte) 11)), null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithUnknownMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        final Message_1_0 sourceMessage =
                createTestMessage(properties, new MessageAnnotations(Map.of(Symbols.ANNOTATION_KEY,
                                                                                  (byte) 11)), null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBody()
    {
        final Message_1_0 sourceMessage = createTestMessage(null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void noBodyWithContentTypeApplicationOctetStream()
    {
        final String mimeType = "foo/bar";
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    void messageAnnotationTakesPrecedenceOverContentType()
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/octet-stream"));
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/x-java-serialized-object", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    private Message_1_0 createTestMessage(final EncodingRetainingSection<?> encodingRetainingSection)
    {
        return createTestMessage(new Properties(), encodingRetainingSection);
    }

    private Message_1_0 createTestMessage(final Properties properties, final EncodingRetainingSection<?> section)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Map.of()),
                                 new MessageAnnotations(Map.of()),
                                 properties,
                                 new ApplicationProperties(Map.of()),
                                 0,
                                 section);
    }

    private Message_1_0 createTestMessage(final Properties properties,
                                          final MessageAnnotations messageAnnotations,
                                          final EncodingRetainingSection<?> section)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Map.of()),
                                 messageAnnotations,
                                 properties,
                                 new ApplicationProperties(Map.of()),
                                 0,
                                 section);
    }

    private Message_1_0 createTestMessage(final MessageAnnotations messageAnnotations,
                                          final EncodingRetainingSection<?> section)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Map.of()),
                                 messageAnnotations,
                                 new Properties(),
                                 new ApplicationProperties(Map.of()),
                                 0,
                                 section);
    }

    private Message_1_0 createTestMessage(final Header header,
                                          final DeliveryAnnotations deliveryAnnotations,
                                          final MessageAnnotations messageAnnotations,
                                          final Properties properties,
                                          final ApplicationProperties applicationProperties,
                                          final long arrivalTime,
                                          final EncodingRetainingSection<?> section)
    {
        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        final MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                deliveryAnnotations.createEncodingRetainingSection(),
                messageAnnotations.createEncodingRetainingSection(),
                properties.createEncodingRetainingSection(),
                applicationProperties.createEncodingRetainingSection(),
                new Footer(Map.of()).createEncodingRetainingSection(),
                arrivalTime,
                0);
        when(storedMessage.getMetaData()).thenReturn(metaData);

        if (section != null)
        {
            // TODO this is leaking QBBs
            final QpidByteBuffer combined = section.getEncodedForm();
            when(storedMessage.getContentSize()).thenReturn((int) section.getEncodedSize());
            final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
            final ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);

            when(storedMessage.getContent(offsetCaptor.capture(), sizeCaptor.capture()))
                    .then(invocation -> combined.view(offsetCaptor.getValue(), sizeCaptor.getValue()));
        }
        else
        {
            when(storedMessage.getContent(0,0)).thenReturn(QpidByteBuffer.emptyQpidByteBuffer());
        }
        return new Message_1_0(storedMessage);
    }

    private byte[] getObjectBytes(final Object object) throws IOException
    {
        final byte[] expected;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(object);
            expected = baos.toByteArray();
        }
        return expected;
    }

    private void doTestDataWithAnnotation(final byte[] data,
                                          final MessageAnnotations messageAnnotations,
                                          final String mimeType, final String expectedMimeType)
    {

        final Data value = new Data(new Binary(data));

        final Message_1_0 sourceMessage;
        if (mimeType != null)
        {
            final Properties properties = new Properties();
            properties.setContentType(Symbol.valueOf(mimeType));
            sourceMessage = createTestMessage(properties, messageAnnotations, value.createEncodingRetainingSection());
        }
        else
        {
            sourceMessage = createTestMessage(messageAnnotations, value.createEncodingRetainingSection());
        }

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(expectedMimeType, convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertArrayEquals(data, ((byte[]) convertedMessage.getMessageBody()), "Unexpected content");
    }
}