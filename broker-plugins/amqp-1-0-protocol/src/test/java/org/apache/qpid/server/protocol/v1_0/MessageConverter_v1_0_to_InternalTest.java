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
import java.util.Collections;
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

public class MessageConverter_v1_0_to_InternalTest extends UnitTestBase
{
    private static final MessageAnnotations MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 0));
    private static final MessageAnnotations OBJECT_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 1));
    private static final MessageAnnotations MAP_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 2));
    private static final MessageAnnotations BYTE_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 3));
    private static final MessageAnnotations STREAM_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 4));
    private static final MessageAnnotations TEXT_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 5));
    private MessageConverter_v1_0_to_Internal _converter;

    @BeforeAll
    public void setUp() throws Exception
    {
        _converter = new MessageConverter_v1_0_to_Internal();
    }

    @Test
    public void testAmqpValueWithNullWithTextMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
    }

    @Test
    public void testAmqpValueWithNullWithMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNullWithObjectMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/x-java-serialized-object", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNullWithMapMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNullWithBytesMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/octet-stream", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNullWithStreamMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNullWithUnknownMessageAnnotation()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)),
                                  amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNullWithContentType()
    {
        Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithNull()
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithString()
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertEquals(expected, convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithStringWithKnownTextualContentType()
    {
        Properties properties = new Properties();
        final String mimeType = "text/foo";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = "content";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpValueWithStringWithUnknownTextualContentType()
    {
        Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = "content";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, convertedMessage.getMessageBody(), "Unexpected content");
    }


    @Test
    public void testAmqpValueWithMap()
    {
        final Map<Object, Object> originalMap = new LinkedHashMap<>();
        originalMap.put("binaryEntry", new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalMap.put("intEntry", 42);
        originalMap.put("uuidEntry", UUID.randomUUID());
        originalMap.put("nullEntry", null);
        originalMap.put(43, "nonstringkey");
        originalMap.put("mapEntry", Collections.singletonMap("foo", "bar"));
        final AmqpValue amqpValue = new AmqpValue(originalMap);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");

        Map<Object, Object> convertedMap = (Map<Object, Object>) convertedMessage.getMessageBody();

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
    public void testAmqpValueWithList()
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalList.add(42);
        originalList.add(null);
        originalList.add(Collections.singletonMap("foo", "bar"));
        final AmqpValue amqpValue = new AmqpValue(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");

        List<Object> convertedList = ((List<Object>) convertedMessage.getMessageBody());
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertArrayEquals(((Binary) originalList.get(0)).getArray(), (byte[]) convertedList.get(0),
                "Unexpected binary item");
        assertEquals(originalList.get(1), convertedList.get(1), "Unexpected int item");
        assertEquals(originalList.get(2), convertedList.get(2), "Unexpected null item");
        assertEquals(new HashMap((Map) originalList.get(3)), new HashMap((Map) convertedList.get(3)),
                "Unexpected map item");
    }


    @Test
    public void testAmqpValueWithAmqpType()
    {
        final Date originalValue = new Date();
        final AmqpValue amqpValue = new AmqpValue(originalValue);
        Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(originalValue, convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testAmqpSequenceWithSimpleTypes()
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(37);
        originalList.add(42F);
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        List<Object> convertedList = ((List<Object>) convertedMessage.getMessageBody());
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertEquals(originalList.get(0), convertedList.get(0), "Unexpected first item");
        assertEquals(originalList.get(1), convertedList.get(1), "Unexpected second item");
    }

    @Test
    public void testDataWithMessageAnnotation()
    {
        final byte[] data = "helloworld".getBytes(UTF_8);
        doTestDataWithAnnotation(data, MESSAGE_MESSAGE_ANNOTATION, null, "application/octet-stream");

    }

    @Test
    public void testDataWithMessageAnnotationWithContentType()
    {
        final byte[] data = "helloworld".getBytes(UTF_8);
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation(data, MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    public void testDataWithObjectMessageAnnotation() throws Exception
    {
        byte[] bytes = "helloworld".getBytes(UTF_8);
        final byte[] expected = getObjectBytes(bytes);
        doTestDataWithAnnotation(expected, OBJECT_MESSAGE_MESSAGE_ANNOTATION,
                                 null,
                                 "application/x-java-serialized-object");
    }

    @Test
    public void testDataWithObjectMessageAnnotationWithContentType() throws Exception
    {
        byte[] bytes = "helloworld".getBytes(UTF_8);
        final byte[] expected = getObjectBytes(bytes);
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation(expected, OBJECT_MESSAGE_MESSAGE_ANNOTATION, mimeType, mimeType);
    }

    @Test
    public void testDataWithMapMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 MAP_MESSAGE_MESSAGE_ANNOTATION,
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithMapMessageAnnotationWithContentType()
    {
        final String mimeType = "foor/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 MAP_MESSAGE_MESSAGE_ANNOTATION,
                                 mimeType, mimeType);
    }

    @Test
    public void testDataWithBytesMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 BYTE_MESSAGE_MESSAGE_ANNOTATION,
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithBytesMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 BYTE_MESSAGE_MESSAGE_ANNOTATION,
                                 mimeType, mimeType);
    }

    @Test
    public void testDataWithStreamMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithStreamMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                 mimeType, mimeType);
    }

    @Test
    public void testDataWithTextMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, null, "application/octet-stream");
    }

    @Test
    public void testDataWithTextMessageAnnotationWithContentType()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, "foo/bar", "foo/bar");
    }

    @Test
    public void testDataWithUnsupportedMessageAnnotation()
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                 (byte) 11)),
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithUnsupportedMessageAnnotationWithContentType()
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                 (byte) 11)),
                                 mimeType, mimeType);
    }

    @Test
    public void testData() throws Exception
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
    public void testNoBodyWithMessageAnnotation()
    {
        Message_1_0 sourceMessage = createTestMessage(MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithObjectMessageAnnotation()
    {
        Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/x-java-serialized-object", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");

        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithMapMessageAnnotation()
    {
        Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithBytesMessageAnnotation()
    {
        Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/octet-stream", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithStreamMessageAnnotation()
    {
        Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithTextMessageAnnotation()
    {
        Message_1_0 sourceMessage = createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithTextMessageAnnotationWithKnownTextualContentType()
    {
        final String mimeType = "text/foo";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));

        Message_1_0 sourceMessage = createTestMessage(properties, TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithTextMessageAnnotationWithUnknownTextualContentType()
    {
        final String mimeType = "foo/bar";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        Message_1_0 sourceMessage = createTestMessage(properties, TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }


    @Test
    public void testNoBodyWithUnknownMessageAnnotation()
    {
        Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)), null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithUnknownMessageAnnotationWithContentType()
    {

        final String mimeType = "foo/bar";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        Message_1_0 sourceMessage =
                createTestMessage(properties, new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)), null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBody()
    {
        final Message_1_0 sourceMessage = createTestMessage(null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testNoBodyWithContentTypeApplicationOctetStream()
    {
        final String mimeType = "foo/bar";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(mimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    @Test
    public void testMessageAnnotationTakesPrecedenceOverContentType()
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/octet-stream"));
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("application/x-java-serialized-object", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        assertNull(convertedMessage.getMessageBody(), "Unexpected content");
    }

    private Message_1_0 createTestMessage(final EncodingRetainingSection encodingRetainingSection)
    {
        return createTestMessage(new Properties(), encodingRetainingSection);
    }

    private Message_1_0 createTestMessage(final Properties properties, final EncodingRetainingSection section)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 new MessageAnnotations(Collections.emptyMap()),
                                 properties,
                                 new ApplicationProperties(Collections.emptyMap()),
                                 0,
                                 section);
    }

    private Message_1_0 createTestMessage(final Properties properties,
                                          final MessageAnnotations messageAnnotations,
                                          final EncodingRetainingSection section)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 messageAnnotations,
                                 properties,
                                 new ApplicationProperties(Collections.emptyMap()),
                                 0,
                                 section);
    }

    private Message_1_0 createTestMessage(final MessageAnnotations messageAnnotations,
                                          final EncodingRetainingSection section)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 messageAnnotations,
                                 new Properties(),
                                 new ApplicationProperties(Collections.emptyMap()),
                                 0,
                                 section);
    }

    private Message_1_0 createTestMessage(final Header header,
                                          final DeliveryAnnotations deliveryAnnotations,
                                          final MessageAnnotations messageAnnotations,
                                          final Properties properties,
                                          final ApplicationProperties applicationProperties,
                                          final long arrivalTime,
                                          final EncodingRetainingSection section)
    {
        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                                                               deliveryAnnotations.createEncodingRetainingSection(),
                                                               messageAnnotations.createEncodingRetainingSection(),
                                                               properties.createEncodingRetainingSection(),
                                                               applicationProperties.createEncodingRetainingSection(),
                                                               new Footer(Collections.emptyMap()).createEncodingRetainingSection(),
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

            when(storedMessage.getContent(offsetCaptor.capture(),
                                          sizeCaptor.capture())).then(invocation -> combined.view(offsetCaptor.getValue(),
                                                                                                  sizeCaptor.getValue()));
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
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos))
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

        Message_1_0 sourceMessage;
        if (mimeType != null)
        {
            Properties properties = new Properties();
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