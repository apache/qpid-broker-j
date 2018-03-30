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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

import org.junit.Before;
import org.junit.Test;
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

    @Before
    public void setUp() throws Exception
    {
        _converter = new MessageConverter_v1_0_to_Internal();
    }

    @Test
    public void testAmqpValueWithNullWithTextMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull("Unexpected content", convertedMessage.getMessageBody());
        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
    }

    @Test
    public void testAmqpValueWithNullWithMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithNullWithObjectMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/x-java-serialized-object",
                            convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithNullWithMapMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithNullWithBytesMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithNullWithStreamMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithNullWithUnknownMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)),
                                  amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithNullWithContentType() throws Exception
    {
        Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", mimeType, convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }


    @Test
    public void testAmqpValueWithNull() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertNull("Unexpected content", convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithString() throws Exception
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", expected, convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithStringWithKnownTextualContentType() throws Exception
    {
        Properties properties = new Properties();
        final String mimeType = "text/foo";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = "content";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", mimeType, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", expected, convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpValueWithStringWithUnknownTextualContentType() throws Exception
    {
        Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        final Object expected = "content";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", expected, convertedMessage.getMessageBody());
    }


    @Test
    public void testAmqpValueWithMap() throws Exception
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

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());

        Map<Object, Object> convertedMap = (Map<Object, Object>) convertedMessage.getMessageBody();

        assertEquals("Unexpected size", (long) originalMap.size(), (long) convertedMap.size());
        assertArrayEquals("Unexpected binary entry", ((Binary) originalMap.get("binaryEntry")).getArray(),
                          (byte[]) convertedMap.get("binaryEntry"));
        assertEquals("Unexpected int entry", originalMap.get("intEntry"), convertedMap.get("intEntry"));
        assertEquals("Unexpected null entry", originalMap.get("nullEntry"), convertedMap.get("nullEntry"));
        assertEquals("Unexpected uuid entry", originalMap.get("uuidEntry"), convertedMap.get("uuidEntry"));
        assertEquals("Unexpected nonstringkey entry", originalMap.get(43), convertedMap.get(43));
        assertEquals("Unexpected map entry",
                            new HashMap((Map) originalMap.get("mapEntry")),
                            new HashMap((Map) convertedMap.get("mapEntry")));
    }

    @Test
    public void testAmqpValueWithList() throws Exception
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalList.add(42);
        originalList.add(null);
        originalList.add(Collections.singletonMap("foo", "bar"));
        final AmqpValue amqpValue = new AmqpValue(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());

        List<Object> convertedList = ((List<Object>) convertedMessage.getMessageBody());
        assertEquals("Unexpected size", (long) originalList.size(), (long) convertedList.size());
        assertArrayEquals("Unexpected binary item", ((Binary) originalList.get(0)).getArray(),
                          (byte[]) convertedList.get(0));
        assertEquals("Unexpected int item", originalList.get(1), convertedList.get(1));
        assertEquals("Unexpected null item", originalList.get(2), convertedList.get(2));
        assertEquals("Unexpected map item",
                            new HashMap((Map) originalList.get(3)),
                            new HashMap((Map) convertedList.get(3)));
    }


    @Test
    public void testAmqpValueWithAmqpType() throws Exception
    {
        final Date originalValue = new Date();
        final AmqpValue amqpValue = new AmqpValue(originalValue);
        Properties properties = new Properties();
        final String mimeType = "foo/bar";
        properties.setContentType(Symbol.valueOf(mimeType));
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", mimeType, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", originalValue, convertedMessage.getMessageBody());
    }

    @Test
    public void testAmqpSequenceWithSimpleTypes() throws Exception
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(37);
        originalList.add(42F);
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        List<Object> convertedList = ((List<Object>) convertedMessage.getMessageBody());
        assertEquals("Unexpected size", (long) originalList.size(), (long) convertedList.size());
        assertEquals("Unexpected first item", originalList.get(0), convertedList.get(0));
        assertEquals("Unexpected second item", originalList.get(1), convertedList.get(1));
    }

    @Test
    public void testDataWithMessageAnnotation() throws Exception
    {
        final byte[] data = "helloworld".getBytes(UTF_8);
        doTestDataWithAnnotation(data, MESSAGE_MESSAGE_ANNOTATION, null, "application/octet-stream");

    }

    @Test
    public void testDataWithMessageAnnotationWithContentType() throws Exception
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
    public void testDataWithMapMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 MAP_MESSAGE_MESSAGE_ANNOTATION,
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithMapMessageAnnotationWithContentType() throws Exception
    {
        final String mimeType = "foor/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 MAP_MESSAGE_MESSAGE_ANNOTATION,
                                 mimeType, mimeType);
    }

    @Test
    public void testDataWithBytesMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 BYTE_MESSAGE_MESSAGE_ANNOTATION,
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithBytesMessageAnnotationWithContentType() throws Exception
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 BYTE_MESSAGE_MESSAGE_ANNOTATION,
                                 mimeType, mimeType);
    }

    @Test
    public void testDataWithStreamMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithStreamMessageAnnotationWithContentType() throws Exception
    {
        final String mimeType = "foo/bar";
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                 mimeType, mimeType);
    }

    @Test
    public void testDataWithTextMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, null, "application/octet-stream");
    }

    @Test
    public void testDataWithTextMessageAnnotationWithContentType() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, "foo/bar", "foo/bar");
    }

    @Test
    public void testDataWithUnsupportedMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                 (byte) 11)),
                                 null, "application/octet-stream");
    }

    @Test
    public void testDataWithUnsupportedMessageAnnotationWithContentType() throws Exception
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

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertArrayEquals("Unexpected content", expected, ((byte[]) convertedMessage.getMessageBody()));
    }

    @Test
    public void testNoBodyWithMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithObjectMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/x-java-serialized-object",
                            convertedMessage.getMessageHeader().getMimeType());

        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithMapMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithBytesMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithStreamMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithTextMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithTextMessageAnnotationWithKnownTextualContentType() throws Exception
    {
        final String mimeType = "text/foo";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));

        Message_1_0 sourceMessage = createTestMessage(properties, TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", mimeType, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithTextMessageAnnotationWithUnknownTextualContentType() throws Exception
    {
        final String mimeType = "foo/bar";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        Message_1_0 sourceMessage = createTestMessage(properties, TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }


    @Test
    public void testNoBodyWithUnknownMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)), null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithUnknownMessageAnnotationWithContentType() throws Exception
    {

        final String mimeType = "foo/bar";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        Message_1_0 sourceMessage =
                createTestMessage(properties, new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)), null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", mimeType, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBody() throws Exception
    {
        final Message_1_0 sourceMessage = createTestMessage(null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testNoBodyWithContentTypeApplicationOctetStream() throws Exception
    {
        final String mimeType = "foo/bar";
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(mimeType));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", mimeType, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
    }

    @Test
    public void testMessageAnnotationTakesPrecedenceOverContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/octet-stream"));
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/x-java-serialized-object",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content", null, convertedMessage.getMessageBody());
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

        assertEquals("Unexpected mime type", expectedMimeType, convertedMessage.getMessageHeader().getMimeType());
        assertArrayEquals("Unexpected content", data, ((byte[]) convertedMessage.getMessageBody()));

    }

}