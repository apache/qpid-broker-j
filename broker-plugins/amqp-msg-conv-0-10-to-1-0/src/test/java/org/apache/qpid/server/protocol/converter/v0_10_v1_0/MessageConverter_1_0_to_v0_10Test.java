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
package org.apache.qpid.server.protocol.converter.v0_10_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.AmqpListToListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.AmqpMapToMapConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.ListToAmqpListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.MapToAmqpMapConverter;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.typedmessage.mimecontentconverter.JmsMapMessageToMap;
import org.apache.qpid.server.typedmessage.mimecontentconverter.JmsStreamMessageToList;
import org.apache.qpid.server.typedmessage.mimecontentconverter.ListToJmsStreamMessage;
import org.apache.qpid.server.typedmessage.mimecontentconverter.MapToJmsMapMessage;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class MessageConverter_1_0_to_v0_10Test extends UnitTestBase
{
    private static final MessageAnnotations MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 0));
    private static final MessageAnnotations OBJECT_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 1));
    private static final MessageAnnotations MAP_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 2));
    private static final MessageAnnotations BYTE_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 3));
    private static final MessageAnnotations STREAM_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 4));
    private static final MessageAnnotations TEXT_MESSAGE_MESSAGE_ANNOTATION =
            new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 5));
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final String OBJECT_STREAM = "application/java-object-stream";

    private MessageConverter_1_0_to_v0_10 _converter;

    @BeforeAll
    void setUp()
    {
        _converter = new MessageConverter_1_0_to_v0_10();
    }

    @Test
    void amqpValueWithNullWithTextMessageAnnotation()
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithMessageAnnotation()
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage =
                createTestMessage(MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithObjectMessageAnnotation() throws Exception
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage =
                createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(getObjectBytes(null), getBytes(content), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithMapMessageAnnotation() throws Exception
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage =
                createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(new MapToJmsMapMessage().toMimeContent(Map.of()), getBytes(content), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithBytesMessageAnnotation()
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage =
                createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OCTET_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithStreamMessageAnnotation()
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage =
                createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("jms/stream-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithUnknownMessageAnnotation()
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final MessageAnnotations messageAnnotations =
                new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 11));
        final Message_1_0 sourceMessage =
                createTestMessage(messageAnnotations, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithContentTypeApplicationOctetStream()
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(OCTET_STREAM));
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OCTET_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithObjectMessageContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(getObjectBytes(null).length, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithNullWithJmsMapContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/map-message"));
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(new MapToJmsMapMessage().toMimeContent(Map.of()), getBytes(content), "Unexpected content size");
    }

    @Test
    void amqpValueWithNull()
    {
        final AmqpValue amqpValue = new AmqpValue(null);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void amqpValueWithString() throws Exception
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, new String(getBytes(content), UTF_8), "Unexpected content");
    }

    @Test
    void amqpValueWithStringWithTextMessageAnnotation() throws Exception
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        final Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, new String(getBytes(content), UTF_8), "Unexpected content");
    }

    @Test
    void amqpValueWithMap() throws Exception
    {
        final Map<String, Object> originalMap = new LinkedHashMap<>();
        originalMap.put("binaryEntry", new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalMap.put("intEntry", 42);
        originalMap.put("nullEntry", null);
        final AmqpValue amqpValue = new AmqpValue(originalMap);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        final Map<String, Object> convertedMap = new JmsMapMessageToMap().toObject(getBytes(content));
        assertEquals(originalMap.size(), (long) convertedMap.size(), "Unexpected size");
        assertArrayEquals(((Binary) originalMap.get("binaryEntry")).getArray(),
                (byte[]) convertedMap.get("binaryEntry"), "Unexpected binary entry");
        assertEquals(originalMap.get("intEntry"), convertedMap.get("intEntry"), "Unexpected int entry");
        assertEquals(originalMap.get("nullEntry"), convertedMap.get("nullEntry"), "Unexpected null entry");
    }

    @Test
    void amqpValueWithMapContainingMap() throws Exception
    {
        final Map<String, Object> originalMap = Map.of("testMap", Map.of("innerKey", "testValue"));
        final AmqpValue amqpValue = new AmqpValue(originalMap);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("amqp/map", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        final Map<String, Object> convertedMap = new AmqpMapToMapConverter().toObject(getBytes(content));
        assertEquals(originalMap.size(), (long) convertedMap.size(), "Unexpected size");
        assertEquals(new HashMap((Map<String, Object>) originalMap.get("testMap")),
                new HashMap((Map<String, Object>) convertedMap.get("testMap")),
                "Unexpected binary entry");
    }

    @Test
    void amqpValueWithMapContainingNonFieldTableCompliantEntries()
    {
        final AmqpValue amqpValue = new AmqpValue(Map.of(13, 42));
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        assertThrows(MessageConversionException.class,
                () -> _converter.convert(sourceMessage, mock(NamedAddressSpace.class)),
                "expected exception not thrown");
    }

    @Test
    void amqpValueWithList() throws Exception
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalList.add(42);
        originalList.add(null);
        final AmqpValue amqpValue = new AmqpValue(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("jms/stream-message", convertedMessage.getMessageHeader().getMimeType(),
                "Unexpected mime type");
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        final List<Object> convertedList = new JmsStreamMessageToList().toObject(getBytes(content));
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertArrayEquals(((Binary) originalList.get(0)).getArray(), (byte[]) convertedList.get(0),
                "Unexpected binary item");
        assertEquals(originalList.get(1), convertedList.get(1), "Unexpected int item");
        assertEquals(originalList.get(2), convertedList.get(2), "Unexpected null item");
    }

    @Test
    void amqpValueWithListContainingMap() throws Exception
    {
        final List<Object> originalList = List.of(Map.of("testKey", "testValue"));
        final AmqpValue amqpValue = new AmqpValue(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("amqp/list", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        final List<Object> convertedList = new AmqpListToListConverter().toObject(getBytes(content));
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertEquals(new HashMap<>((Map) originalList.get(0)),
                new HashMap<>((Map) convertedList.get(0)),
                "Unexpected map item");
    }

    @Test
    void amqpValueWithListContainingUnsupportedType()
    {
        final List<Object> originalList = List.of(new Source());
        final AmqpValue amqpValue = new AmqpValue(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        assertThrows(MessageConversionException.class,
                () -> _converter.convert(sourceMessage, mock(NamedAddressSpace.class)),
                "Expected exception not thrown");
    }

    @Test
    void amqpValueWithUnsupportedType()
    {
        final Integer originalValue = 42;
        final AmqpValue amqpValue = new AmqpValue(originalValue);
        final Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        assertThrows(MessageConversionException.class,
                () -> _converter.convert(sourceMessage, mock(NamedAddressSpace.class)),
                "Expected exception not thrown");
    }

    @Test
    void amqpSequenceWithSimpleTypes() throws Exception
    {
        final List<Integer> expected = List.of(37, 42);
        final AmqpSequence amqpSequence = new AmqpSequence(expected);
        final Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/stream-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(expected, new JmsStreamMessageToList().toObject(getBytes(content)), "Unexpected content");
    }

    @Test
    void amqpSequenceWithMap() throws Exception
    {
        final List<Object> originalList = List.of(Map.of("testKey", "testValue"));
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("amqp/list", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        final List<Object> convertedList = new AmqpListToListConverter().toObject(getBytes(content));
        assertEquals(originalList.size(), (long) convertedList.size(), "Unexpected size");
        assertEquals(new HashMap<>((Map) originalList.get(0)),
                new HashMap<>((Map) convertedList.get(0)),
                "Unexpected map item");
    }

    @Test
    void amqpSequenceWithUnsupportedType()
    {
        final List<Object> originalList = List.of(new Source());
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        final Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        assertThrows(MessageConversionException.class,
                () -> _converter.convert(sourceMessage, mock(NamedAddressSpace.class)),
                "Expected exception not thrown");
    }

    @Test
    void dataWithMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), MESSAGE_MESSAGE_ANNOTATION, OCTET_STREAM);
    }

    @Test
    void dataWithObjectMessageAnnotation() throws Exception
    {
        final byte[] bytes = "helloworld".getBytes(UTF_8);
        final byte[] expected = getObjectBytes(bytes);
        doTestDataWithAnnotation(expected, OBJECT_MESSAGE_MESSAGE_ANNOTATION, OBJECT_STREAM);
    }

    @Test
    void dataWithMapMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), MAP_MESSAGE_MESSAGE_ANNOTATION, OCTET_STREAM);
    }

    @Test
    void dataWithMapMessageAnnotationAndContentTypeJmsMapMessage() throws Exception
    {
        final Map<String, Object> originalMap = Map.of("testKey", "testValue");
        final byte[] data = new MapToJmsMapMessage().toMimeContent(originalMap);
        final String expectedMimeType = "jms/map-message";
        final Data value = new Data(new Binary(data));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        final Message_1_0 sourceMessage =
                createTestMessage(properties, MAP_MESSAGE_MESSAGE_ANNOTATION, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(expectedMimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(data, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithMapMessageAnnotationAndContentTypeAmqpMap() throws Exception
    {
        final Map<String, Object> originalMap = Map.of("testKey", "testValue");
        final byte[] data = new MapToAmqpMapConverter().toMimeContent(originalMap);
        final String expectedMimeType = "amqp/map";
        final Data value = new Data(new Binary(data));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        final Message_1_0 sourceMessage =
                createTestMessage(properties, MAP_MESSAGE_MESSAGE_ANNOTATION, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(expectedMimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(data, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithBytesMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), BYTE_MESSAGE_MESSAGE_ANNOTATION, OCTET_STREAM);
    }

    @Test
    void dataWithStreamMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION, OCTET_STREAM);
    }

    @Test
    void dataWithStreamMessageAnnotationAndContentTypeJmsStreamMessage() throws Exception
    {
        final List<Object> originalList = List.of("testValue");
        final byte[] data = new ListToJmsStreamMessage().toMimeContent(originalList);
        final String expectedMimeType = "jms/stream-message";
        final Data value = new Data(new Binary(data));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        final Message_1_0 sourceMessage =
                createTestMessage(properties, STREAM_MESSAGE_MESSAGE_ANNOTATION, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(expectedMimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(data, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithStreamMessageAnnotationAndContentTypeAmqpList() throws Exception
    {
        final List<Object> originalList = List.of("testValue");
        final byte[] data = new ListToAmqpListConverter().toMimeContent(originalList);
        final String expectedMimeType = "amqp/list";
        final Data value = new Data(new Binary(data));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        final Message_1_0 sourceMessage =
                createTestMessage(properties, STREAM_MESSAGE_MESSAGE_ANNOTATION, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(expectedMimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(data, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithTextMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, "text/plain");
    }

    @Test
    void dataWithUnsupportedMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 11)),
                                 OCTET_STREAM);
    }

    @Test
    void dataWithContentTypeText() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("text/foobar");
    }

    @Test
    void dataWithContentTypeApplicationXml() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/xml");
    }

    @Test
    void dataWithContentTypeApplicationXmlDtd() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/xml-dtd");
    }

    @Test
    void dataWithContentTypeApplicationFooXml() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/foo+xml");
    }

    @Test
    void dataWithContentTypeApplicationJson() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/json");
    }

    @Test
    void dataWithContentTypeApplicationFooJson() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/foo+json");
    }

    @Test
    void dataWithContentTypeApplicationJavascript() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/javascript");
    }

    @Test
    void dataWithContentTypeApplicationEcmascript() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/ecmascript");
    }

    @Test
    void dataWithContentTypeAmqpMap() throws Exception
    {
        final Map<String, Object> originalMap = Map.of("testKey", "testValue");
        final byte[] bytes = new MapToAmqpMapConverter().toMimeContent(originalMap);
        final Data value = new Data(new Binary(bytes));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("amqp/map"));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("amqp/map", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(bytes, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithContentTypeJmsMapMessage() throws Exception
    {
        final Map<String, Object> originalMap = Map.of("testKey", "testValue");
        final byte[] bytes = new MapToJmsMapMessage().toMimeContent(originalMap);
        final Data value = new Data(new Binary(bytes));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/map-message"));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(bytes, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithContentTypeAmqpList() throws Exception
    {
        final List<Object> originalMap = List.of("testValue");
        final byte[] bytes = new ListToAmqpListConverter().toMimeContent(originalMap);
        final Data value = new Data(new Binary(bytes));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("amqp/list"));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("amqp/list", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(bytes, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithContentTypeJmsStreamMessage() throws Exception
    {
        final List<Object> originalMap = List.of("testValue");
        final byte[] bytes = new ListToJmsStreamMessage().toMimeContent(originalMap);
        final Data value = new Data(new Binary(bytes));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/stream-message"));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/stream-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(bytes, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithContentTypeJavaSerializedObject() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(expected, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithContentTypeJavaObjectStream() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(OBJECT_STREAM));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(expected, getBytes(content), "Unexpected content");
    }

    @Test
    void dataWithContentTypeOther() throws Exception
    {
        final byte[] expected = "helloworld".getBytes(UTF_8);
        final Data value = new Data(new Binary(expected));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/bin"));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(OCTET_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(expected, getBytes(content), "Unexpected content");
    }

    @Test
    void data() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        final Message_1_0 sourceMessage = createTestMessage(value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(OCTET_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(expected, getBytes(content), "Unexpected content");
    }

    @Test
    void noBodyWithMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithObjectMessageAnnotation() throws Exception
    {
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(getObjectBytes(null), getBytes(content), "Unexpected content size");
    }

    @Test
    void noBodyWithMapMessageAnnotation() throws Exception
    {
        final Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(new MapToJmsMapMessage().toMimeContent(Map.of()), getBytes(content), "Unexpected content size");
    }

    @Test
    void noBodyWithBytesMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OCTET_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithStreamMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("jms/stream-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithTextMessageAnnotation()
    {
        final Message_1_0 sourceMessage = createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithUnknownMessageAnnotation()
    {
        final Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Map.of(Symbol.valueOf("x-opt-jms-msg-type"), (byte) 11)), null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBody()
    {
        final Message_1_0 sourceMessage = createTestMessage(null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertNull(convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithContentTypeApplicationOctetStream()
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(OCTET_STREAM));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OCTET_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(0, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithObjectMessageContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(getObjectBytes(null).length, convertedMessage.getSize(), "Unexpected content size");
    }

    @Test
    void noBodyWithJmsMapContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/map-message"));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(new MapToJmsMapMessage().toMimeContent(Map.of()), getBytes(content), "Unexpected content size");
    }

    @Test
    void messageAnnotationTakesPrecedenceOverContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(OCTET_STREAM));
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals(OBJECT_STREAM, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertEquals(getObjectBytes(null).length, convertedMessage.getSize(), "Unexpected content size");
    }

    private void doTestConvertOfDataSectionForTextualType(final String contentType) throws Exception
    {
        final String expected = "testContent";
        final Data value = new Data(new Binary(expected.getBytes(UTF_8)));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(contentType));
        final Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals(expected, new String(getBytes(content), UTF_8), "Unexpected content");
        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
    }

    private byte[] getBytes(final QpidByteBuffer content) throws Exception
    {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final InputStream contentInputStream = content.asInputStream())
        {
            contentInputStream.transferTo(bos);
            content.dispose();
            return bos.toByteArray();
        }
    }

    private Message_1_0 createTestMessage(final EncodingRetainingSection encodingRetainingSection)
    {
        return createTestMessage(new Properties(), encodingRetainingSection);
    }

    private Message_1_0 createTestMessage(final Properties properties, final EncodingRetainingSection section)
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
                                          final EncodingRetainingSection section)
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
                                          final EncodingRetainingSection section)
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
                                          final EncodingRetainingSection section)
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
            // TODO this seems to leak QBBs
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
                                          final String expectedMimeType) throws Exception
    {
        final Data value = new Data(new Binary(data));
        final Message_1_0 sourceMessage = createTestMessage(messageAnnotations, value.createEncodingRetainingSection());
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent();

        assertEquals(expectedMimeType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected mime type");
        assertArrayEquals(data, getBytes(content), "Unexpected content");
    }
}
