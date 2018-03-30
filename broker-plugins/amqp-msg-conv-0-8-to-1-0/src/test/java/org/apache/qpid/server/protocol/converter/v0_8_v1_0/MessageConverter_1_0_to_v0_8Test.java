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
package org.apache.qpid.server.protocol.converter.v0_8_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.AmqpListToListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.AmqpMapToMapConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.ListToAmqpListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.MapToAmqpMapConverter;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
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

public class MessageConverter_1_0_to_v0_8Test extends UnitTestBase
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
    private MessageConverter_1_0_to_v0_8 _converter;

    @Before
    public void setUp() throws Exception
    {
        _converter = new MessageConverter_1_0_to_v0_8();
    }

    @Test
    public void testAmqpValueWithNullWithTextMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());

    }

    @Test
    public void testAmqpValueWithNullWithMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testAmqpValueWithNullWithObjectMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertArrayEquals("Unexpected content size", getObjectBytes(null), getBytes(content));
    }

    @Test
    public void testAmqpValueWithNullWithMapMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("Unexpected mime type",
                            "jms/map-message",
                            convertedMessage.getMessageHeader().getMimeType());
        assertArrayEquals("Unexpected content size",
                          new MapToJmsMapMessage().toMimeContent(Collections.emptyMap()),
                          getBytes(content));
    }

    @Test
    public void testAmqpValueWithNullWithBytesMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testAmqpValueWithNullWithStreamMessageAnnotation() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/stream-message",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
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

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testAmqpValueWithNullWithContentTypeApplicationOctetStream() throws Exception
    {
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/octet-stream"));
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testAmqpValueWithNullWithObjectMessageContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());

        assertEquals("Unexpected content size",
                            (long) getObjectBytes(null).length,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testAmqpValueWithNullWithJmsMapContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/map-message"));
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(properties, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("Unexpected mime type",
                            "jms/map-message",
                            convertedMessage.getMessageHeader().getMimeType());

        assertArrayEquals("Unexpected content size",
                          new MapToJmsMapMessage().toMimeContent(Collections.emptyMap()),
                          getBytes(content));
    }

    @Test
    public void testAmqpValueWithNull() throws Exception
    {
        final Object expected = null;
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testAmqpValueWithString() throws Exception
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertEquals("Unexpected content", expected, new String(getBytes(content), UTF_8));
    }

    @Test
    public void testAmqpValueWithStringWithTextMessageAnnotation() throws Exception
    {
        final String expected = "testContent";
        final AmqpValue amqpValue = new AmqpValue(expected);
        Message_1_0 sourceMessage =
                createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertEquals("Unexpected content", expected, new String(getBytes(content), UTF_8));
    }

    @Test
    public void testAmqpValueWithMap() throws Exception
    {
        final Map<String, Object> originalMap = new LinkedHashMap<>();
        originalMap.put("binaryEntry", new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalMap.put("intEntry", 42);
        originalMap.put("nullEntry", null);
        final AmqpValue amqpValue = new AmqpValue(originalMap);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/map-message",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        Map<String, Object> convertedMap = new JmsMapMessageToMap().toObject(getBytes(content));
        assertEquals("Unexpected size", (long) originalMap.size(), (long) convertedMap.size());
        assertArrayEquals("Unexpected binary entry", ((Binary) originalMap.get("binaryEntry")).getArray(),
                          (byte[]) convertedMap.get("binaryEntry"));
        assertEquals("Unexpected int entry", originalMap.get("intEntry"), convertedMap.get("intEntry"));
        assertEquals("Unexpected null entry", originalMap.get("nullEntry"), convertedMap.get("nullEntry"));
    }

    @Test
    public void testAmqpValueWithMapContainingMap() throws Exception
    {
        final Map<String, Object> originalMap =
                Collections.singletonMap("testMap", Collections.singletonMap("innerKey", "testValue"));

        final AmqpValue amqpValue = new AmqpValue(originalMap);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "amqp/map", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        Map<String, Object> convertedMap = new AmqpMapToMapConverter().toObject(getBytes(content));
        assertEquals("Unexpected size", (long) originalMap.size(), (long) convertedMap.size());
        assertEquals("Unexpected binary entry",
                            new HashMap((Map<String, Object>) originalMap.get("testMap")),
                            new HashMap((Map<String, Object>) convertedMap.get("testMap")));
    }

    @Test
    public void testAmqpValueWithMapContainingNonFieldTableCompliantEntries() throws Exception
    {
        final AmqpValue amqpValue = new AmqpValue(Collections.<Object, Object>singletonMap(13, 42));
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        try
        {
            _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
            fail("expected exception not thrown.");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testAmqpValueWithList() throws Exception
    {
        final List<Object> originalList = new ArrayList<>();
        originalList.add(new Binary(new byte[]{0x00, (byte) 0xFF}));
        originalList.add(42);
        originalList.add(null);
        final AmqpValue amqpValue = new AmqpValue(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/stream-message",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<Object> convertedList = new JmsStreamMessageToList().toObject(getBytes(content));
        assertEquals("Unexpected size", (long) originalList.size(), (long) convertedList.size());
        assertArrayEquals("Unexpected binary item", ((Binary) originalList.get(0)).getArray(),
                          (byte[]) convertedList.get(0));
        assertEquals("Unexpected int item", originalList.get(1), convertedList.get(1));
        assertEquals("Unexpected null item", originalList.get(2), convertedList.get(2));
    }

    @Test
    public void testAmqpValueWithListContainingMap() throws Exception
    {
        final List<Object> originalList = Collections.singletonList(Collections.singletonMap("testKey", "testValue"));
        final AmqpValue amqpValue = new AmqpValue(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "amqp/list", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<Object> convertedList = new AmqpListToListConverter().toObject(getBytes(content));
        assertEquals("Unexpected size", (long) originalList.size(), (long) convertedList.size());
        assertEquals("Unexpected map item",
                            new HashMap<String, Object>((Map) originalList.get(0)),
                            new HashMap<String, Object>((Map) convertedList.get(0)));
    }

    @Test
    public void testAmqpValueWithListContainingUnsupportedType() throws Exception
    {
        final List<Object> originalList = Collections.singletonList(new Source());
        final AmqpValue amqpValue = new AmqpValue(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        try
        {
            _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
            fail("expected exception not thrown.");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testAmqpValueWithUnsupportedType() throws Exception
    {
        final Integer originalValue = 42;
        final AmqpValue amqpValue = new AmqpValue(originalValue);
        Message_1_0 sourceMessage = createTestMessage(amqpValue.createEncodingRetainingSection());

        try
        {
            _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
            fail("expected exception not thrown.");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testAmqpSequenceWithSimpleTypes() throws Exception
    {
        final List<Integer> expected = new ArrayList<>();
        expected.add(37);
        expected.add(42);
        final AmqpSequence amqpSequence = new AmqpSequence(expected);
        Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/stream-message",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertEquals("Unexpected content", expected, new JmsStreamMessageToList().toObject(getBytes(content)));
    }

    @Test
    public void testAmqpSequenceWithMap() throws Exception
    {
        final List<Object> originalList = Collections.singletonList(Collections.singletonMap("testKey", "testValue"));
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "amqp/list", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<Object> convertedList = new AmqpListToListConverter().toObject(getBytes(content));
        assertEquals("Unexpected size", (long) originalList.size(), (long) convertedList.size());
        assertEquals("Unexpected map item",
                            new HashMap<String, Object>((Map) originalList.get(0)),
                            new HashMap<String, Object>((Map) convertedList.get(0)));
    }

    @Test
    public void testAmqpSequenceWithUnsupportedType() throws Exception
    {
        final List<Object> originalList = Collections.singletonList(new Source());
        final AmqpSequence amqpSequence = new AmqpSequence(originalList);
        Message_1_0 sourceMessage = createTestMessage(amqpSequence.createEncodingRetainingSection());

        try
        {
            _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
            fail("expected exception not thrown.");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testDataWithMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), MESSAGE_MESSAGE_ANNOTATION, "application/octet-stream");
    }

    @Test
    public void testDataWithObjectMessageAnnotation() throws Exception
    {
        byte[] bytes = "helloworld".getBytes(UTF_8);
        final byte[] expected = getObjectBytes(bytes);
        doTestDataWithAnnotation(expected, OBJECT_MESSAGE_MESSAGE_ANNOTATION, "application/java-object-stream");
    }

    @Test
    public void testDataWithMapMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 MAP_MESSAGE_MESSAGE_ANNOTATION,
                                 "application/octet-stream");
    }

    @Test
    public void testDataWithMapMessageAnnotationAndContentTypeJmsMapMessage() throws Exception
    {
        Map<String, Object> originalMap = Collections.singletonMap("testKey", "testValue");
        byte[] data = new MapToJmsMapMessage().toMimeContent(originalMap);
        String expectedMimeType = "jms/map-message";
        final Data value = new Data(new Binary(data));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        Message_1_0 sourceMessage = createTestMessage(properties,
                                                      MAP_MESSAGE_MESSAGE_ANNOTATION,
                                                      value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", expectedMimeType, convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", data, getBytes(content));
    }

    @Test
    public void testDataWithMapMessageAnnotationAndContentTypeAmqpMap() throws Exception
    {
        Map<String, Object> originalMap = Collections.singletonMap("testKey", "testValue");
        byte[] data = new MapToAmqpMapConverter().toMimeContent(originalMap);
        String expectedMimeType = "amqp/map";
        final Data value = new Data(new Binary(data));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        Message_1_0 sourceMessage = createTestMessage(properties,
                                                      MAP_MESSAGE_MESSAGE_ANNOTATION,
                                                      value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", expectedMimeType, convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", data, getBytes(content));
    }

    @Test
    public void testDataWithBytesMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 BYTE_MESSAGE_MESSAGE_ANNOTATION,
                                 "application/octet-stream");
    }

    @Test
    public void testDataWithStreamMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                 "application/octet-stream");
    }

    @Test
    public void testDataWithStreamMessageAnnotationAndContentTypeJmsStreamMessage() throws Exception
    {
        List<Object> originalList = Collections.singletonList("testValue");
        byte[] data = new ListToJmsStreamMessage().toMimeContent(originalList);
        String expectedMimeType = "jms/stream-message";
        final Data value = new Data(new Binary(data));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        Message_1_0 sourceMessage = createTestMessage(properties,
                                                      STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                                      value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", expectedMimeType, convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", data, getBytes(content));
    }

    @Test
    public void testDataWithStreamMessageAnnotationAndContentTypeAmqpList() throws Exception
    {
        List<Object> originalList = Collections.singletonList("testValue");
        byte[] data = new ListToAmqpListConverter().toMimeContent(originalList);
        String expectedMimeType = "amqp/list";
        final Data value = new Data(new Binary(data));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(expectedMimeType));
        Message_1_0 sourceMessage = createTestMessage(properties,
                                                      STREAM_MESSAGE_MESSAGE_ANNOTATION,
                                                      value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", expectedMimeType, convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", data, getBytes(content));
    }

    @Test
    public void testDataWithTextMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8), TEXT_MESSAGE_MESSAGE_ANNOTATION, "text/plain");
    }

    @Test
    public void testDataWithUnsupportedMessageAnnotation() throws Exception
    {
        doTestDataWithAnnotation("helloworld".getBytes(UTF_8),
                                 new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                 (byte) 11)),
                                 "application/octet-stream");
    }

    @Test
    public void testDataWithContentTypeText() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("text/foobar");
    }

    @Test
    public void testDataWithContentTypeApplicationXml() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/xml");
    }

    @Test
    public void testDataWithContentTypeApplicationXmlDtd() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/xml-dtd");
    }

    @Test
    public void testDataWithContentTypeApplicationFooXml() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/foo+xml");
    }

    @Test
    public void testDataWithContentTypeApplicationJson() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/json");
    }

    @Test
    public void testDataWithContentTypeApplicationFooJson() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/foo+json");
    }

    @Test
    public void testDataWithContentTypeApplicationJavascript() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/javascript");
    }

    @Test
    public void testDataWithContentTypeApplicationEcmascript() throws Exception
    {
        doTestConvertOfDataSectionForTextualType("application/ecmascript");
    }

    @Test
    public void testDataWithContentTypeAmqpMap() throws Exception
    {
        Map<String, Object> originalMap = Collections.singletonMap("testKey", "testValue");
        byte[] bytes = new MapToAmqpMapConverter().toMimeContent(originalMap);

        final Data value = new Data(new Binary(bytes));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("amqp/map"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "amqp/map", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", bytes, getBytes(content));
    }

    @Test
    public void testDataWithContentTypeJmsMapMessage() throws Exception
    {
        Map<String, Object> originalMap = Collections.singletonMap("testKey", "testValue");
        byte[] bytes = new MapToJmsMapMessage().toMimeContent(originalMap);

        final Data value = new Data(new Binary(bytes));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/map-message"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/map-message",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", bytes, getBytes(content));
    }

    @Test
    public void testDataWithContentTypeAmqpList() throws Exception
    {
        List<Object> originalMap = Collections.singletonList("testValue");
        byte[] bytes = new ListToAmqpListConverter().toMimeContent(originalMap);

        final Data value = new Data(new Binary(bytes));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("amqp/list"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "amqp/list", convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", bytes, getBytes(content));
    }

    @Test
    public void testDataWithContentTypeJmsStreamMessage() throws Exception
    {
        List<Object> originalMap = Collections.singletonList("testValue");
        byte[] bytes = new ListToJmsStreamMessage().toMimeContent(originalMap);

        final Data value = new Data(new Binary(bytes));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/stream-message"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/stream-message",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", bytes, getBytes(content));
    }

    @Test
    public void testDataWithContentTypeJavaSerializedObject() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", expected, getBytes(content));
    }


    @Test
    public void testDataWithContentTypeJavaObjectStream() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/java-object-stream"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", expected, getBytes(content));
    }

    @Test
    public void testDataWithContentTypeOther() throws Exception
    {
        final byte[] expected = "helloworld".getBytes(UTF_8);
        final Data value = new Data(new Binary(expected));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/bin"));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", expected, getBytes(content));
    }

    @Test
    public void testData() throws Exception
    {
        final byte[] expected = getObjectBytes("helloworld".getBytes(UTF_8));
        final Data value = new Data(new Binary(expected));
        final Message_1_0 sourceMessage = createTestMessage(value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals("Unexpected content", expected, getBytes(content));
    }

    @Test
    public void testNoBodyWithMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithObjectMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertArrayEquals("Unexpected content size", getObjectBytes(null), getBytes(content));
    }

    @Test
    public void testNoBodyWithMapMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(MAP_MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("Unexpected mime type",
                            "jms/map-message",
                            convertedMessage.getMessageHeader().getMimeType());
        assertArrayEquals("Unexpected content size",
                          new MapToJmsMapMessage().toMimeContent(Collections.emptyMap()),
                          getBytes(content));
    }

    @Test
    public void testNoBodyWithBytesMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(BYTE_MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/octet-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithStreamMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(STREAM_MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "jms/stream-message",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithTextMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage = createTestMessage(TEXT_MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithUnknownMessageAnnotation() throws Exception
    {
        Message_1_0 sourceMessage =
                createTestMessage(new MessageAnnotations(Collections.singletonMap(Symbol.valueOf("x-opt-jms-msg-type"),
                                                                                  (byte) 11)), null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBody() throws Exception
    {
        final Message_1_0 sourceMessage = createTestMessage(null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", null, convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithContentTypeApplicationOctetStream() throws Exception
    {
        Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/octet-stream"));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", "application/octet-stream", convertedMessage.getMessageHeader()
                                                                                                .getMimeType());
        assertEquals("Unexpected content size",
                            (long) 0,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithObjectMessageContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/x-java-serialized-object"));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());

        assertEquals("Unexpected content size",
                            (long) getObjectBytes(null).length,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    @Test
    public void testNoBodyWithJmsMapContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("jms/map-message"));
        final Message_1_0 sourceMessage = createTestMessage(properties, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        assertEquals("Unexpected mime type",
                            "jms/map-message",
                            convertedMessage.getMessageHeader().getMimeType());

        assertArrayEquals("Unexpected content size",
                          new MapToJmsMapMessage().toMimeContent(Collections.emptyMap()),
                          getBytes(content));
    }

    @Test
    public void testMessageAnnotationTakesPrecedenceOverContentType() throws Exception
    {
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf("application/octet-stream"));
        final Message_1_0 sourceMessage = createTestMessage(OBJECT_MESSAGE_MESSAGE_ANNOTATION, null);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type",
                            "application/java-object-stream",
                            convertedMessage.getMessageHeader().getMimeType());
        assertEquals("Unexpected content size",
                            (long) getObjectBytes(null).length,
                            (long) convertedMessage.getMessageMetaData().getContentSize());
    }

    private void doTestConvertOfDataSectionForTextualType(final String contentType) throws Exception
    {
        final String expected = "testContent";
        final Data value = new Data(new Binary(expected.getBytes(UTF_8)));
        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(contentType));
        Message_1_0 sourceMessage = createTestMessage(properties, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final QpidByteBuffer content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertEquals("Unexpected content", expected, new String(getBytes(content), UTF_8));

        assertEquals("Unexpected mime type", "text/plain", convertedMessage.getMessageHeader().getMimeType());
    }

    private byte[] getBytes(final QpidByteBuffer content) throws Exception
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             InputStream contentInputStream = content.asInputStream())
        {
            ByteStreams.copy(contentInputStream, bos);
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
            // TODO this seems to leak QBBs
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
                                          final String expectedMimeType) throws Exception
    {
        final Data value = new Data(new Binary(data));
        Message_1_0 sourceMessage = createTestMessage(messageAnnotations, value.createEncodingRetainingSection());

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        assertEquals("Unexpected mime type", expectedMimeType, convertedMessage.getMessageHeader().getMimeType());
        final QpidByteBuffer content = convertedMessage.getContent();
        assertArrayEquals("Unexpected content", data, getBytes(content));
    }
}
