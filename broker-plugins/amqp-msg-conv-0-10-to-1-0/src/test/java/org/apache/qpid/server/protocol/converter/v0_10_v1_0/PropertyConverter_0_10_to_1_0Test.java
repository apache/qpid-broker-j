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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"unchecked"})
class PropertyConverter_0_10_to_1_0Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_10_to_1_0 _messageConverter;

    @BeforeAll
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_10_to_1_0();
    }

    @Test
    void contentTypeConversion()
    {
        final String contentType = "test-content-type";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(contentType);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(contentType, properties.getContentType().toString(), "Unexpected content type");
    }

    @Test
    void contentTypeJavaObjectStreamConversion()
    {
        final String contentType = "application/x-java-serialized-object";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType("application/java-object-stream");
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(contentType, properties.getContentType().toString(), "Unexpected content type");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentEncoding(contentEncoding);
        final MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), messageProperties, new byte[]{(byte) 1}, 0);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(contentEncoding, properties.getContentEncoding().toString(), "Unexpected content encoding");
    }

    @Test
    void headerConversion()
    {
        final Map<String, Object> headers = Map.of("testProperty1", "testProperty1Value", "intProperty", 1);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(headers, new HashMap<>(applicationProperties), "Unexpected applicationProperties");
    }

    @Test
    void headerConversionWhenQpidSubjectIsPresent()
    {
        final String testSubject = "testSubject";
        final Map<String, Object> headers = Map.of("testProperty1", "testProperty1Value", "qpid.subject", testSubject);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(testSubject, properties.getSubject(), "Unexpected subject");
        assertFalse(applicationProperties.containsKey("qpid.subject"), "Unexpected subject in application properties");
    }

    @Test
    void persistentDeliveryModeConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertTrue(header.getDurable(), "Unexpected durable header");
    }

    @Test
    void nonPersistentDeliveryModeConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertFalse(header.getDurable(), "Unexpected durable header");
    }

    @Test
    void priorityConversion()
    {
        final byte priority = 5;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertEquals(priority, (long) header.getPriority().byteValue(), "Unexpected priority");
    }

    @Test
    void correlationIdConversion()
    {
        final String correlationId = "testCorrelationId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId.getBytes(UTF_8));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(correlationId, properties.getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void binaryCorrelationIdConversion()
    {
        final byte[] correlationId = new byte[]{0x00, (byte) 0xff, (byte) 0xc3};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final boolean condition = properties.getCorrelationId() instanceof Binary;

        assertTrue(condition, String.format("Unexpected correlationId type. expected 'Binary' actual '%s'",
                properties.getCorrelationId().getClass().getSimpleName()));
        assertArrayEquals(correlationId, ((Binary) properties.getCorrelationId()).getArray(), "Unexpected correlationId");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeySpecified()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("amq.direct/test_routing_key", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeSpecified()
    {
        final String exchangeName = "amq.direct";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, null));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(exchangeName, properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(routingKey, properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeIsEmptyStringAndRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo("", routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("test_routing_key", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreNull()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, null));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertNull(properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void expirationConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;
        final long expiration = timestamp + ttl;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExpiration(expiration);
        final MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertNull(properties.getAbsoluteExpiryTime(), "Unexpected expiration");
        assertEquals(ttl, (long) header.getTtl().intValue(), "Unexpected TTL");
    }

    @Test
    void TTLConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        final MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(ttl, header.getTtl().longValue(), "Unexpected TTL");
        assertNull(properties.getAbsoluteExpiryTime(), "Unexpected expiration");
    }

    @Test
    void messageIdConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setMessageId(messageId);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(messageId, properties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void timestampConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        final long timestamp = System.currentTimeMillis() - 1000;
        deliveryProperties.setTimestamp(timestamp);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(timestamp, properties.getCreationTime().getTime(), "Unexpected creation timestamp");
    }

    @Test
    void arrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 1000;
        final MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), new MessageProperties(), null, timestamp);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(timestamp, properties.getCreationTime().getTime(), "Unexpected creation timestamp");
    }

    @Test
    void jmsTypeConversion()
    {
        final String type = "test-type";
        final Map<String, Object> headers = Map.of("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(type, properties.getSubject(), "Unexpected subject");
        assertFalse(applicationProperties.containsKey("x-jms-type"), "Unexpected x-jms-type in application properties");
    }

    @Test
    void qpidSubjectTakesPrecedenceOverJmsType()
    {
        final String jmsType = "test-jms-type";
        final String qpidSubjectType = "test-qpid-type";
        final Map<String, Object> headers = Map.of("x-jms-type", jmsType, "qpid.subject", qpidSubjectType);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(qpidSubjectType, properties.getSubject(), "Unexpected subject");
        assertTrue(applicationProperties.isEmpty(), "Unexpected entries in application properties");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "test-userId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(userId, new String(properties.getUserId().getArray(), UTF_8), "Unexpected user-id");
    }

    @Test
    void headerJMSXGroupIdConversion()
    {
        final String testGroupId = "testGroupId";
        final Map<String, Object> headers = Map.of("JMSXGroupID", testGroupId);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(testGroupId, properties.getGroupId(), "Unexpected group-id");
        assertFalse(applicationProperties.containsKey("JMSXGroupID"), "Unexpected JMSXGroupID in application properties");
    }

    @Test
    void headerJMSXGroupSeqConversion()
    {
        final int testGroupSequenceNumber = 1;
        final Map<String, Object> headers = Map.of("JMSXGroupSeq", testGroupSequenceNumber);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(testGroupSequenceNumber, (long) properties.getGroupSequence().intValue(), "Unexpected group-sequence");
        assertFalse(applicationProperties.containsKey("JMSXGroupSeq"), "Unexpected JMSXGroupSeq in application properties");
    }

    @Test
    void headerJMSXGroupSeqConversionWhenWrongType()
    {
        final short testGroupSequenceNumber = (short) 1;
        final Map<String, Object> headers = Map.of("JMSXGroupSeq", testGroupSequenceNumber);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertNull(properties.getGroupSequence(), "Unexpected group-sequence");
        assertTrue(applicationProperties.containsKey("JMSXGroupSeq"), "JMSXGroupSeq was removed from application properties");
    }

    @Test
    void headerWithMapValueConversionFails()
    {
        final Map<String, Object> headers = Map.of("mapHeader", Map.of());
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is expected");
    }

    @Test
    void headerWithListValueConversionFails()
    {
        final Map<String, Object> headers = Map.of("listHeader", List.of());
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is expected");
    }

    @Test
    void headerWithArrayValueConversionFails()
    {
        final Map<String, Object> headers = Map.of("listHeader", new int[]{1});
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is expected");
    }

    @Test
    void exchangeRoutingKeyConversion()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        deliveryProperties.setRoutingKey(testRoutingKey);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(testExchange + "/" + testRoutingKey, properties.getTo(), "Unexpected to");
    }

    private MessageTransferMessage createTestMessage(final DeliveryProperties deliveryProperties)
    {
        return createTestMessage(deliveryProperties, new MessageProperties(), null, 0);
    }

    private MessageTransferMessage createTestMessage(final MessageProperties messageProperties)
    {
        return createTestMessage(new DeliveryProperties(), messageProperties, null, 0);
    }

    private MessageTransferMessage createTestMessage(final DeliveryProperties deliveryProperties,
                                                     final MessageProperties messageProperties,
                                                     final byte[] content,
                                                     final long arrivalTime)
    {
        final int bodySize = content == null ? 0 : content.length;
        final org.apache.qpid.server.protocol.v0_10.transport.Header header =
                new org.apache.qpid.server.protocol.v0_10.transport.Header(deliveryProperties, messageProperties);
        final MessageMetaData_0_10 metaData = new MessageMetaData_0_10(header, bodySize, arrivalTime);
        final StoredMessage<MessageMetaData_0_10> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData()).thenReturn(metaData);

        if (content != null)
        {
            when(storedMessage.getContentSize()).thenReturn(content.length);
            when(storedMessage.getContent(0, content.length)).thenReturn(QpidByteBuffer.wrap(content));
        }
        else
        {
            when(storedMessage.getContentSize()).thenReturn(0);
            when(storedMessage.getContent(0, 0)).thenReturn(QpidByteBuffer.emptyQpidByteBuffer());
        }
        return new MessageTransferMessage(storedMessage, null);
    }
}
