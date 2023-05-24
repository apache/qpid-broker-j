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

package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class PropertyConverter_0_10_to_InternalTest extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_v0_10_to_Internal _messageConverter;

    @BeforeEach
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_v0_10_to_Internal();
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentEncoding(contentEncoding);
        final MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), messageProperties, new byte[] {(byte) 1}, 0);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(contentEncoding, convertedMessage.getMessageHeader().getEncoding(), "Unexpected content encoding");

    }

    @Test
    void applicationHeadersConversion()
    {
        final Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("intProperty", 1);
        headers.put("nullProperty", null);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final Map<String, Object> header = convertedMessage.getMessageHeader().getHeaderMap();
        assertEquals(headers, new HashMap<>(header), "Unexpected headers");
    }

    @Test
    void persistentDeliveryModeConversion()
    {
        final MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void nonPersistentDeliveryModeConversion()
    {
        final MessageDeliveryMode deliveryMode = MessageDeliveryMode.NON_PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void priorityConversion()
    {
        final byte priority = 7;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
        final MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(priority, (long) convertedMessage.getMessageHeader().getPriority(), "Unexpected priority");
    }

    @Test
    void correlationIdConversion()
    {
        final byte[] correlationId = "testCorrelationId".getBytes();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertArrayEquals(correlationId, convertedMessage.getMessageHeader().getCorrelationId().getBytes(UTF_8),
                "Unexpected correlationId");
    }

    @Test
    void correlationIdConversionWhenNotString()
    {
        final byte[] correlationId = new byte[]{(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(new String(correlationId, UTF_8), convertedMessage.getMessageHeader().getCorrelationId(),
                "Unexpected correlationId");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeySpecified()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final String expectedReplyTo = String.format("%s/%s", exchangeName, routingKey);
        assertEquals(expectedReplyTo, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeSpecified()
    {
        final String exchangeName = "amq.direct";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, null));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(exchangeName, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(routingKey, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeIsEmptyStringAndRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo("", routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(routingKey, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreNull()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, null));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertNull(convertedMessage.getMessageHeader().getReplyTo(), "Unexpected reply-to");
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

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(expiration, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    void ttlConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        final MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(timestamp + ttl, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    void ttlTakesPrecedenceOverExpiration()
    {
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        deliveryProperties.setExpiration(timestamp + ttl + 10000);
        final MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(timestamp + ttl, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    void messageIdConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setMessageId(messageId);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("ID:" + messageId, convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    void timestampConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        final long timestamp = System.currentTimeMillis() - 1000;
        deliveryProperties.setTimestamp(timestamp);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(timestamp, convertedMessage.getMessageHeader().getTimestamp(), "Unexpected creation timestamp");
    }

    @Test
    void arrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 1000;
        final MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(timestamp, convertedMessage.getMessageHeader().getTimestamp(), "Unexpected creation timestamp");
    }

    @Test
    void jmsTypeConversion()
    {
        final String type = "test-type";
        final Map<String, Object> headers = Map.of("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(type, convertedMessage.getMessageHeader().getType(), "Unexpected subject");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "test-userId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(userId, convertedMessage.getMessageHeader().getUserId(), "Unexpected user-id");
    }

    @Test
    void userIdConversionWhenNotUtf8()
    {
        final byte[] userId = new byte[]{(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(new String(userId, UTF_8), convertedMessage.getMessageHeader().getUserId(), "Unexpected user-id");
    }

    @Test
    void exchangeConversion()
    {
        final String testExchange = "testExchange";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(testExchange, convertedMessage.getTo(), "Unexpected to");
    }

    @Test
    void initialRoutingAddressConversion()
    {
        final String testExchange = "testExchange";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        final String testRoutingKey = "testRoutingKey";
        deliveryProperties.setRoutingKey(testRoutingKey);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(testRoutingKey, convertedMessage.getInitialRoutingAddress(), "Unexpected initial routing address");
    }

    @Test
    void applicationIdConversion()
    {
        final String applicationId = "testAppId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setAppId(applicationId.getBytes(UTF_8));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals(applicationId, convertedMessage.getMessageHeader().getAppId(), "Unexpected app-id");
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
