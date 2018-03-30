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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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

public class PropertyConverter_0_10_to_InternalTest extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_v0_10_to_Internal _messageConverter;

    @Before
    public void setUp() throws Exception
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_v0_10_to_Internal();
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentEncoding(contentEncoding);
        MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), messageProperties, new byte[]{(byte) 1}, 0);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected content encoding",
                            contentEncoding,
                            convertedMessage.getMessageHeader().getEncoding());

    }

    @Test
    public void testApplicationHeadersConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("intProperty", 1);
        headers.put("nullProperty", null);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Map<String, Object> header = convertedMessage.getMessageHeader().getHeaderMap();
        assertEquals("Unexpected headers", headers, new HashMap<>(header));
    }

    @Test
    public void testPersistentDeliveryModeConversion()
    {
        MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertTrue("Unexpected persistence", convertedMessage.isPersistent());
        assertTrue("Unexpected persistence of meta data",
                          convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testNonPersistentDeliveryModeConversion()
    {
        MessageDeliveryMode deliveryMode = MessageDeliveryMode.NON_PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertFalse("Unexpected persistence", convertedMessage.isPersistent());
        assertFalse("Unexpected persistence of meta data",
                           convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testPriorityConversion()
    {
        final byte priority = 7;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected priority",
                            (long) priority,
                            (long) convertedMessage.getMessageHeader().getPriority());

    }

    @Test
    public void testCorrelationIdConversion()
    {
        final byte[] correlationId = "testCorrelationId".getBytes();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertTrue("Unexpected correlationId", Arrays.equals(correlationId,
                                                                    convertedMessage.getMessageHeader().getCorrelationId().getBytes(UTF_8)));
    }

    @Test
    public void testCorrelationIdConversionWhenNotString()
    {
        final byte[] correlationId = new byte[]{(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected correlationId",
                            new String(correlationId, UTF_8),
                            convertedMessage.getMessageHeader().getCorrelationId());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeySpecified()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        String expectedReplyTo = String.format("%s/%s", exchangeName, routingKey);
        assertEquals("Unexpected reply-to", expectedReplyTo, convertedMessage.getMessageHeader().getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenExchangeSpecified()
    {
        final String exchangeName = "amq.direct";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, null));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected reply-to", exchangeName, convertedMessage.getMessageHeader().getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected reply-to", routingKey, convertedMessage.getMessageHeader().getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenExchangeIsEmptyStringAndRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo("", routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected reply-to", routingKey, convertedMessage.getMessageHeader().getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreNull()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, null));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertNull("Unexpected reply-to", convertedMessage.getMessageHeader().getReplyTo());
    }

    @Test
    public void testExpirationConversion()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;
        final long expiration = timestamp + ttl;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExpiration(expiration);
        MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected expiration", expiration, convertedMessage.getMessageHeader().getExpiration());
    }

    @Test
    public void testTtlConversion()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected expiration",
                            timestamp + ttl,
                            convertedMessage.getMessageHeader().getExpiration());
    }

    @Test
    public void testTtlTakesPrecedenceOverExpiration()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        deliveryProperties.setExpiration(timestamp + ttl + 10000);
        MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected expiration",
                            timestamp + ttl,
                            convertedMessage.getMessageHeader().getExpiration());
    }

    @Test
    public void testMessageIdConversion()
    {
        UUID messageId = UUID.randomUUID();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setMessageId(messageId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected messageId",
                            "ID:" + messageId,
                            convertedMessage.getMessageHeader().getMessageId());
    }

    @Test
    public void testTimestampConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        final long timestamp = System.currentTimeMillis() - 1000;
        deliveryProperties.setTimestamp(timestamp);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected creation timestamp",
                            timestamp,
                            convertedMessage.getMessageHeader().getTimestamp());
    }

    @Test
    public void testArrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 1000;
        MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), new MessageProperties(), null, timestamp);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected creation timestamp",
                            timestamp,
                            convertedMessage.getMessageHeader().getTimestamp());
    }

    @Test
    public void testJmsTypeConversion()
    {
        final String type = "test-type";
        final Map<String, Object> headers = Collections.singletonMap("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected subject", type, convertedMessage.getMessageHeader().getType());
    }

    @Test
    public void testUserIdConversion()
    {
        final String userId = "test-userId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected user-id", userId, convertedMessage.getMessageHeader().getUserId());
    }

    @Test
    public void testUserIdConversionWhenNotUtf8()
    {
        final byte[] userId = new byte[]{(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected user-id",
                            new String(userId, UTF_8),
                            convertedMessage.getMessageHeader().getUserId());
    }

    @Test
    public void testExchangeConversion()
    {
        final String testExchange = "testExchange";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected to", testExchange, convertedMessage.getTo());
    }

    @Test
    public void testInitialRoutingAddressConversion()
    {
        final String testExchange = "testExchange";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        final String testRoutingKey = "testRoutingKey";
        deliveryProperties.setRoutingKey(testRoutingKey);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected initial routing address",
                            testRoutingKey,
                            convertedMessage.getInitialRoutingAddress());
    }

    @Test
    public void testApplicationIdConversion()
    {
        String applicationId = "testAppId";
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setAppId(applicationId.getBytes(UTF_8));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        assertEquals("Unexpected app-id", applicationId, convertedMessage.getMessageHeader().getAppId());
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
        int bodySize = content == null ? 0 : content.length;
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
