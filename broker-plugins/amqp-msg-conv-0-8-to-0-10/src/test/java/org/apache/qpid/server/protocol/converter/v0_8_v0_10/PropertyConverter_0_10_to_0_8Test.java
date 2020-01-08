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

package org.apache.qpid.server.protocol.converter.v0_8_v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyConverter_0_10_to_0_8Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_10_to_0_8 _messageConverter;

    @Before
    public void setUp() throws Exception
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_10_to_0_8();
    }

    @Test
    public void testContentTypeConversion()
    {
        String contentType = "test-content-type";

        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(contentType);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected content type", contentType, properties.getContentType().toString());
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentEncoding(contentEncoding);
        MessageTransferMessage message = createTestMessage(new DeliveryProperties(), messageProperties, new byte[]{(byte)1}, 0);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected content encoding", contentEncoding, properties.getEncoding().toString());
    }

    @Test
    public void testApplicationHeadersConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("intProperty", 1);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        Map<String, Object> applicationProperties = properties.getHeadersAsMap();
        assertEquals("Unexpected headers", headers, new HashMap<>(applicationProperties));
    }

    @Test
    public void testApplicationHeadersConversionWhenQpidSubjectIsPresent()
    {
        String testSubject = "testSubject";
        Map<String, Object> headers = new HashMap<>();
        headers.put("qpid.subject", testSubject);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        Map<String, Object> applicationProperties = properties.getHeadersAsMap();
        assertEquals("Unexpected subject in application properties",
                            testSubject,
                            applicationProperties.get("qpid.subject"));

    }

    @Test
    public void testPersistentDeliveryModeConversion()
    {
        MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected deliveryMode",
                            (long) deliveryMode.getValue(),
                            (long) properties.getDeliveryMode());

    }

    @Test
    public void testNonPersistentDeliveryModeConversion()
    {
        MessageDeliveryMode deliveryMode = MessageDeliveryMode.NON_PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected deliveryMode",
                            (long) deliveryMode.getValue(),
                            (long) properties.getDeliveryMode());
    }

    @Test
    public void testPriorityConversion()
    {
        final byte priority = 5;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected priority", (long) priority, (long) properties.getPriority());
    }

    @Test
    public void testCorrelationIdConversion()
    {
        final String correlationId = "testCorrelationId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId.getBytes());
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected correlationId", correlationId, properties.getCorrelationId().toString());
    }

    @Test
    public void testCorrelationIdConversionWhenLengthExceeds255()
    {
        final String correlationId = generateLongString();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId.getBytes());
        MessageTransferMessage message = createTestMessage(messageProperties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testCorrelationIdConversionWhenNotString()
    {
        final byte[] correlationId = new byte[] {(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertTrue("Unexpected correlationId",
                          Arrays.equals(correlationId, properties.getCorrelationId().getBytes()));

    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeySpecified()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        String expectedReplyTo = String.format("direct://%s//?routingkey='%s'", exchangeName, routingKey);
        assertEquals("Unexpected reply-to", expectedReplyTo, properties.getReplyTo().toString());
    }

    @Test
    public void testReplyToConversionWhenExchangeSpecified()
    {
        final String exchangeName = "amq.direct";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, null));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        String expectedReplyTo = String.format("direct://%s//", exchangeName);
        assertEquals("Unexpected reply-to", expectedReplyTo, properties.getReplyTo().toString());
    }

    @Test
    public void testReplyToConversionWhenRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);


        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        String expectedReplyTo = String.format("direct:////?routingkey='%s'", routingKey);
        assertEquals("Unexpected reply-to", expectedReplyTo, properties.getReplyTo().toString());
    }

    @Test
    public void testReplyToConversionWhenExchangeIsEmptyStringAndRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo("", routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        String expectedReplyTo = String.format("direct:////?routingkey='%s'", routingKey);
        assertEquals("Unexpected reply-to", expectedReplyTo, properties.getReplyTo().toString());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreNull()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, null));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertNull("Unexpected reply-to", properties.getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenResultExceeds255()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(generateLongString(255), generateLongString(255)));
        MessageTransferMessage message = createTestMessage(messageProperties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testExpirationConversion()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;
        final long expiration = timestamp + ttl;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExpiration(expiration);
        MessageTransferMessage message = createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected expiration", expiration, properties.getExpiration());
    }

    @Test
    public void testTTLConversion()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        MessageTransferMessage message = createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected expiration", timestamp + ttl, properties.getExpiration());
    }

   @Test
    public void testMessageIdConversion()
    {
        UUID messageId = UUID.randomUUID();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setMessageId(messageId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected messageId", "ID:" + messageId, properties.getMessageId().toString());
    }

    @Test
    public void testTimestampConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        final long timestamp = System.currentTimeMillis() - 1000;
        deliveryProperties.setTimestamp(timestamp);
        MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected creation timestamp", timestamp, properties.getTimestamp());
    }

    @Test
    public void testArrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 1000;
        MessageTransferMessage message = createTestMessage(new DeliveryProperties(), new MessageProperties(),  null, timestamp);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected creation timestamp", timestamp, properties.getTimestamp());
    }

    @Test
    public void testJmsTypeConversion()
    {
        final String type = "test-type";
        final Map<String, Object> headers = Collections.singletonMap("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("Unexpected subject", type, properties.getType().toString());
        Map<String, Object> applicationProperties = properties.getHeadersAsMap();
        assertFalse("Unexpected x-jms-type in application properties",
                           applicationProperties.containsKey("x-jms-type"));

    }

    @Test
    public void testJmsTypeConversionWhenLengthExceeds255()
    {
        final String type = generateLongString();
        final Map<String, Object> headers = Collections.singletonMap("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("Exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testUserIdConversion()
    {
        final String userId = "test-userId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("Unexpected user-id", userId, properties.getUserId().toString());
    }

    @Test
    public void testUserIdConversionExceeds255()
    {
        final String userId = generateLongString();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertNull("Unexpected user-id", properties.getUserId());
    }

    @Test
    public void testUserIdConversionWhenNotUtf8()
    {
        final byte[] userId = new byte[] {(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertTrue("Unexpected user-id", Arrays.equals(userId, properties.getUserId().getBytes()));
    }

    @Test
    public void testExchangeConversion()
    {
        final String testExchange = "testExchange";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        assertEquals("Unexpected exchange", testExchange, messagePublishInfo.getExchange().toString());
    }

    @Test
    public void testRoutingKeyConversion()
    {
        final String testRoutingKey = "testRoutingKey";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setRoutingKey(testRoutingKey);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        assertEquals("Unexpected routing key", testRoutingKey, messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testImmediateTrueConversion()
    {
        final boolean immediate = true;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setImmediate(immediate);

        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        assertEquals("Unexpected immediate flag", immediate, messagePublishInfo.isImmediate());
    }

    @Test
    public void testImmediateFalseConversion()
    {
        final boolean immediate = false;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setImmediate(immediate);

        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        assertEquals("Unexpected immediate flag", immediate, messagePublishInfo.isImmediate());
    }

    @Test
    public void testDiscardUnroutableTrueConversion()
    {
        final boolean discardUnroutable = true;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDiscardUnroutable(discardUnroutable);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        final Object expected = !discardUnroutable;
        assertEquals("Unexpected mandatory flag", expected, messagePublishInfo.isMandatory());
    }

    @Test
    public void testDiscardUnroutableFalseConversion()
    {
        final boolean discardUnroutable = false;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDiscardUnroutable(discardUnroutable);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        final Object expected = !discardUnroutable;
        assertEquals("Unexpected mandatory flag", expected, messagePublishInfo.isMandatory());
    }

    @Test
    public void testApplicationIdConversion()
    {
        String applicationId = "testAppId";
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setAppId(applicationId.getBytes(UTF_8));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals("Unexpected application id", applicationId, properties.getAppId().toString());
    }

    @Test
    public void testApplicationIdConversionWhenLengthExceeds255()
    {
        String appId = generateLongString();
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setAppId(appId.getBytes(UTF_8));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        assertNull("Unexpected application id", properties.getAppId());
    }

    private String generateLongString()
    {
        return generateLongString(AMQShortString.MAX_LENGTH + 1);
    }

    private String generateLongString(int stringLength)
    {
        StringBuilder buffer = new StringBuilder();
        for(int i = 0; i < stringLength ; i++)
        {
            buffer.append('x');
        }

        return buffer.toString();
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
        final org.apache.qpid.server.protocol.v0_10.transport.Header header = new org.apache.qpid.server.protocol.v0_10.transport.Header(deliveryProperties, messageProperties);
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
