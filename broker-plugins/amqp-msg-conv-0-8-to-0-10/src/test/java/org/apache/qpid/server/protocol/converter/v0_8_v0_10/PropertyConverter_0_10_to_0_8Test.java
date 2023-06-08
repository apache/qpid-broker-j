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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.apache.qpid.server.protocol.v0_10.transport.Header;
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

@SuppressWarnings("unchecked")
class PropertyConverter_0_10_to_0_8Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_10_to_0_8 _messageConverter;

    @BeforeAll
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_10_to_0_8();
    }

    @Test
    void contentTypeConversion()
    {
        final String contentType = "test-content-type";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(contentType);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(contentType, properties.getContentType().toString(), "Unexpected content type");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentEncoding(contentEncoding);
        final MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), messageProperties, new byte[]{(byte)1}, 0);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(contentEncoding, properties.getEncoding().toString(), "Unexpected content encoding");
    }

    @Test
    void applicationHeadersConversion()
    {
        final Map<String, Object> headers = Map.of("testProperty1", "testProperty1Value", "intProperty", 1);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> applicationProperties = properties.getHeadersAsMap();

        assertEquals(headers, Map.copyOf(applicationProperties), "Unexpected headers");
    }

    @Test
    void applicationHeadersConversionWhenQpidSubjectIsPresent()
    {
        final String testSubject = "testSubject";
        final Map<String, Object> headers = Map.of("qpid.subject", testSubject);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> applicationProperties = properties.getHeadersAsMap();

        assertEquals(testSubject, applicationProperties.get("qpid.subject"), "Unexpected subject in application properties");
    }

    @Test
    void persistentDeliveryModeConversion()
    {
        final MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(deliveryMode.getValue(), (long) properties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    void nonPersistentDeliveryModeConversion()
    {
        final MessageDeliveryMode deliveryMode = MessageDeliveryMode.NON_PERSISTENT;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(deliveryMode);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(deliveryMode.getValue(), (long) properties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    void priorityConversion()
    {
        final byte priority = 5;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(priority, (long) properties.getPriority(), "Unexpected priority");
    }

    @Test
    void correlationIdConversion()
    {
        final String correlationId = "testCorrelationId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId.getBytes());
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(correlationId, properties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    void correlationIdConversionWhenLengthExceeds255()
    {
        final String correlationId = generateLongString();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId.getBytes());
        final MessageTransferMessage message = createTestMessage(messageProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Expected exception not thrown");
    }

    @Test
    void correlationIdConversionWhenNotString()
    {
        final byte[] correlationId = new byte[] {(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertArrayEquals(correlationId, properties.getCorrelationId().getBytes(), "Unexpected correlationId");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeySpecified()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final String expectedReplyTo = String.format("direct://%s//?routingkey='%s'", exchangeName, routingKey);

        assertEquals(expectedReplyTo, properties.getReplyTo().toString(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeSpecified()
    {
        final String exchangeName = "amq.direct";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, null));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final String expectedReplyTo = String.format("direct://%s//", exchangeName);

        assertEquals(expectedReplyTo, properties.getReplyTo().toString(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final String expectedReplyTo = String.format("direct:////?routingkey='%s'", routingKey);

        assertEquals(expectedReplyTo, properties.getReplyTo().toString(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeIsEmptyStringAndRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo("", routingKey));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final String expectedReplyTo = String.format("direct:////?routingkey='%s'", routingKey);

        assertEquals(expectedReplyTo, properties.getReplyTo().toString(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreNull()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, null));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertNull(properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenResultExceeds255()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(generateLongString(255), generateLongString(255)));
        final MessageTransferMessage message = createTestMessage(messageProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Expected exception not thrown");
    }

    @Test
    void expirationConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;
        final long expiration = timestamp + ttl;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExpiration(expiration);
        final MessageTransferMessage message = createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(expiration, properties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void TTLConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        final MessageTransferMessage message = createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(timestamp + ttl, properties.getExpiration(), "Unexpected expiration");
    }

   @Test
    void messageIdConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setMessageId(messageId);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("ID:" + messageId, properties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    void timestampConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        final long timestamp = System.currentTimeMillis() - 1000;
        deliveryProperties.setTimestamp(timestamp);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(timestamp, properties.getTimestamp(), "Unexpected creation timestamp");
    }

    @Test
    void arrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 1000;
        final MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), new MessageProperties(),  null, timestamp);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(timestamp, properties.getTimestamp(), "Unexpected creation timestamp");
    }

    @Test
    void jmsTypeConversion()
    {
        final String type = "test-type";
        final Map<String, Object> headers = Map.of("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> applicationProperties = properties.getHeadersAsMap();

        assertEquals(type, properties.getType().toString(), "Unexpected subject");
        assertFalse(applicationProperties.containsKey("x-jms-type"), "Unexpected x-jms-type in application properties");
    }

    @Test
    void jmsTypeConversionWhenLengthExceeds255()
    {
        final String type = generateLongString();
        final Map<String, Object> headers = Map.of("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        final MessageTransferMessage message = createTestMessage(messageProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is not thrown");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "test-userId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(userId, properties.getUserId().toString(), "Unexpected user-id");
    }

    @Test
    void userIdConversionExceeds255()
    {
        final String userId = generateLongString();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertNull(properties.getUserId(), "Unexpected user-id");
    }

    @Test
    void userIdConversionWhenNotUtf8()
    {
        final byte[] userId = new byte[] {(byte) 0xc3, 0x28};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId);
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertArrayEquals(userId, properties.getUserId().getBytes(), "Unexpected user-id");
    }

    @Test
    void exchangeConversion()
    {
        final String testExchange = "testExchange";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
    }

    @Test
    void routingKeyConversion()
    {
        final String testRoutingKey = "testRoutingKey";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setRoutingKey(testRoutingKey);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void immediateTrueConversion()
    {
        final boolean immediate = true;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setImmediate(immediate);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(immediate, messagePublishInfo.isImmediate(), "Unexpected immediate flag");
    }

    @Test
    void immediateFalseConversion()
    {
        final boolean immediate = false;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setImmediate(immediate);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(immediate, messagePublishInfo.isImmediate(), "Unexpected immediate flag");
    }

    @Test
    void discardUnroutableTrueConversion()
    {
        final boolean discardUnroutable = true;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDiscardUnroutable(discardUnroutable);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        final Object expected = !discardUnroutable;

        assertEquals(expected, messagePublishInfo.isMandatory(), "Unexpected mandatory flag");
    }

    @Test
    void discardUnroutableFalseConversion()
    {
        final boolean discardUnroutable = false;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDiscardUnroutable(discardUnroutable);
        final MessageTransferMessage message = createTestMessage(deliveryProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        final Object expected = !discardUnroutable;

        assertEquals(expected, messagePublishInfo.isMandatory(), "Unexpected mandatory flag");
    }

    @Test
    void applicationIdConversion()
    {
        final String applicationId = "testAppId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setAppId(applicationId.getBytes(UTF_8));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(applicationId, properties.getAppId().toString(), "Unexpected application id");
    }

    @Test
    void applicationIdConversionWhenLengthExceeds255()
    {
        final String appId = generateLongString();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setAppId(appId.getBytes(UTF_8));
        final MessageTransferMessage message = createTestMessage(messageProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties properties = convertedMessage.getContentHeaderBody().getProperties();

        assertNull(properties.getAppId(), "Unexpected application id");
    }

    private String generateLongString()
    {
        return generateLongString(AMQShortString.MAX_LENGTH + 1);
    }

    private String generateLongString(final int stringLength)
    {
        return "x".repeat(Math.max(0, stringLength));
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
        final Header header = new Header(deliveryProperties, messageProperties);
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
