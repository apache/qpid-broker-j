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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyConverter_0_8_to_0_10Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_8_to_0_10 _messageConverter;

    @BeforeAll
    public void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_8_to_0_10();
    }

    @Test
    public void testContentTypeConversion()
    {
        String contentType = "test-content-type";
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setContentType(contentType);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(contentType, messageProperties.getContentType(), "Unexpected content type");
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setEncoding(contentEncoding);
        AMQMessage message = createTestMessage(basicContentHeaderProperties, new byte[]{(byte) 1}, 0);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(contentEncoding, messageProperties.getContentEncoding(), "Unexpected content encoding");
    }

    @Test
    public void testHeaderConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("intProperty", 1);
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        Map<String, Object> applicationProperties = messageProperties.getApplicationHeaders();
        assertEquals(headers, new HashMap<>(applicationProperties), "Unexpected applicationProperties");
    }

    @Test
    public void testPersistentDeliveryModeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(MessageDeliveryMode.PERSISTENT, deliveryProperties.getDeliveryMode(), "Unexpected deliveryMode");

    }

    @Test
    public void testNonPersistentDeliveryModeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(MessageDeliveryMode.NON_PERSISTENT, deliveryProperties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    public void testPriorityConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte priority = 5;
        basicContentHeaderProperties.setPriority(priority);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(priority, (long) deliveryProperties.getPriority().getValue(), "Unexpected priority");
    }

    @Test
    public void testCorrelationIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String correlationId = "testCorrelationId";
        basicContentHeaderProperties.setCorrelationId(AMQShortString.valueOf(correlationId));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(correlationId, new String(messageProperties.getCorrelationId(), UTF_8), "Unexpected correlationId");
    }

    @Test
    public void testApplicationIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String applicationId = "testApplicationId";
        basicContentHeaderProperties.setAppId(applicationId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(applicationId, new String(messageProperties.getAppId(), UTF_8), "Unexpected applicationId");
    }

    @Test
    public void testReplyToConversionWhenBindingURLFormatIsUsed()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct://amq.direct/destination_name/queue_name?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("amq.direct", messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("test_routing_key", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    public void testReplyToConversionWhenBindingURLFormatIsUsed2()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct://amq.direct//queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("amq.direct", messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("queue_name", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    public void testReplyToConversionWhenBindingURLFormatIsUsed3()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct:////queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertNull(messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("queue_name", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    public void testReplyToConversionWhenBindingURLFormatIsUsed4()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct:////?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertNull(messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("test_routing_key", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    public void testReplyToConversionWhenNonBindingURLFormatIsUsed()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "test";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertFalse(messageProperties.getReplyTo().hasExchange(), "Unexpected reply-to exchange");
        assertEquals("test", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    public void testExpirationConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;
        final long expiration = timestamp + ttl;
        basicContentHeaderProperties.setExpiration(expiration);
        basicContentHeaderProperties.setTimestamp(timestamp);
        AMQMessage message = createTestMessage(basicContentHeaderProperties, timestamp);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(ttl, deliveryProperties.getTtl(), "Unexpected TTL");
        assertEquals(expiration, deliveryProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    public void testUuidMessageIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId(messageId.toString());
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    public void testUuidMessageIdWithPrefixConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId("ID:" + messageId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    public void testNonUuidMessageIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String messageId = "testMessageId";
        basicContentHeaderProperties.setMessageId("ID:" + messageId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(UUID.nameUUIDFromBytes(messageId.getBytes(UTF_8)), messageProperties.getMessageId(),
                "Unexpected message id");
    }

    @Test
    public void testTimestampConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis() - 10000;
        basicContentHeaderProperties.setTimestamp(timestamp);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(timestamp, deliveryProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    public void testTypeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String type = "test-type";
        basicContentHeaderProperties.setType(type);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        Map<String, Object> applicationProperties = messageProperties.getApplicationHeaders();
        assertEquals(type, applicationProperties.get("x-jms-type"), "Unexpected x-jms-type in application headers");
    }

    @Test
    public void testUserIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String userId = "test-userId";
        basicContentHeaderProperties.setUserId(userId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(userId, new String(messageProperties.getUserId(), UTF_8), "Unexpected user-id");
    }

    @Test
    public void testPublishInfoExchangeConversion()
    {
        final String testExchange = "testExchange";
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setExchange(AMQShortString.valueOf(testExchange));

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
    }

    @Test
    public void testPublishInfoRoutingKeyConversion()
    {
        final String testRoutingKey = "testRoutingKey";
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(testRoutingKey, deliveryProperties.getRoutingKey(), "Unexpected routing-key");
    }

    @Test
    public void testPublishInfoImmediateTrueConversion()
    {
        final boolean immediate = true;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setImmediate(immediate);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(immediate, deliveryProperties.getImmediate(), "Unexpected immediate flag");
    }

    @Test
    public void testPublishInfoImmediateFalseConversion()
    {
        final boolean immediate = false;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setImmediate(immediate);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(immediate, deliveryProperties.getImmediate(), "Unexpected immediate flag");
    }

    @Test
    public void testPublishInfoMandatoryTrueConversion()
    {
        final boolean mandatory = true;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setMandatory(mandatory);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        final Object expected = !mandatory;
        assertEquals(expected, deliveryProperties.getDiscardUnroutable(), "Unexpected discard-unroutable flag");
    }

    @Test
    public void testPublishInfoMandatoryFalseConversion()
    {
        final boolean mandatory = false;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setMandatory(mandatory);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        final Object expected = !mandatory;
        assertEquals(expected, deliveryProperties.getDiscardUnroutable(), "Unexpected discard-unroutable flag");
    }

    @Test
    public void testContentLengthConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte[] content = {(byte) 1, (byte) 2};
        AMQMessage message = createTestMessage(basicContentHeaderProperties, content, 0);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals(content.length, messageProperties.getContentLength(), "Unexpected content-length");
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties)
    {
        return createTestMessage(basicContentHeaderProperties, null, 0);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         long arrivalTime)
    {
        return createTestMessage(basicContentHeaderProperties, null, arrivalTime);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         final byte[] content, final long arrivalTime)
    {
        final ContentHeaderBody contentHeaderBody = mock(ContentHeaderBody.class);
        when(contentHeaderBody.getProperties()).thenReturn(basicContentHeaderProperties);

        final StoredMessage<MessageMetaData> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData()).thenReturn(new MessageMetaData(new MessagePublishInfo(),
                                                                         contentHeaderBody,
                                                                         arrivalTime));

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

        return new AMQMessage(storedMessage);
    }
}
