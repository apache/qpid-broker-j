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

@SuppressWarnings("unchecked")
class PropertyConverter_0_8_to_0_10Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_8_to_0_10 _messageConverter;

    @BeforeAll
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_8_to_0_10();
    }

    @Test
    void contentTypeConversion()
    {
        final String contentType = "test-content-type";
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setContentType(contentType);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(contentType, messageProperties.getContentType(), "Unexpected content type");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setEncoding(contentEncoding);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties, new byte[]{(byte) 1}, 0);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(contentEncoding, messageProperties.getContentEncoding(), "Unexpected content encoding");
    }

    @Test
    void headerConversion()
    {
        final Map<String, Object> headers = Map.of("testProperty1", "testProperty1Value", "intProperty", 1);
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationProperties = messageProperties.getApplicationHeaders();

        assertEquals(headers, Map.copyOf(applicationProperties), "Unexpected applicationProperties");
    }

    @Test
    void persistentDeliveryModeConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(MessageDeliveryMode.PERSISTENT, deliveryProperties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    void nonPersistentDeliveryModeConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(MessageDeliveryMode.NON_PERSISTENT, deliveryProperties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    void priorityConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte priority = 5;
        basicContentHeaderProperties.setPriority(priority);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(priority, (long) deliveryProperties.getPriority().getValue(), "Unexpected priority");
    }

    @Test
    void correlationIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String correlationId = "testCorrelationId";
        basicContentHeaderProperties.setCorrelationId(AMQShortString.valueOf(correlationId));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(correlationId, new String(messageProperties.getCorrelationId(), UTF_8), "Unexpected correlationId");
    }

    @Test
    void applicationIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String applicationId = "testApplicationId";
        basicContentHeaderProperties.setAppId(applicationId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(applicationId, new String(messageProperties.getAppId(), UTF_8), "Unexpected applicationId");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct://amq.direct/destination_name/queue_name?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals("amq.direct", messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("test_routing_key", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed2()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct://amq.direct//queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals("amq.direct", messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("queue_name", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed3()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct:////queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertNull(messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("queue_name", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed4()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct:////?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertNull(messageProperties.getReplyTo().getExchange(), "Unexpected reply-to exchange");
        assertEquals("test_routing_key", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    void replyToConversionWhenNonBindingURLFormatIsUsed()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "test";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertFalse(messageProperties.getReplyTo().hasExchange(), "Unexpected reply-to exchange");
        assertEquals("test", messageProperties.getReplyTo().getRoutingKey(), "Unexpected reply-to routing-key");
    }

    @Test
    void expirationConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;
        final long expiration = timestamp + ttl;
        basicContentHeaderProperties.setExpiration(expiration);
        basicContentHeaderProperties.setTimestamp(timestamp);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties, timestamp);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(ttl, deliveryProperties.getTtl(), "Unexpected TTL");
        assertEquals(expiration, deliveryProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void uuidMessageIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId(messageId.toString());
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void uuidMessageIdWithPrefixConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId("ID:" + messageId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void nonUuidMessageIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String messageId = "testMessageId";
        basicContentHeaderProperties.setMessageId("ID:" + messageId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(UUID.nameUUIDFromBytes(messageId.getBytes(UTF_8)), messageProperties.getMessageId(),
                "Unexpected message id");
    }

    @Test
    void timestampConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis() - 10000;
        basicContentHeaderProperties.setTimestamp(timestamp);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(timestamp, deliveryProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void typeConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String type = "test-type";
        basicContentHeaderProperties.setType(type);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationProperties = messageProperties.getApplicationHeaders();

        assertEquals(type, applicationProperties.get("x-jms-type"), "Unexpected x-jms-type in application headers");
    }

    @Test
    void userIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String userId = "test-userId";
        basicContentHeaderProperties.setUserId(userId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(userId, new String(messageProperties.getUserId(), UTF_8), "Unexpected user-id");
    }

    @Test
    void publishInfoExchangeConversion()
    {
        final String testExchange = "testExchange";
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setExchange(AMQShortString.valueOf(testExchange));
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
    }

    @Test
    void publishInfoRoutingKeyConversion()
    {
        final String testRoutingKey = "testRoutingKey";
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(testRoutingKey, deliveryProperties.getRoutingKey(), "Unexpected routing-key");
    }

    @Test
    void publishInfoImmediateTrueConversion()
    {
        final boolean immediate = true;
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setImmediate(immediate);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(immediate, deliveryProperties.getImmediate(), "Unexpected immediate flag");
    }

    @Test
    void publishInfoImmediateFalseConversion()
    {
        final boolean immediate = false;
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setImmediate(immediate);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(immediate, deliveryProperties.getImmediate(), "Unexpected immediate flag");
    }

    @Test
    void publishInfoMandatoryTrueConversion()
    {
        final boolean mandatory = true;
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setMandatory(mandatory);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        final Object expected = !mandatory;

        assertEquals(expected, deliveryProperties.getDiscardUnroutable(), "Unexpected discard-unroutable flag");
    }

    @Test
    void publishInfoMandatoryFalseConversion()
    {
        final boolean mandatory = false;
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setMandatory(mandatory);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        final Object expected = !mandatory;

        assertEquals(expected, deliveryProperties.getDiscardUnroutable(), "Unexpected discard-unroutable flag");
    }

    @Test
    void contentLengthConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte[] content = {(byte) 1, (byte) 2};
        final AMQMessage message = createTestMessage(basicContentHeaderProperties, content, 0);
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
                                         final long arrivalTime)
    {
        return createTestMessage(basicContentHeaderProperties, null, arrivalTime);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         final byte[] content,
                                         final long arrivalTime)
    {
        final ContentHeaderBody contentHeaderBody = mock(ContentHeaderBody.class);
        when(contentHeaderBody.getProperties()).thenReturn(basicContentHeaderProperties);

        final StoredMessage<MessageMetaData> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData())
                .thenReturn(new MessageMetaData(new MessagePublishInfo(), contentHeaderBody, arrivalTime));

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
