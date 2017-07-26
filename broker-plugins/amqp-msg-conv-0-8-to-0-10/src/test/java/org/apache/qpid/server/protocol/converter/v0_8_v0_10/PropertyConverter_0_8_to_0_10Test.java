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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
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
import org.apache.qpid.test.utils.QpidTestCase;

public class PropertyConverter_0_8_to_0_10Test extends QpidTestCase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_8_to_0_10 _messageConverter;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_8_to_0_10();
    }

    public void testContentTypeConversion()
    {
        String contentType = "test-content-type";
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setContentType(contentType);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected content type", contentType, messageProperties.getContentType());
    }

    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setEncoding(contentEncoding);
        AMQMessage message = createTestMessage(basicContentHeaderProperties, new byte[]{(byte) 1}, 0);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected content encoding", contentEncoding, messageProperties.getContentEncoding());
    }

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
        assertEquals("Unexpected application applicationProperties", headers, new HashMap<>(applicationProperties));
    }

    public void testPersistentDeliveryModeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected deliveryMode", MessageDeliveryMode.PERSISTENT, deliveryProperties.getDeliveryMode());
    }

    public void testNonPersistentDeliveryModeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected deliveryMode", MessageDeliveryMode.NON_PERSISTENT, deliveryProperties.getDeliveryMode());
    }

    public void testPriorityConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte priority = 5;
        basicContentHeaderProperties.setPriority(priority);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected priority", priority, deliveryProperties.getPriority().getValue());
    }

    public void testCorrelationIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String correlationId = "testCorrelationId";
        basicContentHeaderProperties.setCorrelationId(AMQShortString.valueOf(correlationId));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected correlationId", correlationId, new String(messageProperties.getCorrelationId(), UTF_8));
    }

    public void testApplicationIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String applicationId = "testApplicationId";
        basicContentHeaderProperties.setAppId(applicationId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected applicationId", applicationId, new String(messageProperties.getAppId(), UTF_8));
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct://amq.direct/destination_name/queue_name?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected reply-to exchange", "amq.direct", messageProperties.getReplyTo().getExchange());
        assertEquals("Unexpected reply-to routing-key", "test_routing_key", messageProperties.getReplyTo().getRoutingKey());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed2()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct://amq.direct//queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected reply-to exchange", "amq.direct", messageProperties.getReplyTo().getExchange());
        assertEquals("Unexpected reply-to routing-key", "queue_name", messageProperties.getReplyTo().getRoutingKey());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed3()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct:////queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertNull("Unexpected reply-to exchange",  messageProperties.getReplyTo().getExchange());
        assertEquals("Unexpected reply-to routing-key", "queue_name", messageProperties.getReplyTo().getRoutingKey());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed4()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct:////?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertNull("Unexpected reply-to exchange", messageProperties.getReplyTo().getExchange());
        assertEquals("Unexpected reply-to routing-key", "test_routing_key", messageProperties.getReplyTo().getRoutingKey());
    }

    public void testReplyToConversionWhenNonBindingURLFormatIsUsed()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "test";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertFalse("Unexpected reply-to exchange", messageProperties.getReplyTo().hasExchange());
        assertEquals("Unexpected reply-to routing-key", "test", messageProperties.getReplyTo().getRoutingKey());
    }

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
        assertEquals("Unexpected TTL", ttl, deliveryProperties.getTtl());
        assertEquals("Unexpected expiration", expiration, deliveryProperties.getExpiration());
    }

    public void testUuidMessageIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId(messageId.toString());
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId", messageId, messageProperties.getMessageId());
    }

    public void testUuidMessageIdWithPrefixConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId("ID:" + messageId.toString());
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId", messageId, messageProperties.getMessageId());
    }

    public void testNonUuidMessageIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String messageId = "testMessageId";
        basicContentHeaderProperties.setMessageId(messageId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("non-UUID messageId should not be converted");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testTimestampConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis() - 10000;
        basicContentHeaderProperties.setTimestamp(timestamp);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected timestamp", timestamp, deliveryProperties.getTimestamp());
    }

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
        assertEquals("Unexpected x-jms-type in application headers", type, applicationProperties.get("x-jms-type"));
    }

    public void testUserIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String userId = "test-userId";
        basicContentHeaderProperties.setUserId(userId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected user-id", userId, new String(messageProperties.getUserId(), UTF_8));
    }

    public void testPublishInfoExchangeConversion()
    {
        final String testExchange = "testExchange";
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setExchange(AMQShortString.valueOf(testExchange));

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", testExchange, deliveryProperties.getExchange());
    }

    public void testPublishInfoRoutingKeyConversion()
    {
        final String testRoutingKey = "testRoutingKey";
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected routing-key", testRoutingKey, deliveryProperties.getRoutingKey());
    }

    public void testPublishInfoImmediateTrueConversion()
    {
        final boolean immediate = true;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setImmediate(immediate);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected immediate flag", immediate, deliveryProperties.getImmediate());
    }

    public void testPublishInfoImmediateFalseConversion()
    {
        final boolean immediate = false;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setImmediate(immediate);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected immediate flag", immediate, deliveryProperties.getImmediate());
    }

    public void testPublishInfoMandatoryTrueConversion()
    {
        final boolean mandatory = true;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setMandatory(mandatory);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected discard-unroutable flag", !mandatory, deliveryProperties.getDiscardUnroutable());
    }

    public void testPublishInfoMandatoryFalseConversion()
    {
        final boolean mandatory = false;
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setMandatory(mandatory);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected discard-unroutable flag", !mandatory, deliveryProperties.getDiscardUnroutable());
    }

    public void testContentLengthConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte[] content = {(byte) 1, (byte) 2};
        AMQMessage message = createTestMessage(basicContentHeaderProperties, content, 0);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected content-length", content.length, messageProperties.getContentLength());
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
            when(storedMessage.getContent(0, content.length)).thenReturn(Collections.singleton(QpidByteBuffer.wrap(
                    content)));
        }

        return new AMQMessage(storedMessage);
    }
}
