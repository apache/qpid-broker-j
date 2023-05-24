
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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.message.internal.InternalMessageMetaDataType;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class PropertyConverter_Internal_to_v0_8Test extends UnitTestBase
{
    private MessageConverter_Internal_to_v0_8 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @BeforeEach
    void setUp() throws Exception
    {
        _messageConverter = new MessageConverter_Internal_to_v0_8();
        _addressSpace = mock(NamedAddressSpace.class);
        when(_addressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
    }

    @Test
    void durableTrueConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage originalMessage = createTestMessage(header, null, true);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(BasicContentHeaderProperties.PERSISTENT,
                (long) convertedMessage.getContentHeaderBody().getProperties().getDeliveryMode(),
                "Unexpected delivery mode");

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void durableFalseConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage originalMessage = createTestMessage(header, null, false);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(BasicContentHeaderProperties.NON_PERSISTENT,
                (long) convertedMessage.getContentHeaderBody().getProperties().getDeliveryMode(),
                "Unexpected delivery mode");
        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                               "Unexpected persistence of meta data");
    }

    @Test
    void priorityConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final byte priority = (byte) 7;
        when(header.getPriority()).thenReturn(priority);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(priority, (long) convertedMessage.getContentHeaderBody().getProperties().getPriority(),
                "Unexpected priority");
    }

    @Test
    void expirationConversion()
    {
        final long ttl = 10000;
        final long expiryTime = System.currentTimeMillis() + ttl;
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getExpiration()).thenReturn(expiryTime);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(expiryTime, convertedMessage.getContentHeaderBody().getProperties().getExpiration(),
                "Unexpected expiration time");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentEncoding, convertedMessage.getContentHeaderBody().getProperties().getEncodingAsString(),
                "Unexpected content encoding");

    }

    @Test
    void longContentEncodingConversion()
    {
        final String contentEncoding = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        final InternalMessage originalMessage = createTestMessage(header);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(originalMessage, _addressSpace),
                "Expected exception is not thrown");
    }

    @Test
    void messageIdConversion()
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId, convertedMessage.getContentHeaderBody().getProperties().getMessageIdAsString(),
                "Unexpected messageId");
    }

    @Test
    void messageIdConversionWhenLengthExceeds255()
    {
        final String messageId = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull(convertedMessage.getContentHeaderBody().getProperties().getMessageId(), "Unexpected messageId");
    }

    @Test
    void correlationIdConversionWhenLengthExceeds255()
    {
        final String correlationId = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        final InternalMessage originalMessage = createTestMessage(header);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(originalMessage, _addressSpace),
                "Expected exception is not thrown");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(userId, convertedMessage.getContentHeaderBody().getProperties().getUserIdAsString(),
                "Unexpected userId");
    }

    @Test
    void userIdConversionWhenLengthExceeds255()
    {
        final String userId = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull(convertedMessage.getContentHeaderBody().getProperties().getUserId(), "Unexpected userId");
    }

    @Test
    void timestampConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getTimestamp()).thenReturn(timestamp);
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(timestamp, convertedMessage.getContentHeaderBody().getProperties().getTimestamp(),
                "Unexpected timestamp");
    }

    @Test
    void headersConversion()
    {
        final Map<String, Object> properties = Map.of("testProperty1", "testProperty1Value",
                "intProperty", 1);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getHeaderNames()).thenReturn(properties.keySet());
        doAnswer(invocation ->
        {
            final String originalArgument = (String) (invocation.getArguments())[0];
            return properties.get(originalArgument);
        }).when(header).getHeader(any(String.class));
        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> convertedHeaders = convertedMessage.getContentHeaderBody().getProperties().getHeadersAsMap();
        assertEquals(properties, new HashMap<>(convertedHeaders), "Unexpected application properties");
    }

    @Test
    void headersConversionWhenKeyLengthExceeds255()
    {
        final Map<String, Object> properties = Map.of(generateLongString(), "test");
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getHeaderNames()).thenReturn(properties.keySet());
        doAnswer(invocation ->
        {
            final String originalArgument = (String) (invocation.getArguments())[0];
            return properties.get(originalArgument);
        }).when(header).getHeader(any(String.class));
        final InternalMessage originalMessage = createTestMessage(header);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(originalMessage, _addressSpace),
                "Expected exception not thrown");
    }

    @Test
    void replyToConversionWhenQueueIsSpecified()
    {
        final String replyTo = "testQueue";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());

        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("direct:////" + replyTo,
                convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeIsSpecified()
    {
        final String replyTo = "testExchange";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        when(exchange.getType()).thenReturn(ExchangeDefaults.FANOUT_EXCHANGE_CLASS);

        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());

        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("fanout://" + replyTo + "//",
                convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchange";
        final String routingKey = "testKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getType()).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);

        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());

        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("topic://" + exchangeName + "//?routingkey='" + routingKey + "'",
                convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenNonExistingExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchange";
        final String routingKey = "testKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);

        final InternalMessage originalMessage = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("direct:////?routingkey='" + replyTo + "'",
                convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void toConversionWhenExchangeAndRoutingKeyIsSpecified()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        final String to = testExchange + "/" + testRoutingKey;

        final InternalMessage message = createTestMessage(to);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(),
                "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeIsSpecified()
    {
        final String testExchange = "testExchange";
        final InternalMessage message = createTestMessage(testExchange);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals("", messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void conversionWhenToIsUnsetButInitialRoutingKeyIsSet()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        final InternalMessage message = createTestMessage("");
        final String testInitialRoutingAddress = testExchange + "/" + testRoutingKey;
        message.setInitialRoutingAddress(testInitialRoutingAddress);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenQueueIsSpecified()
    {
        final String testQueue = "testQueue";
        final InternalMessage message = createTestMessage(testQueue);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(testQueue), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testQueue, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsUnknown()
    {
        final String globalPrefix = "/testPrefix";
        final String queueName = "testQueue";
        final String globalAddress = globalPrefix + "/" + queueName;

        final InternalMessage message = createTestMessage(globalAddress);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(globalAddress, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsKnown()
    {
        final String globalPrefix = "/testPrefix";
        final String queueName = "testQueue";
        final String globalAddress = globalPrefix + "/" + queueName;

        final InternalMessage message = createTestMessage(globalAddress);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        when(_addressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(queueName, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeLengthExceeds255()
    {
        final String testExchange = generateLongString();
        final String testRoutingKey = "testRoutingKey";

        final String to = testExchange + "/" + testRoutingKey;

        final InternalMessage message = createTestMessage(to);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _addressSpace),
                "Exception is not thrown");
    }

    @Test
    void toConversionWhenRoutingKeyLengthExceeds255()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = generateLongString();

        final String to = testExchange + "/" + testRoutingKey;

        final InternalMessage message = createTestMessage(to);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _addressSpace),
                "Exception is not thrown");
    }

    @Test
    void toConversionWhenDestinationIsSpecifiedButDoesNotExists()
    {
        final String testDestination = "testDestination";

        final InternalMessage message = createTestMessage(testDestination);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testDestination, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header)
    {
        return createTestMessage(header, null, false);
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header,
                                              final byte[] content,
                                              final boolean persistent)
    {
        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(header);
        final int contentSize = content == null ? 0 : content.length;
        final StoredMessage<InternalMessageMetaData> storedMessage =
                createInternalStoredMessage(persistent, internalMessageHeader, contentSize);
        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(storedMessage));
    }

    private StoredMessage<InternalMessageMetaData> createInternalStoredMessage(final boolean persistent,
                                                                               final InternalMessageHeader internalMessageHeader,
                                                                               final int contentSize)
    {
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(persistent, internalMessageHeader, contentSize);
        final StoredMessage<InternalMessageMetaData> storedMessage = mock(StoredMessage.class);

        when(storedMessage.getMetaData()).thenReturn(metaData);
        when(storedMessage.getContentSize()).thenReturn(contentSize);
        return storedMessage;
    }

    private InternalMessage createTestMessage(final String to)
    {
        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(mock(AMQMessageHeader.class));
        final StoredMessage<InternalMessageMetaData> handle =
                createInternalStoredMessage(false, internalMessageHeader, 0);
        return new InternalMessage(handle, internalMessageHeader, null, to);
    }
    
    private String generateLongString()
    {
        return generateLongString(AMQShortString.MAX_LENGTH + 1);
    }

    private String generateLongString(int stringLength)
    {
        return "x".repeat(Math.max(0, stringLength));
    }
}
