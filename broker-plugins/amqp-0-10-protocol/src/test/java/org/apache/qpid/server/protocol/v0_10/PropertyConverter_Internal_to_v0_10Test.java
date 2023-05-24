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

package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.message.internal.InternalMessageMetaDataType;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class PropertyConverter_Internal_to_v0_10Test extends UnitTestBase
{
    private static final int AMQP_0_8_SHORT_STRING_MAX_LENGTH = 255;

    private MessageConverter_Internal_to_v0_10 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @BeforeEach
    void setUp()
    {
        _addressSpace = mock(NamedAddressSpace.class);
        when(_addressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_Internal_to_v0_10();
    }

    @Test
    void persistentTrueConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage originalMessage = createTestMessage(header, null, true, System.currentTimeMillis());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(MessageDeliveryMode.PERSISTENT, convertedMessage.getHeader().getDeliveryProperties().getDeliveryMode(),
                "Unexpected delivery mode");

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void persistentFalseConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage originalMessage = createTestMessage(header, null, false, System.currentTimeMillis());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(MessageDeliveryMode.NON_PERSISTENT, convertedMessage.getHeader().getDeliveryProperties().getDeliveryMode(),
                "Unexpected delivery mode");
        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void priorityConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final byte priority = (byte) 7;
        when(header.getPriority()).thenReturn(priority);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(priority, (long) convertedMessage.getHeader().getDeliveryProperties().getPriority().getValue(),
                "Unexpected priority");

    }

    @Test
    void expirationConversion() throws IOException
    {
        final long ttl = 10000;
        final long arrivalTime = System.currentTimeMillis();
        final long expiryTime = arrivalTime + ttl;
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getExpiration()).thenReturn(expiryTime);
        final InternalMessage originalMessage = createTestMessage(header, arrivalTime);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(expiryTime, convertedMessage.getHeader().getDeliveryProperties().getExpiration(),
                "Unexpected expiration time");

        assertEquals(ttl, convertedMessage.getHeader().getDeliveryProperties().getTtl(), "Unexpected TTL");
    }

    @Test
    void contentEncodingConversion() throws IOException
    {
        final String contentEncoding = "my-test-encoding";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentEncoding, convertedMessage.getHeader().getMessageProperties().getContentEncoding(),
                "Unexpected content encoding");
    }

    @Test
    void longContentEncodingConversion() throws IOException
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
    void messageIdUUIDConversion() throws IOException
    {
        final UUID messageId = UUID.randomUUID();
        final String messageIdAsString = messageId.toString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn("ID:" + messageIdAsString);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId, convertedMessage.getHeader().getMessageProperties().getMessageId(),
                "Unexpected messageId");
    }

    @Test
    void messageIdStringConversion() throws IOException
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull(convertedMessage.getHeader().getMessageProperties().getMessageId(), "Unexpected messageId");
    }

    @Test
    void correlationIdConversion() throws IOException
    {
        final String correlationId = "testCorrelationId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertArrayEquals(correlationId.getBytes(UTF_8),
                convertedMessage.getHeader().getMessageProperties().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdConversionWhenLengthExceeds16Bits() throws IOException
    {
        final String correlationId = generateLongLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        final InternalMessage originalMessage = createTestMessage(header);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(originalMessage, _addressSpace),
                "Expected exception is not thrown");
    }

    @Test
    void userIdConversion() throws IOException
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertArrayEquals(userId.getBytes(UTF_8), convertedMessage.getHeader().getMessageProperties().getUserId(),
                "Unexpected userId");
    }

    @Test
    void userIdConversionWhenLengthExceeds16Bits() throws IOException
    {
        final String userId = generateLongLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull(convertedMessage.getHeader().getMessageProperties().getUserId(), "Unexpected userId");
    }

    @Test
    void timestampConversion() throws IOException
    {
        final long timestamp = System.currentTimeMillis();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getTimestamp()).thenReturn(timestamp);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(timestamp, convertedMessage.getHeader().getDeliveryProperties().getTimestamp(),
                "Unexpected timestamp");
    }

    @Test
    void arrivalTimeConversion() throws IOException
    {
        final long timestamp = System.currentTimeMillis();

        final InternalMessage originalMessage = createTestMessage(mock(AMQMessageHeader.class), timestamp);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(timestamp, convertedMessage.getHeader().getDeliveryProperties().getTimestamp(),
                "Unexpected timestamp");
    }

    @Test
    void headersConversion() throws IOException
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

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> convertedHeaders =
                convertedMessage.getHeader().getMessageProperties().getApplicationHeaders();
        assertEquals(properties, new HashMap<>(convertedHeaders), "Unexpected application properties");
    }

    @Test
    void headersConversionWithUnsupportedTypes() throws IOException
    {
        final Map<String, Object> properties = Map.of("bigDecimalProperty", new BigDecimal(1));
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
    void headersConversionWhenKeyLengthExceeds255() throws IOException
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
    void contentLengthConversion() throws IOException
    {
        final byte[] content = {(byte) 1, (byte) 2};

        final InternalMessage originalMessage = createTestMessage(mock(AMQMessageHeader.class), content, false, 0);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(content.length, convertedMessage.getHeader().getMessageProperties().getContentLength(),
                "Unexpected timestamp");
    }

    @Test
    void replyToConversionWhenQueueIsSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "myTestQueue";
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(replyTo),anyBoolean());
        when(header.getReplyTo()).thenReturn(replyTo);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("", convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(replyTo, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenExchangeIsSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "myTestExchange";
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(replyTo),anyBoolean());
        when(header.getReplyTo()).thenReturn(replyTo);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals(replyTo, convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals("", convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String exchangeName = "testExchangeName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(exchangeName),anyBoolean());
        when(header.getReplyTo()).thenReturn(replyTo);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals(exchangeName, convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(routingKey, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenReplyToCannotBeResolved() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "direct://amq.direct//test?routingkey='test'";
        when(header.getReplyTo()).thenReturn(replyTo);
        final InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("", convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(replyTo, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeAndRoutingKeyIsSpecified() throws IOException
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        final String to = testExchange + "/" + testRoutingKey;

        final InternalMessage message = createTestMessage(to);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testRoutingKey, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeIsSpecified() throws IOException
    {
        final String testExchange = "testExchange";

        final InternalMessage message = createTestMessage(testExchange);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals("", deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenQueueIsSpecified() throws IOException
    {
        final String testQueue = "testQueue";

        final InternalMessage message = createTestMessage(testQueue);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(testQueue), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testQueue, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsUnknown() throws IOException
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;

        final InternalMessage message = createTestMessage(globalAddress);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(globalAddress, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsKnown() throws IOException
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;

        final InternalMessage message = createTestMessage(globalAddress);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        when(_addressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(queueName, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeLengthExceeds255() throws IOException
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
    void toConversionWhenRoutingKeyLengthExceeds255() throws Exception
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
    void toConversionWhenDestinationIsSpecifiedButDoesNotExists() throws IOException
    {
        final String testDestination = "testDestination";
        final InternalMessage message = createTestMessage(testDestination);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testDestination, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    private InternalMessage createTestMessage(String to) throws IOException
    {
        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(mock(AMQMessageHeader.class));
        final StoredMessage<InternalMessageMetaData> handle =
                createInternalStoredMessage(null,false, internalMessageHeader);
        return new InternalMessage(handle, internalMessageHeader, null, to);
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header) throws IOException
    {
        return createTestMessage(header, null, false, System.currentTimeMillis());
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header, final long arrivalTime) throws IOException
    {
        return createTestMessage(header, null, false, arrivalTime);
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header,
                                              final byte[] content,
                                              final boolean persistent,
                                              final long arrivalTime) throws IOException
    {
        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(header, arrivalTime);
        final StoredMessage<InternalMessageMetaData> storedMessage =
                createInternalStoredMessage(content, persistent, internalMessageHeader);
        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(storedMessage));
    }

    private StoredMessage<InternalMessageMetaData> createInternalStoredMessage(final byte[] content,
                                                                               final boolean persistent,
                                                                               final InternalMessageHeader internalMessageHeader) throws IOException
    {
        final StoredMessage<InternalMessageMetaData> storedMessage = mock(StoredMessage.class);
        final int contentSize = content == null ? 0 : content.length;
        if (contentSize > 0)
        {
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 final ObjectOutputStream oos = new ObjectOutputStream(baos))
            {
                oos.writeObject(content);

                when(storedMessage.getContent(0, contentSize)).thenReturn(QpidByteBuffer.wrap(
                        baos.toByteArray()));
            }
        }
        when(storedMessage.getContentSize()).thenReturn(contentSize);
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(persistent, internalMessageHeader, contentSize);
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return storedMessage;
    }

    private String generateLongString()
    {
        return generateLongString(AMQP_0_8_SHORT_STRING_MAX_LENGTH + 1);
    }

    private String generateLongString(long stringLength)
    {
        final StringBuilder buffer = new StringBuilder();
        for (long i = 0; i < stringLength; i++)
        {
            buffer.append('x');
        }
        return buffer.toString();
    }

    private String generateLongLongString()
    {
        return generateLongString(1 << 16);
    }
}
