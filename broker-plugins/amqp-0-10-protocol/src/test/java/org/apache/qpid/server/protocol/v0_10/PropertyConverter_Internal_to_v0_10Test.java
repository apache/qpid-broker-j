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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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

public class PropertyConverter_Internal_to_v0_10Test extends UnitTestBase
{
    private static final int AMQP_0_8_SHORT_STRING_MAX_LENGTH = 255;
    private MessageConverter_Internal_to_v0_10 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @Before
    public void setUp() throws Exception
    {
        _addressSpace = mock(NamedAddressSpace.class);
        when(_addressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_Internal_to_v0_10();
    }

    @Test
    public void testPersistentTrueConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        InternalMessage originalMessage = createTestMessage(header, null, true, System.currentTimeMillis());

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected delivery mode",
                            MessageDeliveryMode.PERSISTENT,
                            convertedMessage.getHeader().getDeliveryProperties().getDeliveryMode());

        assertTrue("Unexpected persistence of message", convertedMessage.isPersistent());
        assertTrue("Unexpected persistence of meta data",
                          convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testPersistentFalseConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        InternalMessage originalMessage = createTestMessage(header, null, false, System.currentTimeMillis());

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected delivery mode",
                            MessageDeliveryMode.NON_PERSISTENT,
                            convertedMessage.getHeader().getDeliveryProperties().getDeliveryMode());
        assertFalse("Unexpected persistence of message", convertedMessage.isPersistent());
        assertFalse("Unexpected persistence of meta data",
                           convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testPriorityConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        byte priority = (byte) 7;
        when(header.getPriority()).thenReturn(priority);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected priority",
                            (long) priority,
                            (long) convertedMessage.getHeader().getDeliveryProperties().getPriority().getValue());

    }

    @Test
    public void testExpirationConversion() throws InterruptedException, IOException
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getExpiration()).thenReturn(expiryTime);
        InternalMessage originalMessage = createTestMessage(header, arrivalTime);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected expiration time",
                            expiryTime,
                            convertedMessage.getHeader().getDeliveryProperties().getExpiration());

        assertEquals("Unexpected TTL", ttl, convertedMessage.getHeader().getDeliveryProperties().getTtl());
    }

    @Test
    public void testContentEncodingConversion() throws IOException
    {
        String contentEncoding = "my-test-encoding";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected content encoding",
                            contentEncoding,
                            convertedMessage.getHeader().getMessageProperties().getContentEncoding());
    }

    @Test
    public void testLongContentEncodingConversion() throws IOException
    {
        String contentEncoding = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        InternalMessage originalMessage = createTestMessage(header);

        try
        {
            _messageConverter.convert(originalMessage, _addressSpace);
            fail("Expected exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testMessageIdUUIDConversion() throws IOException
    {
        UUID messageId = UUID.randomUUID();
        final String messageIdAsString = messageId.toString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn("ID:" + messageIdAsString);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected messageId",
                            messageId,
                            convertedMessage.getHeader().getMessageProperties().getMessageId());
    }

    @Test
    public void testMessageIdStringConversion() throws IOException
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull("Unexpected messageId", convertedMessage.getHeader().getMessageProperties().getMessageId());
    }

    @Test
    public void testCorrelationIdConversion() throws IOException
    {
        String correlationId = "testCorrelationId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue("Unexpected correlationId", Arrays.equals(correlationId.getBytes(UTF_8),
                                                                    convertedMessage.getHeader().getMessageProperties().getCorrelationId()));
    }

    @Test
    public void testCorrelationIdConversionWhenLengthExceeds16Bits() throws IOException
    {
        final String correlationId = generateLongLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        InternalMessage originalMessage = createTestMessage(header);

        try
        {
            _messageConverter.convert(originalMessage, _addressSpace);
            fail("Expected exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }


    @Test
    public void testUserIdConversion() throws IOException
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue("Unexpected userId", Arrays.equals(userId.getBytes(UTF_8),
                                                             convertedMessage.getHeader().getMessageProperties().getUserId()));
    }

    @Test
    public void testUserIdConversionWhenLengthExceeds16Bits() throws IOException
    {
        final String userId = generateLongLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull("Unexpected userId", convertedMessage.getHeader().getMessageProperties().getUserId());
    }

    @Test
    public void testTimestampConversion() throws IOException
    {
        final long timestamp = System.currentTimeMillis();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getTimestamp()).thenReturn(timestamp);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp",
                            timestamp,
                            convertedMessage.getHeader().getDeliveryProperties().getTimestamp());
    }

    @Test
    public void testArrivalTimeConversion() throws IOException
    {
        final long timestamp = System.currentTimeMillis();

        InternalMessage originalMessage = createTestMessage(mock(AMQMessageHeader.class), timestamp);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp",
                            timestamp,
                            convertedMessage.getHeader().getDeliveryProperties().getTimestamp());
    }

    @Test
    public void testHeadersConversion() throws IOException
    {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty1", "testProperty1Value");
        properties.put("intProperty", 1);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getHeaderNames()).thenReturn(properties.keySet());
        doAnswer(invocation ->
                 {
                     final String originalArgument = (String) (invocation.getArguments())[0];
                     return properties.get(originalArgument);
                 }).when(header).getHeader(any(String.class));
        InternalMessage originalMessage = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Map<String, Object> convertedHeaders =
                convertedMessage.getHeader().getMessageProperties().getApplicationHeaders();
        assertEquals("Unexpected application properties", properties, new HashMap<>(convertedHeaders));
    }

    @Test
    public void testHeadersConversionWithUnsupportedTypes() throws IOException
    {
        final Map<String, Object> properties = Collections.singletonMap("bigDecimalProperty", new BigDecimal(1));
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getHeaderNames()).thenReturn(properties.keySet());
        doAnswer(invocation ->
                 {
                     final String originalArgument = (String) (invocation.getArguments())[0];
                     return properties.get(originalArgument);
                 }).when(header).getHeader(any(String.class));
        InternalMessage originalMessage = createTestMessage(header);

        try
        {
            _messageConverter.convert(originalMessage, _addressSpace);
            fail("Expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }


    @Test
    public void testHeadersConversionWhenKeyLengthExceeds255() throws IOException
    {
        final Map<String, Object> properties = Collections.singletonMap(generateLongString(), "test");
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getHeaderNames()).thenReturn(properties.keySet());
        doAnswer(invocation ->
                 {
                     final String originalArgument = (String) (invocation.getArguments())[0];
                     return properties.get(originalArgument);
                 }).when(header).getHeader(any(String.class));
        InternalMessage originalMessage = createTestMessage(header);

        try
        {
            _messageConverter.convert(originalMessage, _addressSpace);
            fail("Expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testContentLengthConversion() throws IOException
    {
        byte[] content = {(byte) 1, (byte) 2};

        InternalMessage originalMessage = createTestMessage(mock(AMQMessageHeader.class),
                                                            content, false, 0);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp",
                            (long) content.length,
                            convertedMessage.getHeader().getMessageProperties().getContentLength());
    }

    @Test
    public void testReplyToConversionWhenQueueIsSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "myTestQueue";
        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(replyTo),anyBoolean());
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", "", convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", replyTo, convertedReplyTo.getRoutingKey());
    }

    @Test
    public void testReplyToConversionWhenExchangeIsSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "myTestExchange";
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(replyTo),anyBoolean());
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", replyTo, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", "", convertedReplyTo.getRoutingKey());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(exchangeName),anyBoolean());
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", exchangeName, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", routingKey, convertedReplyTo.getRoutingKey());
    }

    @Test
    public void testReplyToConversionWhenReplyToCannotBeResolved() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "direct://amq.direct//test?routingkey='test'";
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", "", convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", replyTo, convertedReplyTo.getRoutingKey());
    }


    @Test
    public void testToConversionWhenExchangeAndRoutingKeyIsSpecified() throws IOException
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        String to = testExchange + "/" + testRoutingKey;

        InternalMessage message = createTestMessage(to);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", testExchange, deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testRoutingKey, deliveryProperties.getRoutingKey());
    }

    @Test
    public void testToConversionWhenExchangeIsSpecified() throws IOException
    {
        final String testExchange = "testExchange";

        InternalMessage message = createTestMessage(testExchange);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", testExchange, deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", "", deliveryProperties.getRoutingKey());
    }

    @Test
    public void testToConversionWhenQueueIsSpecified() throws IOException
    {
        final String testQueue = "testQueue";

        InternalMessage message = createTestMessage(testQueue);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(testQueue), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testQueue, deliveryProperties.getRoutingKey());
    }

    @Test
    public void testToConversionWhenGlobalAddressIsUnknown() throws IOException
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;

        InternalMessage message = createTestMessage(globalAddress);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", globalAddress, deliveryProperties.getRoutingKey());
    }

    @Test
    public void testToConversionWhenGlobalAddressIsKnown() throws IOException
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;

        InternalMessage message = createTestMessage(globalAddress);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        when(_addressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", queueName, deliveryProperties.getRoutingKey());
    }

    @Test
    public void testToConversionWhenExchangeLengthExceeds255() throws IOException
    {
        final String testExchange = generateLongString();
        final String testRoutingKey = "testRoutingKey";

        String to = testExchange + "/" + testRoutingKey;

        InternalMessage message = createTestMessage(to);

        try
        {
            _messageConverter.convert(message, _addressSpace);
            fail("Exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testToConversionWhenRoutingKeyLengthExceeds255() throws Exception
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = generateLongString();

        String to = testExchange + "/" + testRoutingKey;

        InternalMessage message = createTestMessage(to);

        try
        {
            _messageConverter.convert(message, _addressSpace);
            fail("Exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    @Test
    public void testToConversionWhenDestinationIsSpecifiedButDoesNotExists() throws IOException
    {
        final String testDestination = "testDestination";

        InternalMessage message = createTestMessage(testDestination);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testDestination, deliveryProperties.getRoutingKey());
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
                                              byte[] content,
                                              final boolean persistent, final long arrivalTime) throws IOException
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
        int contentSize = content == null ? 0 : content.length;
        if (contentSize > 0)
        {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos))
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
        StringBuilder buffer = new StringBuilder();
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
