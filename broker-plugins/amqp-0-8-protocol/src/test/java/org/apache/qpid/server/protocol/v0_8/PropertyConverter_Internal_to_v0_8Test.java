
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

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

public class PropertyConverter_Internal_to_v0_8Test extends UnitTestBase
{
    private MessageConverter_Internal_to_v0_8 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @Before
    public void setUp() throws Exception
    {
        _messageConverter = new MessageConverter_Internal_to_v0_8();
        _addressSpace = mock(NamedAddressSpace.class);
        when(_addressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
    }

    @Test
    public void testDurableTrueConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        InternalMessage originalMessage = createTestMessage(header, null, true);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected delivery mode",
                            (long) BasicContentHeaderProperties.PERSISTENT,
                            (long) convertedMessage.getContentHeaderBody().getProperties().getDeliveryMode());

        assertTrue("Unexpected persistence of message", convertedMessage.isPersistent());
        assertTrue("Unexpected persistence of meta data",
                          convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testDurableFalseConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        InternalMessage originalMessage = createTestMessage(header, null, false);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected delivery mode",
                            (long) BasicContentHeaderProperties.NON_PERSISTENT,
                            (long) convertedMessage.getContentHeaderBody().getProperties().getDeliveryMode());
        assertFalse("Unexpected persistence of message", convertedMessage.isPersistent());
        assertFalse("Unexpected persistence of meta data",
                           convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testPriorityConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        byte priority = (byte) 7;
        when(header.getPriority()).thenReturn(priority);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected priority",
                            (long) priority,
                            (long) convertedMessage.getContentHeaderBody().getProperties().getPriority());
    }

    @Test
    public void testExpirationConversion()
    {
        long ttl = 10000;
        long expiryTime = System.currentTimeMillis() + ttl;
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getExpiration()).thenReturn(expiryTime);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected expiration time",
                            expiryTime,
                            convertedMessage.getContentHeaderBody().getProperties().getExpiration());
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected content encoding",
                            contentEncoding,
                            convertedMessage.getContentHeaderBody().getProperties().getEncodingAsString());

    }

    @Test
    public void testLongContentEncodingConversion()
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
    public void testMessageIdConversion()
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected messageId",
                            messageId,
                            convertedMessage.getContentHeaderBody().getProperties().getMessageIdAsString());
    }

    @Test
    public void testMessageIdConversionWhenLengthExceeds255()
    {
        final String messageId = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull("Unexpected messageId",
                          convertedMessage.getContentHeaderBody().getProperties().getMessageId());

    }

    @Test
    public void testCorrelationIdConversionWhenLengthExceeds255()
    {
        final String correlationId = generateLongString();
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
    public void testUserIdConversion()
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected userId",
                            userId,
                            convertedMessage.getContentHeaderBody().getProperties().getUserIdAsString());
    }

    @Test
    public void testUserIdConversionWhenLengthExceeds255()
    {
        final String userId = generateLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull("Unexpected userId", convertedMessage.getContentHeaderBody().getProperties().getUserId());
    }

    @Test
    public void testTimestampConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getTimestamp()).thenReturn(timestamp);
        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp",
                            timestamp,
                            convertedMessage.getContentHeaderBody().getProperties().getTimestamp());
    }

    @Test
    public void testHeadersConversion()
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

        final AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Map<String, Object> convertedHeaders = convertedMessage.getContentHeaderBody().getProperties().getHeadersAsMap();
        assertEquals("Unexpected application properties", properties, new HashMap<>(convertedHeaders));
    }

    @Test
    public void testHeadersConversionWhenKeyLengthExceeds255()
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
    public void testReplyToConversionWhenQueueIsSpecified()
    {
        final String replyTo = "testQueue";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());

        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected reply-to",
                            "direct:////" + replyTo,
                            convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString());
    }

    @Test
    public void testReplyToConversionWhenExchangeIsSpecified()
    {
        final String replyTo = "testExchange";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        when(exchange.getType()).thenReturn(ExchangeDefaults.FANOUT_EXCHANGE_CLASS);

        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());

        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected reply-to",
                            "fanout://" + replyTo + "//",
                            convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchange";
        final String routingKey = "testKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getType()).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);

        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());

        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected reply-to",
                            "topic://" + exchangeName + "//?routingkey='" + routingKey + "'",
                            convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString());
    }

    @Test
    public void testReplyToConversionWhenNonExistingExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchange";
        final String routingKey = "testKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);

        InternalMessage originalMessage = createTestMessage(header);

        AMQMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected reply-to",
                            "direct:////?routingkey='" + replyTo + "'",
                            convertedMessage.getContentHeaderBody().getProperties().getReplyToAsString());
    }

    @Test
    public void testToConversionWhenExchangeAndRoutingKeyIsSpecified()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        String to = testExchange + "/" + testRoutingKey;

        InternalMessage message = createTestMessage(to);

        Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", testExchange, messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", testRoutingKey, messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testToConversionWhenExchangeIsSpecified()
    {
        final String testExchange = "testExchange";
        InternalMessage message = createTestMessage(testExchange);

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", testExchange, messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", "", messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testConversionWhenToIsUnsetButInitialRoutingKeyIsSet()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        InternalMessage message = createTestMessage("");
        final String testInitialRoutingAddress = testExchange + "/" + testRoutingKey;
        message.setInitialRoutingAddress(testInitialRoutingAddress);

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_addressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", testExchange, messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", testRoutingKey, messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testToConversionWhenQueueIsSpecified()
    {
        final String testQueue = "testQueue";
        InternalMessage message = createTestMessage(testQueue);

        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(testQueue), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", "", messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", testQueue, messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testToConversionWhenGlobalAddressIsUnknown()
    {
        final String globalPrefix = "/testPrefix";
        final String queueName = "testQueue";
        final String globalAddress = globalPrefix + "/" + queueName;

        InternalMessage message = createTestMessage(globalAddress);

        Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", "", messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", globalAddress, messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testToConversionWhenGlobalAddressIsKnown()
    {
        final String globalPrefix = "/testPrefix";
        final String queueName = "testQueue";
        final String globalAddress = globalPrefix + "/" + queueName;

        InternalMessage message = createTestMessage(globalAddress);

        Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_addressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        when(_addressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", "", messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", queueName, messagePublishInfo.getRoutingKey().toString());
    }

    @Test
    public void testToConversionWhenExchangeLengthExceeds255()
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
    public void testToConversionWhenRoutingKeyLengthExceeds255()
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
    public void testToConversionWhenDestinationIsSpecifiedButDoesNotExists()
    {
        final String testDestination = "testDestination";

        InternalMessage message = createTestMessage(testDestination);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _addressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("Unexpected exchange", "", messagePublishInfo.getExchange().toString());
        assertEquals("Unexpected routing key", testDestination, messagePublishInfo.getRoutingKey().toString());
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header)
    {
        return createTestMessage(header, null, false);
    }

    private InternalMessage createTestMessage(final AMQMessageHeader header,
                                              byte[] content,
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

    private InternalMessage createTestMessage(String to)
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
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < stringLength; i++)
        {
            buffer.append('x');
        }

        return buffer.toString();
    }
}
