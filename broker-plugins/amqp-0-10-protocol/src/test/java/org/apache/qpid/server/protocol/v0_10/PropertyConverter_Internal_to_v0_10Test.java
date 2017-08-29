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
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
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
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.QpidTestCase;

public class PropertyConverter_Internal_to_v0_10Test extends QpidTestCase
{
    private MessageConverter_Internal_to_v0_10 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _addressSpace = mock(NamedAddressSpace.class);
        when(_addressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_Internal_to_v0_10();
    }

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

    public void testPriorityConversion() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        byte priority = (byte) 7;
        when(header.getPriority()).thenReturn(priority);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected priority",
                     priority,
                     convertedMessage.getHeader().getDeliveryProperties().getPriority().getValue());
    }

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

        assertEquals("Unexpected TTL",
                     ttl,
                     convertedMessage.getHeader().getDeliveryProperties().getTtl());
    }

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

    public void testMessageIdStringConversion() throws IOException
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull("Unexpected messageId", convertedMessage.getHeader().getMessageProperties().getMessageId());
    }

    public void testCorrelationIdConversion() throws IOException
    {
        String correlationId = "testCorrelationId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue("Unexpected correlationId",
                   Arrays.equals(correlationId.getBytes(UTF_8),
                                 convertedMessage.getHeader().getMessageProperties().getCorrelationId()));
    }

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


    public void testUserIdConversion() throws IOException
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue("Unexpected userId",
                   Arrays.equals(userId.getBytes(UTF_8),
                                 convertedMessage.getHeader().getMessageProperties().getUserId()));
    }

    public void testUserIdConversionWhenLengthExceeds16Bits() throws IOException
    {
        final String userId = generateLongLongString();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertNull("Unexpected userId", convertedMessage.getHeader().getMessageProperties().getUserId());
    }

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

    public void testArrivalTimeConversion() throws IOException
    {
        final long timestamp = System.currentTimeMillis();

        InternalMessage originalMessage = createTestMessage(mock(AMQMessageHeader.class), timestamp);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp",
                     timestamp,
                     convertedMessage.getHeader().getDeliveryProperties().getTimestamp());
    }

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

    public void testContentLengthConversion() throws IOException
    {
        byte[] content = {(byte) 1, (byte) 2};

        InternalMessage originalMessage = createTestMessage(mock(AMQMessageHeader.class),
                                                            content, false, 0);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp",
                     content.length,
                     convertedMessage.getHeader().getMessageProperties().getContentLength());
    }

    public void testReplyToConversionWhenQueueIsSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "myTestQueue";
        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        when(_addressSpace.getAttainedMessageDestination(replyTo)).thenReturn(queue);
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", "", convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", replyTo, convertedReplyTo.getRoutingKey());
    }

    public void testReplyToConversionWhenExchangeIsSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String replyTo = "myTestExchange";
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        when(_addressSpace.getAttainedMessageDestination(replyTo)).thenReturn(exchange);
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", replyTo, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", "", convertedReplyTo.getRoutingKey());
    }

    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecified() throws IOException
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(_addressSpace.getAttainedMessageDestination(exchangeName)).thenReturn(exchange);
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        MessageTransferMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", exchangeName, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", routingKey, convertedReplyTo.getRoutingKey());
    }

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
        int contentSize = 0;
        final StoredMessage<InternalMessageMetaData> storedMessage = mock(StoredMessage.class);
        if (content != null)
        {

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos))
            {
                oos.writeObject(content);
                contentSize = baos.size();
                when(storedMessage.getContent(0, contentSize)).thenReturn(Collections.singletonList(QpidByteBuffer.wrap(
                        baos.toByteArray())));
            }
        }
        when(storedMessage.getContentSize()).thenReturn(contentSize);
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(persistent, internalMessageHeader, contentSize);
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(storedMessage));
    }

    private String generateLongString()
    {
        return generateLongString(AMQShortString.MAX_LENGTH + 1);
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
