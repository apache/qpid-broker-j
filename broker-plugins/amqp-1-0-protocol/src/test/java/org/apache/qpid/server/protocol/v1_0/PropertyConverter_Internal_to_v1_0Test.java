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
package org.apache.qpid.server.protocol.v1_0;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.message.internal.InternalMessageMetaDataType;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyConverter_Internal_to_v1_0Test extends UnitTestBase
{
    private MessageConverter_Internal_to_v1_0 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @Before
    public void setUp() throws Exception
    {
        _messageConverter = new MessageConverter_Internal_to_v1_0();
        _addressSpace = mock(NamedAddressSpace.class);
    }

    @Test
    public void testDurableTrueConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        InternalMessage originalMessage = createTestMessage(header, null, true);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue("Unexpected persistence of message", convertedMessage.isPersistent());
        assertTrue("Unexpected persistence of meta data",
                          convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testDurableFalseConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        InternalMessage originalMessage = createTestMessage(header, null, false);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

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

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected priority",
                            (long) priority,
                            (long) convertedMessage.getMessageHeader().getPriority());

    }

    @Test
    public void testExpirationConversion() throws InterruptedException
    {
        long ttl = 10000;
        long expiryTime = System.currentTimeMillis() + ttl;
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getExpiration()).thenReturn(expiryTime);
        InternalMessage originalMessage = createTestMessage(header);
        Thread.sleep(1L);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Long convertedTtl = MessageConverter_from_1_0.getTtl(convertedMessage);
        assertEquals("Unexpected TTL", expiryTime - originalMessage.getArrivalTime(), convertedTtl.longValue());
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Symbol convertedContentEncoding = MessageConverter_from_1_0.getContentEncoding(convertedMessage);
        assertEquals("Unexpected content encoding", contentEncoding, convertedContentEncoding.toString());
    }

    @Test
    public void testMessageIdStringConversion()
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Object convertedMessageId = MessageConverter_from_1_0.getMessageId(convertedMessage);
        assertEquals("Unexpected messageId", messageId, convertedMessageId);
    }

    @Test
    public void testMessageIdUuidConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId.toString());
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Object convertedMessageId = MessageConverter_from_1_0.getMessageId(convertedMessage);
        assertEquals("Unexpected messageId", messageId, convertedMessageId);
    }

    @Test
    public void testMessageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1L);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId.toString());
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Object convertedMessageId = MessageConverter_from_1_0.getMessageId(convertedMessage);
        assertEquals("Unexpected messageId", messageId, convertedMessageId);
    }

    @Test
    public void testCorrelationIdStringConversion()
    {
        final String correlationId = "testCorrelationId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Object convertedCorrelationId = MessageConverter_from_1_0.getCorrelationId(convertedMessage);
        assertEquals("Unexpected messageId", correlationId, convertedCorrelationId);
    }

    @Test
    public void testCorrelationIdUuidConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId.toString());
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Object convertedCorrelationId = MessageConverter_from_1_0.getCorrelationId(convertedMessage);
        assertEquals("Unexpected correlationId", correlationId, convertedCorrelationId);
    }

    @Test
    public void testCorrelationIdUnsignedLongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1L);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId.toString());
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Object convertedCorrelationId = MessageConverter_from_1_0.getCorrelationId(convertedMessage);
        assertEquals("Unexpected correlationId", correlationId, convertedCorrelationId);
    }

    @Test
    public void testUserIdConversion()
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Binary convertedUserId = MessageConverter_from_1_0.getUserId(convertedMessage);
        assertTrue("Unexpected userId", Arrays.equals(userId.getBytes(UTF_8), convertedUserId.getArray()));
    }

    @Test
    public void testReplyToConversion()
    {
        final String replyTo = "amq.direct/test";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        String convertedReplyTo = MessageConverter_from_1_0.getReplyTo(convertedMessage);
        assertEquals("Unexpected replyTo", replyTo, convertedReplyTo);
    }

    @Test
    public void testToConversion() throws IOException
    {
        final String to = "amq.direct/test";
        InternalMessage originalMessage = createTestMessage(to);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected to", to, convertedMessage.getTo());
    }

    @Test
    public void testTimestampConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getTimestamp()).thenReturn(timestamp);
        InternalMessage originalMessage = createTestMessage(header);

        Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Date creationTime = MessageConverter_from_1_0.getCreationTime(convertedMessage);
        assertNotNull("timestamp not converted", creationTime);
        assertEquals("Unexpected timestamp", timestamp, creationTime.getTime());
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

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        Map<String, Object> convertedHeaders = convertedMessage.getApplicationPropertiesSection().getValue();
        assertEquals("Unexpected application properties", properties, new HashMap<>(convertedHeaders));
    }

    @Test
    public void testHeadersConversionWithNonSimpleTypes()
    {
        final Map<String, Object> properties = Collections.singletonMap("listProperty", Collections.emptyList());
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

    private InternalMessage createTestMessage(String to)
    {
        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(mock(AMQMessageHeader.class));
        final StoredMessage<InternalMessageMetaData> handle =
                createInternalStoredMessage(null,false, internalMessageHeader);
        return new InternalMessage(handle, internalMessageHeader, null, to);
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
        final StoredMessage<InternalMessageMetaData> storedMessage =
                createInternalStoredMessage(content, persistent, internalMessageHeader);
        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(storedMessage));
    }

    private StoredMessage<InternalMessageMetaData> createInternalStoredMessage(final byte[] content,
                                                                               final boolean persistent,
                                                                               final InternalMessageHeader internalMessageHeader)
    {
        final int contentSize = content == null ? 0 : content.length;
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(persistent, internalMessageHeader, contentSize);
        final StoredMessage<InternalMessageMetaData> storedMessage = mock(StoredMessage.class);

        when(storedMessage.getMetaData()).thenReturn(metaData);
        when(storedMessage.getContentSize()).thenReturn(contentSize);
        return storedMessage;
    }
}
