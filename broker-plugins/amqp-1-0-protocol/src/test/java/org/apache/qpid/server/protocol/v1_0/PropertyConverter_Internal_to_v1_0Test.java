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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

class PropertyConverter_Internal_to_v1_0Test extends UnitTestBase
{
    private MessageConverter_Internal_to_v1_0 _messageConverter;
    private NamedAddressSpace _addressSpace;

    @BeforeAll
    void setUp()
    {
        _messageConverter = new MessageConverter_Internal_to_v1_0();
        _addressSpace = mock(NamedAddressSpace.class);
    }

    @Test
    void durableTrueConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage originalMessage = createTestMessage(header, null, true);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void durableFalseConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage originalMessage = createTestMessage(header, null, false);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void priorityConversion()
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        byte priority = (byte) 7;
        when(header.getPriority()).thenReturn(priority);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(priority, (long) convertedMessage.getMessageHeader().getPriority(), "Unexpected priority");
    }

    @Test
    void expirationConversion() throws InterruptedException
    {
        final long ttl = 10000;
        final long expiryTime = System.currentTimeMillis() + ttl;
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getExpiration()).thenReturn(expiryTime);
        final InternalMessage originalMessage = createTestMessage(header);
        Thread.sleep(1L);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Long convertedTtl = MessageConverter_from_1_0.getTtl(convertedMessage);
        assertNotNull(convertedTtl);
        assertEquals(expiryTime - originalMessage.getArrivalTime(), convertedTtl.longValue(), "Unexpected TTL");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getEncoding()).thenReturn(contentEncoding);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Symbol convertedContentEncoding = MessageConverter_from_1_0.getContentEncoding(convertedMessage);
        assertEquals(contentEncoding, convertedContentEncoding.toString(), "Unexpected content encoding");
    }

    @Test
    void messageIdStringConversion()
    {
        final String messageId = "testMessageId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Object convertedMessageId = MessageConverter_from_1_0.getMessageId(convertedMessage);
        assertEquals(messageId, convertedMessageId, "Unexpected messageId");
    }

    @Test
    void messageIdUuidConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId.toString());
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Object convertedMessageId = MessageConverter_from_1_0.getMessageId(convertedMessage);
        assertEquals(messageId, convertedMessageId, "Unexpected messageId");
    }

    @Test
    void messageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1L);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(messageId.toString());
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Object convertedMessageId = MessageConverter_from_1_0.getMessageId(convertedMessage);
        assertEquals(messageId, convertedMessageId, "Unexpected messageId");
    }

    @Test
    void correlationIdStringConversion()
    {
        final String correlationId = "testCorrelationId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Object convertedCorrelationId = MessageConverter_from_1_0.getCorrelationId(convertedMessage);
        assertEquals(correlationId, convertedCorrelationId, "Unexpected messageId");
    }

    @Test
    void correlationIdUuidConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId.toString());
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Object convertedCorrelationId = MessageConverter_from_1_0.getCorrelationId(convertedMessage);
        assertEquals(correlationId, convertedCorrelationId, "Unexpected correlationId");
    }

    @Test
    void correlationIdUnsignedLongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1L);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getCorrelationId()).thenReturn(correlationId.toString());
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Object convertedCorrelationId = MessageConverter_from_1_0.getCorrelationId(convertedMessage);
        assertEquals(correlationId, convertedCorrelationId, "Unexpected correlationId");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "testUserId";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getUserId()).thenReturn(userId);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Binary convertedUserId = MessageConverter_from_1_0.getUserId(convertedMessage);
        assertArrayEquals(userId.getBytes(UTF_8), convertedUserId.getArray(), "Unexpected userId");
    }

    @Test
    void replyToConversion()
    {
        final String replyTo = "amq.direct/test";
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getReplyTo()).thenReturn(replyTo);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final String convertedReplyTo = MessageConverter_from_1_0.getReplyTo(convertedMessage);
        assertEquals(replyTo, convertedReplyTo, "Unexpected replyTo");
    }

    @Test
    void toConversion()
    {
        final String to = "amq.direct/test";
        final InternalMessage originalMessage = createTestMessage(to);
        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(to, convertedMessage.getTo(), "Unexpected to");
    }

    @Test
    void timestampConversion()
    {
        final long timestamp = System.currentTimeMillis();
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getTimestamp()).thenReturn(timestamp);
        final InternalMessage originalMessage = createTestMessage(header);

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Date creationTime = MessageConverter_from_1_0.getCreationTime(convertedMessage);
        assertNotNull(creationTime, "timestamp not converted");
        assertEquals(timestamp, creationTime.getTime(), "Unexpected timestamp");
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

        final Message_1_0 convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> convertedHeaders = convertedMessage.getApplicationPropertiesSection().getValue();
        assertEquals(properties, new HashMap<>(convertedHeaders), "Unexpected application properties");
    }

    @Test
    void headersConversionWithNonSimpleTypes()
    {
        final Map<String, Object> properties = Map.of("listProperty", List.of());
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
                                              final byte[] content,
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
