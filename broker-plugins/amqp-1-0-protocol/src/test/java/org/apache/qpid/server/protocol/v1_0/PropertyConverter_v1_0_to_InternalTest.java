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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Footer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class PropertyConverter_v1_0_to_InternalTest extends UnitTestBase
{
    private MessageConverter_v1_0_to_Internal _messageConverter;
    private NamedAddressSpace _addressSpace;

    @BeforeAll
    void setUp()
    {
        _messageConverter = new MessageConverter_v1_0_to_Internal();
        _addressSpace = mock(NamedAddressSpace.class);
    }

    @Test
    void durableTrueConversion()
    {
        final Header header = new Header();
        header.setDurable(true);
        final Message_1_0 originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void durableFalseConversion()
    {
        final Header header = new Header();
        header.setDurable(false);
        final Message_1_0 originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void priorityConversion()
    {
        final byte priority = 7;
        final Header header = new Header();
        header.setPriority(UnsignedByte.valueOf(priority));
        final Message_1_0 originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(priority, (long) convertedMessage.getMessageHeader().getPriority(), "Unexpected priority");
    }

    @Test
    void absoluteExpiryTimeConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        final Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(expiryTime));
        final Message_1_0 originalMessage = createTestMessage(properties, arrivalTime);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(0, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    void TTLConversion()
    {
        final long ttl = 10000;
        final long arrivalTime = System.currentTimeMillis();
        final long expiryTime = arrivalTime + ttl;
        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));

        final Message_1_0 originalMessage = createTestMessage(header, arrivalTime);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(expiryTime, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentEncoding, convertedMessage.getMessageHeader().getEncoding(), "Unexpected content encoding");
    }

    @Test
    void messageIdStringConversion()
    {
        final String messageId = "testMessageId";
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId, convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdUuidConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId.toString(), convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1L);
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId.toString(), convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdBinaryConversion()
    {
        final byte[] data = new byte[]{(byte) 0xc3, 0x28};
        final Binary messageId = new Binary(data);
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId.toString(), convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    void correlationIdStringConversion()
    {
        final String correlationId = "testMessageCorrelationId";
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(correlationId, convertedMessage.getMessageHeader().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdUuidConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(correlationId.toString(), convertedMessage.getMessageHeader().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdUnsignedLongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1L);
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(correlationId.toString(), convertedMessage.getMessageHeader().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdBinaryConversion()
    {
        final byte[] data = new byte[]{(byte) 0xc3, 0x28};
        final Binary correlationId = new Binary(data);
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(correlationId.toString(), convertedMessage.getMessageHeader().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void userIdConversion()
    {
        final byte[] data = new byte[]{(byte) 0xc3, 0x28};
        final Binary userId = new Binary(data);
        final Properties properties = new Properties();
        properties.setUserId(userId);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(new String(data, UTF_8), convertedMessage.getMessageHeader().getUserId(), "Unexpected userId");
    }

    @Test
    void replyToConversion()
    {
        final String replyTo = "amq.direct/test";
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(replyTo, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    void creationTimeConversion()
    {
        final long creationTime = System.currentTimeMillis();
        final Properties properties = new Properties();
        properties.setCreationTime(new Date(creationTime));
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(creationTime, convertedMessage.getMessageHeader().getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void toConversionIntoToAndInitialRoutingAddressWhenToIncludesExchangeNameAndRoutingKey()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test";
        final String to = String.format("%s/%s", exchangeName, routingKey);
        final Properties properties = new Properties();
        properties.setTo(to);
        final Message_1_0 originalMessage = createTestMessage(properties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(to, convertedMessage.getTo(), "Unexpected to");
        assertEquals("", convertedMessage.getInitialRoutingAddress(), "Unexpected initialRoutingAddress");
    }

    @Test
    void applicationPropertiesConversion()
    {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty1", "testProperty1Value");
        properties.put("intProperty", 1);
        properties.put("nullProperty", null);
        final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        final Message_1_0 originalMessage = createTestMessage(applicationProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> headers = convertedMessage.getMessageHeader().getHeaderMap();
        assertEquals(properties, new HashMap<>(headers), "Unexpected headers");
    }

    private Message_1_0 createTestMessage(final Header header)
    {
        return createTestMessage(header, 0);
    }

    private Message_1_0 createTestMessage(final Header header, long arrivalTime)
    {
        return createTestMessage(header, new DeliveryAnnotations(Map.of()), new MessageAnnotations(Map.of()),
                new Properties(), new ApplicationProperties(Map.of()), arrivalTime, null);
    }

    private Message_1_0 createTestMessage(final Properties properties)
    {
        return createTestMessage(properties, 0L);
    }

    private Message_1_0 createTestMessage(final Properties properties, final long arrivalTime)
    {
        return createTestMessage(new Header(), new DeliveryAnnotations(Map.of()), new MessageAnnotations(Map.of()),
                properties, new ApplicationProperties(Map.of()), arrivalTime, null);
    }

    private Message_1_0 createTestMessage(final ApplicationProperties applicationProperties)
    {
        return createTestMessage(new Header(), new DeliveryAnnotations(Map.of()), new MessageAnnotations(Map.of()),
                new Properties(), applicationProperties, 0, null);
    }

    private Message_1_0 createTestMessage(final Header header,
                                          final DeliveryAnnotations deliveryAnnotations,
                                          final MessageAnnotations messageAnnotations,
                                          final Properties properties,
                                          final ApplicationProperties applicationProperties,
                                          final long arrivalTime,
                                          final byte[] content)
    {
        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        final MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                deliveryAnnotations.createEncodingRetainingSection(),
                messageAnnotations.createEncodingRetainingSection(),
                properties.createEncodingRetainingSection(),
                applicationProperties.createEncodingRetainingSection(),
                new Footer(Map.of()).createEncodingRetainingSection(),
                arrivalTime,
                content == null ? 0 : content.length);
        when(storedMessage.getMetaData()).thenReturn(metaData);

        if (content != null)
        {
            final Binary binary = new Binary(content);
            final DataSection dataSection = new Data(binary).createEncodingRetainingSection();
            final QpidByteBuffer qbb = dataSection.getEncodedForm();
            final int length = qbb.remaining();
            when(storedMessage.getContentSize()).thenReturn(length);
            when(storedMessage.getContent(0, length)).thenReturn(qbb);
        }
        else
        {
            when(storedMessage.getContentSize()).thenReturn(0);
            when(storedMessage.getContent(0, 0)).thenReturn(QpidByteBuffer.emptyQpidByteBuffer());
        }
        return new Message_1_0(storedMessage);
    }
}
