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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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

public class PropertyConverter_v1_0_to_InternalTest extends UnitTestBase
{
    private MessageConverter_v1_0_to_Internal _messageConverter;
    private NamedAddressSpace _addressSpace;

    @Before
    public void setUp() throws Exception
    {
        _messageConverter = new MessageConverter_v1_0_to_Internal();
        _addressSpace = mock(NamedAddressSpace.class);
    }

    @Test
    public void testDurableTrueConversion()
    {
        final Header header = new Header();
        header.setDurable(true);
        final Message_1_0 originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue("Unexpected persistence of message", convertedMessage.isPersistent());
        assertTrue("Unexpected persistence of meta data",
                          convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testDurableFalseConversion()
    {
        final Header header = new Header();
        header.setDurable(false);
        final Message_1_0 originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertFalse("Unexpected persistence of message", convertedMessage.isPersistent());
        assertFalse("Unexpected persistence of meta data",
                           convertedMessage.getStoredMessage().getMetaData().isPersistent());
    }

    @Test
    public void testPriorityConversion()
    {
        final byte priority = 7;
        final Header header = new Header();
        header.setPriority(UnsignedByte.valueOf(priority));
        final Message_1_0 originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected priority",
                            (long) priority,
                            (long) convertedMessage.getMessageHeader().getPriority());

    }

    @Test
    public void testAbsoluteExpiryTimeConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(expiryTime));
        Message_1_0 originalMessage = createTestMessage(properties, arrivalTime);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected expiration", (long) 0, convertedMessage.getMessageHeader().getExpiration());
    }

    @Test
    public void testTTLConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));

        Message_1_0 originalMessage = createTestMessage(header, arrivalTime);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected expiration", expiryTime, convertedMessage.getMessageHeader().getExpiration());
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected content encoding",
                            contentEncoding,
                            convertedMessage.getMessageHeader().getEncoding());

    }

    @Test
    public void testMessageIdStringConversion()
    {
        final String messageId = "testMessageId";
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected messageId", messageId, convertedMessage.getMessageHeader().getMessageId());
    }

    @Test
    public void testMessageIdUuidConversion()
    {
        final UUID messageId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected messageId",
                            messageId.toString(),
                            convertedMessage.getMessageHeader().getMessageId());
    }

    @Test
    public void testMessageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1L);
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected messageId",
                            messageId.toString(),
                            convertedMessage.getMessageHeader().getMessageId());
    }

    @Test
    public void testMessageIdBinaryConversion()
    {
        final byte[] data = new byte[]{(byte) 0xc3, 0x28};
        final Binary messageId = new Binary(data);
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected messageId",
                            messageId.toString(),
                            convertedMessage.getMessageHeader().getMessageId());
    }

    @Test
    public void testCorrelationIdStringConversion()
    {
        final String correlationId = "testMessageCorrelationId";
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected correlationId",
                            correlationId,
                            convertedMessage.getMessageHeader().getCorrelationId());
    }

    @Test
    public void testCorrelationIdUuidConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected correlationId",
                            correlationId.toString(),
                            convertedMessage.getMessageHeader().getCorrelationId());
    }

    @Test
    public void testCorrelationIdUnsignedLongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1L);
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected correlationId",
                            correlationId.toString(),
                            convertedMessage.getMessageHeader().getCorrelationId());
    }

    @Test
    public void testCorrelationIdBinaryConversion()
    {
        final byte[] data = new byte[]{(byte) 0xc3, 0x28};
        final Binary correlationId = new Binary(data);
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected correlationId",
                            correlationId.toString(),
                            convertedMessage.getMessageHeader().getCorrelationId());
    }

    @Test
    public void testUserIdConversion()
    {
        final byte[] data = new byte[]{(byte) 0xc3, 0x28};
        final Binary userId = new Binary(data);
        Properties properties = new Properties();
        properties.setUserId(userId);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected userId",
                            new String(data, UTF_8),
                            convertedMessage.getMessageHeader().getUserId());
    }

    @Test
    public void testReplyToConversion()
    {
        final String replyTo = "amq.direct/test";
        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected replyTo", replyTo, convertedMessage.getMessageHeader().getReplyTo());
    }

    @Test
    public void testCreationTimeConversion()
    {
        final long creationTime = System.currentTimeMillis();
        Properties properties = new Properties();
        properties.setCreationTime(new Date(creationTime));
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected timestamp", creationTime, convertedMessage.getMessageHeader().getTimestamp());
    }

    @Test
    public void testToConversionIntoToAndInitialRoutingAddressWhenToIncludesExchangeNameAndRoutingKey()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test";
        final String to = String.format("%s/%s", exchangeName, routingKey);
        Properties properties = new Properties();
        properties.setTo(to);
        Message_1_0 originalMessage = createTestMessage(properties);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals("Unexpected to", to, convertedMessage.getTo());
        assertEquals("Unexpected initialRoutingAddress", "", convertedMessage.getInitialRoutingAddress());
    }

    @Test
    public void testApplicationPropertiesConversion()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty1", "testProperty1Value");
        properties.put("intProperty", 1);
        properties.put("nullProperty", null);
        ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        Message_1_0 originalMessage = createTestMessage(applicationProperties);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> headers = convertedMessage.getMessageHeader().getHeaderMap();
        assertEquals("Unexpected headers", properties, new HashMap<>(headers));
    }


    private Message_1_0 createTestMessage(final Header header)
    {
        return createTestMessage(header, 0);
    }

    private Message_1_0 createTestMessage(final Header header, long arrivalTime)
    {
        return createTestMessage(header,
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 new MessageAnnotations(Collections.emptyMap()),
                                 new Properties(),
                                 new ApplicationProperties(Collections.emptyMap()),
                                 arrivalTime,
                                 null);
    }

    private Message_1_0 createTestMessage(final Properties properties)
    {
        return createTestMessage(properties, 0L);
    }

    private Message_1_0 createTestMessage(final Properties properties, final long arrivalTime)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 new MessageAnnotations(Collections.emptyMap()),
                                 properties,
                                 new ApplicationProperties(Collections.emptyMap()),
                                 arrivalTime,
                                 null);
    }

    private Message_1_0 createTestMessage(final ApplicationProperties applicationProperties)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 new MessageAnnotations(Collections.emptyMap()),
                                 new Properties(),
                                 applicationProperties,
                                 0,
                                 null);
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
        MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                                                               deliveryAnnotations.createEncodingRetainingSection(),
                                                               messageAnnotations.createEncodingRetainingSection(),
                                                               properties.createEncodingRetainingSection(),
                                                               applicationProperties.createEncodingRetainingSection(),
                                                               new Footer(Collections.emptyMap()).createEncodingRetainingSection(),
                                                               arrivalTime,
                                                               content == null ? 0 : content.length);
        when(storedMessage.getMetaData()).thenReturn(metaData);

        if (content != null)
        {
            Binary binary = new Binary(content);
            DataSection dataSection = new Data(binary).createEncodingRetainingSection();
            QpidByteBuffer qbb = dataSection.getEncodedForm();
            int length = qbb.remaining();
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
