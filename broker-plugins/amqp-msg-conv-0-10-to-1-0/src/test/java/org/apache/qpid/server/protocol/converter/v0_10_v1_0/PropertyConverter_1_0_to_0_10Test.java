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
package org.apache.qpid.server.protocol.converter.v0_10_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.QpidByteBufferUtils;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
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
import org.apache.qpid.test.utils.QpidTestCase;

public class PropertyConverter_1_0_to_0_10Test extends QpidTestCase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_1_0_to_v0_10 _messageConverter;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _namedAddressSpace = mock(NamedAddressSpace.class);
        when(_namedAddressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_1_0_to_v0_10();
    }

    public void testContentEncodingConversion()
    {

        String contentEncoding = "my-test-encoding";
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected content encoding", contentEncoding, messageProperties.getContentEncoding());
    }

    public void testContentEncodingConversionWhenLengthExceeds255()
    {
        String contentEncoding = generateLongString();
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        Message_1_0 message = createTestMessage(properties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testApplicationPropertiesConversion()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty1", "testProperty1Value");
        properties.put("intProperty", 1);
        ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        Message_1_0 message = createTestMessage(applicationProperties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected headers", properties, new HashMap<>(headers));
    }

    public void testSubjectConversion()
    {
        final String subject = "testSubject";
        Properties properties = new Properties();
        properties.setSubject(subject);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected qpid.subject is missing from headers", subject, headers.get("qpid.subject"));
        assertEquals("Unexpected type", subject, headers.get("x-jms-type"));
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected routing-key", subject, deliveryProperties.getRoutingKey());
    }

    public void testSubjectDoesNoReplaceApplicationPropertyQpidSubject()
    {
        final String subject = "testSubject";
        Properties properties = new Properties();
        properties.setSubject(subject);
        final String qpidSubject = "testSubject2";
        Map<String, Object> applicationPropertiesMap = Collections.singletonMap("qpid.subject", qpidSubject);
        ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);

        Message_1_0 message = createTestMessage(new Header(),
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                properties,
                                                applicationProperties,
                                                0,
                                                null);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected qpid.subject is missing from headers", qpidSubject, headers.get("qpid.subject"));
        assertEquals("Unexpected type", subject, headers.get("x-jms-type"));
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected routing-key", subject, deliveryProperties.getRoutingKey());
    }

    public void testSubjectDoesNoReplaceApplicationPropertyXJMSType()
    {
        final String subject = "testSubject";
        Properties properties = new Properties();
        properties.setSubject(subject);
        final String jmsType = "testJmsType";
        Map<String, Object> applicationPropertiesMap = Collections.singletonMap("x-jms-type", jmsType);
        ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);

        Message_1_0 message = createTestMessage(new Header(),
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                properties,
                                                applicationProperties,
                                                0,
                                                null);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected qpid.subject is missing from headers", subject, headers.get("qpid.subject"));
        assertEquals("Unexpected type", jmsType, headers.get("x-jms-type"));
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected routing-key", subject, deliveryProperties.getRoutingKey());
    }

    public void testSubjectConversionWhenSubjectExceeds255()
    {
        final String subject = generateLongString();
        Properties properties = new Properties();
        properties.setSubject(subject);
        Message_1_0 message = createTestMessage(properties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("Expected conversion exception");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testDurableConversion()
    {
        final Header header = new Header();
        header.setDurable(true);
        Message_1_0 message = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected deliveryMode", MessageDeliveryMode.PERSISTENT, deliveryProperties.getDeliveryMode());
    }

    public void testNonDurableConversion()
    {
        final Header header = new Header();
        header.setDurable(false);
        Message_1_0 message = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected deliveryMode",
                     MessageDeliveryMode.NON_PERSISTENT,
                     deliveryProperties.getDeliveryMode());
    }

    public void testPriorityConversion()
    {
        final Header header = new Header();
        final byte priority = (byte) 7;
        header.setPriority(UnsignedByte.valueOf(priority));
        Message_1_0 message = createTestMessage(header);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected priority", priority, deliveryProperties.getPriority().getValue());
    }

    public void testCorrelationIdStringConversion()
    {
        final String correlationId = "testCorrelationId";
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertTrue("Unexpected correlationId",
                   Arrays.equals(correlationId.getBytes(), messageProperties.getCorrelationId()));
    }

    public void testCorrelationIdLongStringConversion()
    {
        final String correlationId = generateLongLongString();
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("Expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testCorrelationIdULongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1);
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertTrue("Unexpected correlationId",
                   Arrays.equals(longToBytes(correlationId.longValue()), messageProperties.getCorrelationId()));
    }

    public void testCorrelationIdUUIDConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final byte[] expectedBytes =
                longToBytes(correlationId.getMostSignificantBits(), correlationId.getLeastSignificantBits());
        assertTrue("Unexpected correlationId", Arrays.equals(expectedBytes, messageProperties.getCorrelationId()));
    }

    public void testCorrelationIdBinaryConversion()
    {
        final String testCorrelationId = "testCorrelationId";
        final Binary correlationId = new Binary(testCorrelationId.getBytes(UTF_8));
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertTrue("Unexpected correlationId",
                   Arrays.equals(testCorrelationId.getBytes(), messageProperties.getCorrelationId()));
    }

    public void testReplyToConversionWhenResultExceeds255()
    {
        final String replyTo = generateLongString() + "/" + generateLongString();
        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("expected exception not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testReplyToConversionWhenQueueIsSpecified() throws IOException
    {
        final String replyTo = "myTestQueue";
        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        when(_namedAddressSpace.getAttainedMessageDestination(replyTo)).thenReturn(queue);

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", "", convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", replyTo, convertedReplyTo.getRoutingKey());
    }

    public void testReplyToConversionWhenExchangeIsSpecified() throws IOException
    {
        final String replyTo = "myTestExchange";
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        when(_namedAddressSpace.getAttainedMessageDestination(replyTo)).thenReturn(exchange);

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", replyTo, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", "", convertedReplyTo.getRoutingKey());
    }

    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecified() throws IOException
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(_namedAddressSpace.getAttainedMessageDestination(exchangeName)).thenReturn(exchange);

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", exchangeName, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", routingKey, convertedReplyTo.getRoutingKey());
    }

    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecifiedAndGlobalPrefixIsUsed() throws IOException
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String globalPrefix = "/testPrefix";
        final String replyTo = String.format("%s/%s/%s", globalPrefix, exchangeName, routingKey);
        when(_namedAddressSpace.getLocalAddress(replyTo)).thenReturn(exchangeName + "/" + routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(_namedAddressSpace.getAttainedMessageDestination(exchangeName)).thenReturn(exchange);

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", exchangeName, convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", routingKey, convertedReplyTo.getRoutingKey());
    }

    public void testReplyToConversionWhenReplyToCannotBeResolved() throws IOException
    {
        final String replyTo = "direct://amq.direct//test?routingkey='test'";

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final ReplyTo convertedReplyTo =
                convertedMessage.getHeader().getMessageProperties().getReplyTo();
        assertEquals("Unexpected exchange", "", convertedReplyTo.getExchange());
        assertEquals("Unexpected routing key", replyTo, convertedReplyTo.getRoutingKey());
    }

    public void testTTLConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expectedExpiration = arrivalTime + ttl;
        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        Message_1_0 message = createTestMessage(header, arrivalTime);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected ttl", ttl, deliveryProperties.getTtl());
        assertEquals("Unexpected expiration", expectedExpiration, deliveryProperties.getExpiration());
    }

    public void testAbsoluteExpiryTimeConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(expiryTime));
        Message_1_0 message = createTestMessage(properties, arrivalTime);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected ttl", ttl, deliveryProperties.getTtl());
        assertEquals("Unexpected expiration", expiryTime, deliveryProperties.getExpiration());
    }

    public void testConversionOfTtlTakesPrecedenceOverAbsoluteExpiryTime()
    {
        long ttl = 10000;
        final long time = System.currentTimeMillis();
        long absoluteExpiryTime = time + ttl;
        long arrivalTime = time + 1;

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));

        Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(absoluteExpiryTime));

        Message_1_0 message = createTestMessage(header,
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                properties,
                                                new ApplicationProperties(Collections.emptyMap()),
                                                arrivalTime,
                                                null);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected ttl", ttl, deliveryProperties.getTtl());
        assertEquals("Unexpected expiration", arrivalTime + ttl, deliveryProperties.getExpiration());
    }

    public void testMessageIdStringConversion()
    {
        final String messageId = "testMessageId";
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId",
                     UUID.nameUUIDFromBytes(messageId.getBytes(UTF_8)),
                     messageProperties.getMessageId());
    }

    public void testMessageIdUuidAsStringConversion()
    {
        final UUID messageId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setMessageId(messageId.toString());
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId", messageId, messageProperties.getMessageId());
    }

    public void testMessageIdUUIDConversion()
    {
        final UUID messageId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId", messageId, messageProperties.getMessageId());
    }

    public void testMessageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1);
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId",
                     UUID.nameUUIDFromBytes(longToBytes(messageId.longValue())),
                     messageProperties.getMessageId());
    }

    public void testMessageIdBinaryConversion()
    {
        final String messageId = "testMessageId";
        Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId.getBytes(UTF_8)));
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId",
                     UUID.nameUUIDFromBytes(messageId.getBytes(UTF_8)),
                     messageProperties.getMessageId());
    }

    public void testMessageIdByteArrayConversion()
    {
        final byte[] messageId = "testMessageId".getBytes(UTF_8);
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId", UUID.nameUUIDFromBytes(messageId), messageProperties.getMessageId());
    }

    public void testMessageIdBinaryConversionWhenNonUtf8()
    {
        final byte[] messageId = new byte[]{(byte) 0xc3, 0x28};
        Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId));
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected messageId", UUID.nameUUIDFromBytes(messageId), messageProperties.getMessageId());
    }

    public void testCreationTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 10000;
        final long arrivalTime = timestamp + 1;
        Properties properties = new Properties();
        properties.setCreationTime(new Date(timestamp));
        Message_1_0 message = createTestMessage(new Header(),
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                properties,
                                                new ApplicationProperties(Collections.emptyMap()),
                                                arrivalTime,
                                                null);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected timestamp", timestamp, deliveryProperties.getTimestamp());
    }

    public void testArrivalTimeConversion()
    {
        final long arrivalTime = System.currentTimeMillis() - 10000;
        Message_1_0 message = createTestMessage(new Header(), arrivalTime);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected timestamp", arrivalTime, deliveryProperties.getTimestamp());
    }

    public void testUserIdConversion()
    {
        final String userId = "test-userId";
        Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertTrue("Unexpected user-id", Arrays.equals(userId.getBytes(UTF_8), messageProperties.getUserId()));
    }

    public void testUserIdConversionWhenLengthExceeds16Bit()
    {
        final String userId = generateLongLongString();
        Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertNull("Unexpected user-id", messageProperties.getUserId());
    }

    public void testGroupIdConversion()
    {
        String testGroupId = generateLongString();
        Properties properties = new Properties();
        properties.setGroupId(testGroupId);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected group-id", testGroupId, applicationHeaders.get("JMSXGroupID"));
    }

    public void testGroupIdDoesNotReplaceApplicationPropertiesJMSXGroupID()
    {
        String testGroupId = "group1";
        Properties properties = new Properties();
        properties.setGroupId(testGroupId);
        final String JMSXGroupID = "group2";
        Map<String, Object> applicationPropertiesMap = Collections.singletonMap("JMSXGroupID", JMSXGroupID);
        ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);

        Message_1_0 message = createTestMessage(new Header(),
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                properties,
                                                applicationProperties,
                                                0,
                                                null);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected group-id", JMSXGroupID, applicationHeaders.get("JMSXGroupID"));
    }

    public void testGroupSequenceConversion()
    {
        int testGroupSequence = 1;
        Properties properties = new Properties();
        properties.setGroupSequence(UnsignedInteger.valueOf(testGroupSequence));
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected group-id", testGroupSequence, applicationHeaders.get("JMSXGroupSeq"));
    }

    public void testGroupSequenceDoesNotReplaceApplicationPropertiesJMSXGroupSeq()
    {
        int testGroupSequence = 1;
        Properties properties = new Properties();
        properties.setGroupSequence(UnsignedInteger.valueOf(testGroupSequence));

        final int JMSXGroupSeq = 2;
        Map<String, Object> applicationPropertiesMap = Collections.singletonMap("JMSXGroupSeq", JMSXGroupSeq);
        ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);

        Message_1_0 message = createTestMessage(new Header(),
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                properties,
                                                applicationProperties,
                                                0,
                                                null);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();
        assertEquals("Unexpected JMSXGroupSeq", JMSXGroupSeq, applicationHeaders.get("JMSXGroupSeq"));
    }


    public void testApplicationPropertiesConversionWhenKeyLengthExceeds255()
    {
        Map<String, Object> properties = Collections.singletonMap("testProperty-" + generateLongString(), "testValue");
        ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        Message_1_0 message = createTestMessage(applicationProperties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("Exception is expected");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testToConversionWhenExchangeAndRoutingKeyIsSpecified()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        String to = testExchange + "/" + testRoutingKey;
        Properties properties = new Properties();
        properties.setTo(to);
        Message_1_0 message = createTestMessage(properties);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        when(_namedAddressSpace.getAttainedMessageDestination(testExchange)).thenReturn(exchange);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", testExchange, deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testRoutingKey, deliveryProperties.getRoutingKey());
    }

    public void testToConversionWhenExchangeIsSpecified()
    {
        final String testExchange = "testExchange";
        Properties properties = new Properties();
        properties.setTo(testExchange);
        Message_1_0 message = createTestMessage(properties);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        when(_namedAddressSpace.getAttainedMessageDestination(testExchange)).thenReturn(exchange);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", testExchange, deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", "", deliveryProperties.getRoutingKey());
    }

    public void testToConversionWhenExchangeIsSpecifiedAndSubjectIsSet()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        Properties properties = new Properties();
        properties.setTo(testExchange);
        properties.setSubject(testRoutingKey);
        Message_1_0 message = createTestMessage(properties);

        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        when(_namedAddressSpace.getAttainedMessageDestination(testExchange)).thenReturn(exchange);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", testExchange, deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testRoutingKey, deliveryProperties.getRoutingKey());
    }

    public void testToConversionWhenQueueIsSpecified()
    {
        final String testQueue = "testQueue";
        Properties properties = new Properties();
        properties.setTo(testQueue);
        Message_1_0 message = createTestMessage(properties);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        when(_namedAddressSpace.getAttainedMessageDestination(testQueue)).thenReturn(queue);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testQueue, deliveryProperties.getRoutingKey());
    }

    public void testToConversionWhenGlobalAddressIsUnknown()
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;
        Properties properties = new Properties();
        properties.setTo(globalAddress);
        Message_1_0 message = createTestMessage(properties);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        when(_namedAddressSpace.getAttainedMessageDestination(queueName)).thenReturn(queue);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", globalAddress, deliveryProperties.getRoutingKey());
    }

    public void testToConversionWhenGlobalAddressIsKnown()
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;
        Properties properties = new Properties();
        properties.setTo(globalAddress);
        Message_1_0 message = createTestMessage(properties);

        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        when(_namedAddressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);
        when(_namedAddressSpace.getAttainedMessageDestination(queueName)).thenReturn(queue);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", queueName, deliveryProperties.getRoutingKey());
    }

    public void testToConversionWhenExchangeLengthExceeds255()
    {
        final String testExchange = generateLongString();
        final String testRoutingKey = "testRoutingKey";

        String to = testExchange + "/" + testRoutingKey;
        Properties properties = new Properties();
        properties.setTo(to);
        Message_1_0 message = createTestMessage(properties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("Exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testToConversionWhenRoutingKeyLengthExceeds255()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = generateLongString();

        String to = testExchange + "/" + testRoutingKey;
        Properties properties = new Properties();
        properties.setTo(to);
        Message_1_0 message = createTestMessage(properties);

        try
        {
            _messageConverter.convert(message, _namedAddressSpace);
            fail("Exception is not thrown");
        }
        catch (MessageConversionException e)
        {
            // pass
        }
    }

    public void testToConversionWhenDestinationIsSpecifiedButDoesNotExists()
    {
        final String testDestination = "testDestination";
        Properties properties = new Properties();
        properties.setTo(testDestination);
        Message_1_0 message = createTestMessage(properties);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();
        assertEquals("Unexpected exchange", "", deliveryProperties.getExchange());
        assertEquals("Unexpected routing key", testDestination, deliveryProperties.getRoutingKey());
    }

    public void testContentToContentLengthConversion()
    {
        final byte[] content = new byte[]{0x31, 0x00, 0x10};
        Message_1_0 message = createTestMessage(new Header(),
                                                new DeliveryAnnotations(Collections.emptyMap()),
                                                new MessageAnnotations(Collections.emptyMap()),
                                                new Properties(),
                                                new ApplicationProperties(Collections.emptyMap()),
                                                0,
                                                content);

        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        assertEquals("Unexpected content length", content.length, messageProperties.getContentLength());
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
            List<QpidByteBuffer> qbbList = dataSection.getEncodedForm();
            int length = (int) QpidByteBufferUtils.remaining(qbbList);
            when(storedMessage.getContentSize()).thenReturn(length);
            when(storedMessage.getContent(0, length)).thenReturn(qbbList);
        }
        return new Message_1_0(storedMessage);
    }

    private String generateLongString()
    {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < 256; i++)
        {
            buffer.append('x');
        }

        return buffer.toString();
    }

    private String generateLongLongString()
    {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < 1 << 16; i++)
        {
            buffer.append('x');
        }

        return buffer.toString();
    }

    private byte[] longToBytes(long... x)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * x.length);
        for (long l : x)
        {
            buffer.putLong(l);
        }
        return buffer.array();
    }
}
