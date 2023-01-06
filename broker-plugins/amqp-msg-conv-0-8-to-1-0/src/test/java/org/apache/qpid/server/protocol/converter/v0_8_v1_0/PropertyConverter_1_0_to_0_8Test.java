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
package org.apache.qpid.server.protocol.converter.v0_8_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Footer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyConverter_1_0_to_0_8Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_1_0_to_v0_8 _messageConverter;

    @BeforeAll
    public void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        when(_namedAddressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_1_0_to_v0_8();
    }

    @Test
    public void testContentEncodingConversion()
    {

        String contentEncoding = "my-test-encoding";
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(contentEncoding, convertedProperties.getEncoding().toString(), "Unexpected content encoding");

    }

    @Test
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

    @Test
    public void testApplicationPropertiesConversion()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty1", "testProperty1Value");
        properties.put("intProperty", 1);
        ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        Message_1_0 message = createTestMessage(applicationProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        assertEquals(properties, new HashMap<>(headers), "Unexpected headers");
    }

    @Test
    public void testApplicationPropertiesConversionWithUuid()
    {
        Map<String, Object> properties = new HashMap<>();
        final String key = "uuidProperty";
        properties.put(key, UUID.randomUUID());
        ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        Message_1_0 message = createTestMessage(applicationProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        assertEquals(properties.size(), (long) headers.size(), "Unexpected headers size");
        assertEquals(properties.get(key), UUID.fromString((String) headers.get(key)), "Unexpected headers");
    }

    @Test
    public void testApplicationPropertiesConversionWithDate()
    {
        Map<String, Object> properties = new HashMap<>();
        final String key = "dateProperty";
        properties.put(key, new Date());
        ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        Message_1_0 message = createTestMessage(applicationProperties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        assertEquals(properties.size(), (long) headers.size(), "Unexpected headers size");
        assertEquals(properties.get(key), new Date((Long) headers.get(key)), "Unexpected headers");
    }

    @Test
    public void testSubjectConversion()
    {
        final String subject = "testSubject";
        Properties properties = new Properties();
        properties.setSubject(subject);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        assertEquals(subject, headers.get("qpid.subject"), "Unexpected qpid.subject is missing from headers");
        assertEquals(subject, convertedProperties.getType().toString(), "Unexpected type");
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();
        assertEquals(subject, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing-key");
    }

    @Test
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

    @Test
    public void testDurableConversion()
    {
        final Header header = new Header();
        header.setDurable(true);
        Message_1_0 message = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(BasicContentHeaderProperties.PERSISTENT, (long) convertedProperties.getDeliveryMode(),
                "Unexpected deliveryMode");
    }

    @Test
    public void testNonDurableConversion()
    {
        final Header header = new Header();
        header.setDurable(false);
        Message_1_0 message = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(BasicContentHeaderProperties.NON_PERSISTENT, (long) convertedProperties.getDeliveryMode(),
                "Unexpected deliveryMode");
    }

    @Test
    public void testPriorityConversion()
    {
        final Header header = new Header();
        final byte priority = (byte) 7;
        header.setPriority(UnsignedByte.valueOf(priority));
        Message_1_0 message = createTestMessage(header);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(priority, (long) convertedProperties.getPriority(), "Unexpected priority");
    }

    @Test
    public void testCorrelationIdStringConversion()
    {
        final String correlationId = "testCorrelationId";
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(correlationId, convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    public void testCorrelationIdLongStringConversion()
    {
        final String correlationId = generateLongString();
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

    @Test
    public void testCorrelationIdULongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1);
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(correlationId.toString(), convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    public void testCorrelationIdUUIDConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(correlationId.toString(), convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    public void testCorrelationIdBinaryConversion()
    {
        final String testCorrelationId = "testCorrelationId";
        final Binary correlationId = new Binary(testCorrelationId.getBytes(UTF_8));
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(testCorrelationId, convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    public void testCorrelationIdBinaryConversionWhenNotUtf8()
    {
        final byte[] testCorrelationId = new byte[]{(byte) 0xc3, 0x28};
        final Binary correlationId = new Binary(testCorrelationId);
        Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertArrayEquals(testCorrelationId, convertedProperties.getCorrelationId().getBytes(), "Unexpected correlationId");

    }

    @Test
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

    @Test
    public void testReplyToConversionWhenQueueIsSpecified()
    {
        final String replyTo = "myTestQueue";
        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("direct:////" + replyTo, convertedProperties.getReplyToAsString(), "Unexpected reply-to");
    }

    @Test
    public void testReplyToConversionWhenExchangeIsSpecified()
    {
        final String replyTo = "myTestExchange";
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        when(exchange.getType()).thenReturn(ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("fanout://" + replyTo + "//", convertedProperties.getReplyToAsString(), "Unexpected reply-to");
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getType()).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("topic://" + exchangeName + "//?routingkey='" + routingKey + "'", convertedProperties.getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreSpecifiedAndGlobalPrefixIsUsed()
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String globalPrefix = "/testPrefix";
        final String replyTo = String.format("%s/%s/%s", globalPrefix, exchangeName, routingKey);
        when(_namedAddressSpace.getLocalAddress(replyTo)).thenReturn(exchangeName + "/" + routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getType()).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("topic://" + exchangeName + "//?routingkey='" + routingKey + "'", convertedProperties.getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    public void testReplyToConversionWhenReplyToCannotBeResolved()
    {
        final String replyTo = "direct://amq.direct//test?routingkey='test'";

        Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("direct:////?routingkey='" + replyTo + "'", convertedProperties.getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    public void testTTLConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expectedExpiration = arrivalTime + ttl;
        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        Message_1_0 message = createTestMessage(header, arrivalTime);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(expectedExpiration, convertedProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    public void testAbsoluteExpiryTimeConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(expiryTime));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(expiryTime, convertedProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    public void testConversionOfAbsoluteExpiryTimeTakesPrecedenceOverTTL()
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
                                                arrivalTime
                                               );

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(absoluteExpiryTime, convertedProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    public void testMessageIdStringConversion()
    {
        final String messageId = "testMessageId";
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(messageId, convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    public void testMessageIdLongStringConversion()
    {
        final String messageId = generateLongString();
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertNull(convertedProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    public void testMessageIdUUIDConversion()
    {
        final UUID messageId = UUID.randomUUID();
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(messageId.toString(), convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    public void testMessageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1);
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(messageId.toString(), convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    public void testMessageIdBinaryConversion()
    {
        final String messageId = "testMessageId";
        Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId.getBytes()));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(messageId, convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    public void testMessageIdByteArrayConversion()
    {
        final byte[] messageId = "testMessageId".getBytes();
        Properties properties = new Properties();
        properties.setMessageId(messageId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertArrayEquals(messageId, convertedProperties.getMessageId().getBytes(), "Unexpected messageId");
    }

    @Test
    public void testMessageIdBinaryConversionWhenNonUtf8()
    {
        final byte[] messageId = new byte[]{(byte) 0xc3, 0x28};
        Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertArrayEquals(messageId, convertedProperties.getMessageId().getBytes(), "Unexpected messageId");
    }

    @Test
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
                                                arrivalTime
                                               );

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(timestamp, convertedProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    public void testArrivalTimeConversion()
    {
        final long arrivalTime = System.currentTimeMillis() - 10000;
        Message_1_0 message = createTestMessage(new Header(), arrivalTime);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(arrivalTime, convertedProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    public void testUserIdConversion()
    {
        final String userId = "test-userId";
        Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertEquals(userId, convertedProperties.getUserIdAsString(), "Unexpected user-id");
    }

    @Test
    public void testUserIdConversionWhenLengthExceeds255()
    {
        final String userId = generateLongString();
        Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertNull(convertedProperties.getUserId(), "Unexpected user-id");
    }

    @Test
    public void testUserIdConversionWhenNonUtf8()
    {
        final byte[] userId = new byte[]{(byte) 0xc3, 0x28};
        Properties properties = new Properties();
        properties.setUserId(new Binary(userId));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        assertArrayEquals(userId, convertedProperties.getUserId().getBytes(), "Unexpected user-id");
    }

    @Test
    public void testGroupIdConversion()
    {
        String testGroupId = generateLongString();
        Properties properties = new Properties();
        properties.setGroupId(testGroupId);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        assertEquals(testGroupId, headers.get("JMSXGroupID"), "Unexpected group-id");
    }

    @Test
    public void testGroupSequenceConversion()
    {
        int testGroupSequence = 1;
        Properties properties = new Properties();
        properties.setGroupSequence(UnsignedInteger.valueOf(testGroupSequence));
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        assertEquals(testGroupSequence, headers.get("JMSXGroupSeq"), "Unexpected group-id");
    }


    @Test
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

    @Test
    public void testToConversionWhenExchangeAndRoutingKeyIsSpecified()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";

        String to = testExchange + "/" + testRoutingKey;
        Properties properties = new Properties();
        properties.setTo(to);
        Message_1_0 message = createTestMessage(properties);

        Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    public void testToConversionWhenExchangeIsSpecified()
    {
        final String testExchange = "testExchange";
        Properties properties = new Properties();
        properties.setTo(testExchange);
        Message_1_0 message = createTestMessage(properties);

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals("", messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    public void testToConversionWhenExchangeIsSpecifiedAndSubjectIsSet()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        Properties properties = new Properties();
        properties.setTo(testExchange);
        properties.setSubject(testRoutingKey);
        Message_1_0 message = createTestMessage(properties);

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange),anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    public void testToConversionWhenQueueIsSpecified()
    {
        final String testQueue = "testQueue";
        Properties properties = new Properties();
        properties.setTo(testQueue);
        Message_1_0 message = createTestMessage(properties);

        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(testQueue), anyBoolean());

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testQueue, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    public void testToConversionWhenGlobalAddressUnrecognized()
    {
        final String globalAddress = "/testQueue";
        Properties properties = new Properties();
        properties.setTo(globalAddress);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(globalAddress, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    public void testToConversionWhenGlobalAddressIsKnown()
    {
        final String globalPrefix = "/testPrefix";
        final String queueName = "testQueue";
        final String globalAddress = globalPrefix + "/" + queueName;
        Properties properties = new Properties();
        properties.setTo(globalAddress);
        Message_1_0 message = createTestMessage(properties);

        Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        when(_namedAddressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(queueName, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
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

    @Test
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

    @Test
    public void testToConversionWhenDestinationIsSpecifiedButDoesNotExists()
    {
        final String testDestination = "testDestination";
        Properties properties = new Properties();
        properties.setTo(testDestination);
        Message_1_0 message = createTestMessage(properties);

        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testDestination, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
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
                                 arrivalTime
                                );
    }

    private Message_1_0 createTestMessage(final Properties properties)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 new MessageAnnotations(Collections.emptyMap()),
                                 properties,
                                 new ApplicationProperties(Collections.emptyMap()),
                                 0
                                );
    }

    private Message_1_0 createTestMessage(final ApplicationProperties applicationProperties)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Collections.emptyMap()),
                                 new MessageAnnotations(Collections.emptyMap()),
                                 new Properties(),
                                 applicationProperties,
                                 0
                                );
    }

    private Message_1_0 createTestMessage(final Header header,
                                          final DeliveryAnnotations deliveryAnnotations,
                                          final MessageAnnotations messageAnnotations,
                                          final Properties properties,
                                          final ApplicationProperties applicationProperties,
                                          final long arrivalTime)
    {
        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getContentSize()).thenReturn(0);
        when(storedMessage.getContent(0, 0)).thenReturn(QpidByteBuffer.emptyQpidByteBuffer());
        MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                                                               deliveryAnnotations.createEncodingRetainingSection(),
                                                               messageAnnotations.createEncodingRetainingSection(),
                                                               properties.createEncodingRetainingSection(),
                                                               applicationProperties.createEncodingRetainingSection(),
                                                               new Footer(Collections.emptyMap()).createEncodingRetainingSection(),
                                                               arrivalTime,
                                                               0);
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return new Message_1_0(storedMessage);
    }

    private String generateLongString()
    {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < AMQShortString.MAX_LENGTH + 1; i++)
        {
            buffer.append('x');
        }

        return buffer.toString();
    }
}
