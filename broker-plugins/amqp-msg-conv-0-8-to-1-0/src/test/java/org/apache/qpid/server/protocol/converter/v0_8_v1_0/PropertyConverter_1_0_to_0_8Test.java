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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

@SuppressWarnings({"unchecked"})
class PropertyConverter_1_0_to_0_8Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_1_0_to_v0_8 _messageConverter;

    @BeforeAll
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        when(_namedAddressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_1_0_to_v0_8();
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(contentEncoding, convertedProperties.getEncoding().toString(), "Unexpected content encoding");
    }

    @Test
    void contentEncodingConversionWhenLengthExceeds255()
    {
        final String contentEncoding = generateLongString();
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        final Message_1_0 message = createTestMessage(properties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Expected exception not thrown");
    }

    @Test
    void applicationPropertiesConversion()
    {
        final Map<String, Object> properties = Map.of("testProperty1", "testProperty1Value", "intProperty", 1);
        final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        final Message_1_0 message = createTestMessage(applicationProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();

        assertEquals(properties, new HashMap<>(headers), "Unexpected headers");
    }

    @Test
    void applicationPropertiesConversionWithUuid()
    {
        final String key = "uuidProperty";
        final Map<String, Object> properties = Map.of(key, UUID.randomUUID());
        final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        final Message_1_0 message = createTestMessage(applicationProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();

        assertEquals(properties.size(), (long) headers.size(), "Unexpected headers size");
        assertEquals(properties.get(key), UUID.fromString((String) headers.get(key)), "Unexpected headers");
    }

    @Test
    void applicationPropertiesConversionWithDate()
    {
        final String key = "dateProperty";
        final Map<String, Object> properties = Map.of(key, new Date());
        final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        final Message_1_0 message = createTestMessage(applicationProperties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();

        assertEquals(properties.size(), (long) headers.size(), "Unexpected headers size");
        assertEquals(properties.get(key), new Date((Long) headers.get(key)), "Unexpected headers");
    }

    @Test
    void subjectConversion()
    {
        final String subject = "testSubject";
        final Properties properties = new Properties();
        properties.setSubject(subject);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(subject, headers.get("qpid.subject"), "Unexpected qpid.subject is missing from headers");
        assertEquals(subject, convertedProperties.getType().toString(), "Unexpected type");
        assertEquals(subject, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing-key");
    }

    @Test
    void subjectConversionWhenSubjectExceeds255()
    {
        final String subject = generateLongString();
        final Properties properties = new Properties();
        properties.setSubject(subject);
        final Message_1_0 message = createTestMessage(properties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Expected conversion exception");
    }

    @Test
    void durableConversion()
    {
        final Header header = new Header();
        header.setDurable(true);
        final Message_1_0 message = createTestMessage(header);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(BasicContentHeaderProperties.PERSISTENT, (long) convertedProperties.getDeliveryMode(),
                "Unexpected deliveryMode");
    }

    @Test
    void nonDurableConversion()
    {
        final Header header = new Header();
        header.setDurable(false);
        final Message_1_0 message = createTestMessage(header);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(BasicContentHeaderProperties.NON_PERSISTENT, (long) convertedProperties.getDeliveryMode(),
                "Unexpected deliveryMode");
    }

    @Test
    void priorityConversion()
    {
        final Header header = new Header();
        final byte priority = (byte) 7;
        header.setPriority(UnsignedByte.valueOf(priority));
        final Message_1_0 message = createTestMessage(header);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(priority, (long) convertedProperties.getPriority(), "Unexpected priority");
    }

    @Test
    void correlationIdStringConversion()
    {
        final String correlationId = "testCorrelationId";
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(correlationId, convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    void correlationIdLongStringConversion()
    {
        final String correlationId = generateLongString();
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Expected exception not thrown");
    }

    @Test
    void correlationIdULongConversion()
    {
        final UnsignedLong correlationId = UnsignedLong.valueOf(-1);
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(correlationId.toString(), convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    void correlationIdUUIDConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(correlationId.toString(), convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    void correlationIdBinaryConversion()
    {
        final String testCorrelationId = "testCorrelationId";
        final Binary correlationId = new Binary(testCorrelationId.getBytes(UTF_8));
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(testCorrelationId, convertedProperties.getCorrelationId().toString(), "Unexpected correlationId");
    }

    @Test
    void correlationIdBinaryConversionWhenNotUtf8()
    {
        final byte[] testCorrelationId = new byte[]{(byte) 0xc3, 0x28};
        final Binary correlationId = new Binary(testCorrelationId);
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertArrayEquals(testCorrelationId, convertedProperties.getCorrelationId().getBytes(), "Unexpected correlationId");
    }

    @Test
    void replyToConversionWhenResultExceeds255()
    {
        final String replyTo = generateLongString() + "/" + generateLongString();
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Expected exception not thrown");
    }

    @Test
    void replyToConversionWhenQueueIsSpecified()
    {
        final String replyTo = "myTestQueue";
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("direct:////" + replyTo, convertedProperties.getReplyToAsString(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeIsSpecified()
    {
        final String replyTo = "myTestExchange";
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        when(exchange.getType()).thenReturn(ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("fanout://" + replyTo + "//", convertedProperties.getReplyToAsString(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getType()).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("topic://" + exchangeName + "//?routingkey='" + routingKey + "'", convertedProperties.getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreSpecifiedAndGlobalPrefixIsUsed()
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String globalPrefix = "/testPrefix";
        final String replyTo = String.format("%s/%s/%s", globalPrefix, exchangeName, routingKey);
        when(_namedAddressSpace.getLocalAddress(replyTo)).thenReturn(exchangeName + "/" + routingKey);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getType()).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("topic://" + exchangeName + "//?routingkey='" + routingKey + "'", convertedProperties.getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenReplyToCannotBeResolved()
    {
        final String replyTo = "direct://amq.direct//test?routingkey='test'";
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals("direct:////?routingkey='" + replyTo + "'", convertedProperties.getReplyToAsString(),
                "Unexpected reply-to");
    }

    @Test
    void TTLConversion()
    {
        final long ttl = 10000;
        final long arrivalTime = System.currentTimeMillis();
        final long expectedExpiration = arrivalTime + ttl;
        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        final Message_1_0 message = createTestMessage(header, arrivalTime);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(expectedExpiration, convertedProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void absoluteExpiryTimeConversion()
    {
        final long ttl = 10000;
        final long arrivalTime = System.currentTimeMillis();
        final long expiryTime = arrivalTime + ttl;
        final Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(expiryTime));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(expiryTime, convertedProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void conversionOfAbsoluteExpiryTimeTakesPrecedenceOverTTL()
    {
        final long ttl = 10000;
        final long time = System.currentTimeMillis();
        final long absoluteExpiryTime = time + ttl;
        final long arrivalTime = time + 1;
        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        final Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(absoluteExpiryTime));
        final Message_1_0 message = createTestMessage(header,
                new DeliveryAnnotations(Map.of()),
                new MessageAnnotations(Map.of()),
                properties,
                new ApplicationProperties(Map.of()),
                arrivalTime);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(absoluteExpiryTime, convertedProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void messageIdStringConversion()
    {
        final String messageId = "testMessageId";
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(messageId, convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    void messageIdLongStringConversion()
    {
        final String messageId = generateLongString();
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertNull(convertedProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdUUIDConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(messageId.toString(), convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    void messageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1);
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(messageId.toString(), convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    void messageIdBinaryConversion()
    {
        final String messageId = "testMessageId";
        final Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId.getBytes()));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(messageId, convertedProperties.getMessageId().toString(), "Unexpected messageId");
    }

    @Test
    void messageIdByteArrayConversion()
    {
        final byte[] messageId = "testMessageId".getBytes();
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertArrayEquals(messageId, convertedProperties.getMessageId().getBytes(), "Unexpected messageId");
    }

    @Test
    void messageIdBinaryConversionWhenNonUtf8()
    {
        final byte[] messageId = new byte[]{(byte) 0xc3, 0x28};
        final Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertArrayEquals(messageId, convertedProperties.getMessageId().getBytes(), "Unexpected messageId");
    }

    @Test
    void creationTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 10000;
        final long arrivalTime = timestamp + 1;
        final Properties properties = new Properties();
        properties.setCreationTime(new Date(timestamp));
        final Message_1_0 message = createTestMessage(new Header(),
                new DeliveryAnnotations(Map.of()),
                new MessageAnnotations(Map.of()),
                properties,
                new ApplicationProperties(Map.of()),
                arrivalTime);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(timestamp, convertedProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void arrivalTimeConversion()
    {
        final long arrivalTime = System.currentTimeMillis() - 10000;
        final Message_1_0 message = createTestMessage(new Header(), arrivalTime);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(arrivalTime, convertedProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "test-userId";
        final Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertEquals(userId, convertedProperties.getUserIdAsString(), "Unexpected user-id");
    }

    @Test
    void userIdConversionWhenLengthExceeds255()
    {
        final String userId = generateLongString();
        final Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertNull(convertedProperties.getUserId(), "Unexpected user-id");
    }

    @Test
    void userIdConversionWhenNonUtf8()
    {
        final byte[] userId = new byte[]{(byte) 0xc3, 0x28};
        final Properties properties = new Properties();
        properties.setUserId(new Binary(userId));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();

        assertArrayEquals(userId, convertedProperties.getUserId().getBytes(), "Unexpected user-id");
    }

    @Test
    void groupIdConversion()
    {
        final String testGroupId = generateLongString();
        final Properties properties = new Properties();
        properties.setGroupId(testGroupId);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();

        assertEquals(testGroupId, headers.get("JMSXGroupID"), "Unexpected group-id");
    }

    @Test
    void groupSequenceConversion()
    {
        final int testGroupSequence = 1;
        final Properties properties = new Properties();
        properties.setGroupSequence(UnsignedInteger.valueOf(testGroupSequence));
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final BasicContentHeaderProperties convertedProperties = convertedMessage.getContentHeaderBody().getProperties();
        final Map<String, Object> headers = convertedProperties.getHeadersAsMap();

        assertEquals(testGroupSequence, headers.get("JMSXGroupSeq"), "Unexpected group-id");
    }

    @Test
    void applicationPropertiesConversionWhenKeyLengthExceeds255()
    {
        final Map<String, Object> properties = Map.of("testProperty-" + generateLongString(), "testValue");
        final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        final Message_1_0 message = createTestMessage(applicationProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is expected");
    }

    @Test
    void toConversionWhenExchangeAndRoutingKeyIsSpecified()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        final String to = testExchange + "/" + testRoutingKey;
        final Properties properties = new Properties();
        properties.setTo(to);
        final Message_1_0 message = createTestMessage(properties);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeIsSpecified()
    {
        final String testExchange = "testExchange";
        final Properties properties = new Properties();
        properties.setTo(testExchange);
        final Message_1_0 message = createTestMessage(properties);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals("", messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeIsSpecifiedAndSubjectIsSet()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        final Properties properties = new Properties();
        properties.setTo(testExchange);
        properties.setSubject(testRoutingKey);
        final Message_1_0 message = createTestMessage(properties);
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(testExchange);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange),anyBoolean());
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals(testExchange, messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testRoutingKey, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenQueueIsSpecified()
    {
        final String testQueue = "testQueue";
        final Properties properties = new Properties();
        properties.setTo(testQueue);
        final Message_1_0 message = createTestMessage(properties);
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(testQueue);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(testQueue), anyBoolean());
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testQueue, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressUnrecognized()
    {
        final String globalAddress = "/testQueue";
        final Properties properties = new Properties();
        properties.setTo(globalAddress);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(globalAddress, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsKnown()
    {
        final String globalPrefix = "/testPrefix";
        final String queueName = "testQueue";
        final String globalAddress = globalPrefix + "/" + queueName;
        final Properties properties = new Properties();
        properties.setTo(globalAddress);
        final Message_1_0 message = createTestMessage(properties);
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        when(_namedAddressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(queueName, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenExchangeLengthExceeds255()
    {
        final String testExchange = generateLongString();
        final String testRoutingKey = "testRoutingKey";
        final String to = testExchange + "/" + testRoutingKey;
        final Properties properties = new Properties();
        properties.setTo(to);
        final Message_1_0 message = createTestMessage(properties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is not thrown");
    }

    @Test
    void toConversionWhenRoutingKeyLengthExceeds255()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = generateLongString();
        final String to = testExchange + "/" + testRoutingKey;
        final Properties properties = new Properties();
        properties.setTo(to);
        final Message_1_0 message = createTestMessage(properties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is not thrown");
    }

    @Test
    void toConversionWhenDestinationIsSpecifiedButDoesNotExists()
    {
        final String testDestination = "testDestination";
        final Properties properties = new Properties();
        properties.setTo(testDestination);
        final Message_1_0 message = createTestMessage(properties);
        final AMQMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessagePublishInfo messagePublishInfo = convertedMessage.getMessagePublishInfo();

        assertEquals("", messagePublishInfo.getExchange().toString(), "Unexpected exchange");
        assertEquals(testDestination, messagePublishInfo.getRoutingKey().toString(), "Unexpected routing key");
    }

    private Message_1_0 createTestMessage(final Header header)
    {
        return createTestMessage(header, 0);
    }

    private Message_1_0 createTestMessage(final Header header, final long arrivalTime)
    {
        return createTestMessage(header,
                                 new DeliveryAnnotations(Map.of()),
                                 new MessageAnnotations(Map.of()),
                                 new Properties(),
                                 new ApplicationProperties(Map.of()),
                                 arrivalTime);
    }

    private Message_1_0 createTestMessage(final Properties properties)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Map.of()),
                                 new MessageAnnotations(Map.of()),
                                 properties,
                                 new ApplicationProperties(Map.of()),
                                 0);
    }

    private Message_1_0 createTestMessage(final ApplicationProperties applicationProperties)
    {
        return createTestMessage(new Header(),
                                 new DeliveryAnnotations(Map.of()),
                                 new MessageAnnotations(Map.of()),
                                 new Properties(),
                                 applicationProperties,
                                 0);
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
        final MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                deliveryAnnotations.createEncodingRetainingSection(),
                messageAnnotations.createEncodingRetainingSection(),
                properties.createEncodingRetainingSection(),
                applicationProperties.createEncodingRetainingSection(),
                new Footer(Map.of()).createEncodingRetainingSection(),
                arrivalTime,
                0);
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return new Message_1_0(storedMessage);
    }

    private String generateLongString()
    {
        return "x".repeat(AMQShortString.MAX_LENGTH + 1);
    }
}
