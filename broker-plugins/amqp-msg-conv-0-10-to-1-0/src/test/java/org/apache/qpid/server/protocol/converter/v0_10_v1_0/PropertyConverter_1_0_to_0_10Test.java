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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
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
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class PropertyConverter_1_0_to_0_10Test extends UnitTestBase
{
    private static final DeliveryAnnotations DELIVERY_ANNOTATIONS = new DeliveryAnnotations(Map.of());
    private static final MessageAnnotations MSG_ANNOTATIONS = new MessageAnnotations(Map.of());

    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_1_0_to_v0_10 _messageConverter;

    @BeforeAll
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        when(_namedAddressSpace.getLocalAddress(anyString())).then(returnsFirstArg());
        _messageConverter = new MessageConverter_1_0_to_v0_10();
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final Properties properties = new Properties();
        properties.setContentEncoding(Symbol.valueOf(contentEncoding));
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(contentEncoding, messageProperties.getContentEncoding(), "Unexpected content encoding");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();

        assertEquals(properties, new HashMap<>(headers), "Unexpected headers");
    }

    @Test
    void subjectConversion()
    {
        final String subject = "testSubject";
        final Properties properties = new Properties();
        properties.setSubject(subject);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(subject, headers.get("qpid.subject"), "Unexpected qpid.subject is missing from headers");
        assertEquals(subject, headers.get("x-jms-type"), "Unexpected type");
        assertEquals(subject, deliveryProperties.getRoutingKey(), "Unexpected routing-key");
    }

    @Test
    void subjectDoesNoReplaceApplicationPropertyQpidSubject()
    {
        final String subject = "testSubject";
        final Properties properties = new Properties();
        properties.setSubject(subject);
        final String qpidSubject = "testSubject2";
        final Map<String, Object> applicationPropertiesMap = Map.of("qpid.subject", qpidSubject);
        final ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);
        final Message_1_0 message = createTestMessage(new Header(), properties, applicationProperties, 0, null);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(qpidSubject, headers.get("qpid.subject"), "Unexpected qpid.subject is missing from headers");
        assertEquals(subject, headers.get("x-jms-type"), "Unexpected type");
        assertEquals(subject, deliveryProperties.getRoutingKey(), "Unexpected routing-key");
    }

    @Test
    void subjectDoesNoReplaceApplicationPropertyXJMSType()
    {
        final String subject = "testSubject";
        final Properties properties = new Properties();
        properties.setSubject(subject);
        final String jmsType = "testJmsType";
        final Map<String, Object> applicationPropertiesMap = Map.of("x-jms-type", jmsType);
        final ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);
        final Message_1_0 message = createTestMessage(new Header(), properties, applicationProperties, 0, null);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> headers = messageProperties.getApplicationHeaders();
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(subject, headers.get("qpid.subject"), "Unexpected qpid.subject is missing from headers");
        assertEquals(jmsType, headers.get("x-jms-type"), "Unexpected type");
        assertEquals(subject, deliveryProperties.getRoutingKey(), "Unexpected routing-key");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(MessageDeliveryMode.PERSISTENT, deliveryProperties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    void nonDurableConversion()
    {
        final Header header = new Header();
        header.setDurable(false);
        final Message_1_0 message = createTestMessage(header);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(MessageDeliveryMode.NON_PERSISTENT, deliveryProperties.getDeliveryMode(), "Unexpected deliveryMode");
    }

    @Test
    void priorityConversion()
    {
        final Header header = new Header();
        final byte priority = (byte) 7;
        header.setPriority(UnsignedByte.valueOf(priority));
        final Message_1_0 message = createTestMessage(header);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(priority, (long) deliveryProperties.getPriority().getValue(), "Unexpected priority");
    }

    @Test
    void correlationIdStringConversion()
    {
        final String correlationId = "testCorrelationId";
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertArrayEquals(correlationId.getBytes(), messageProperties.getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdLongStringConversion()
    {
        final String correlationId = generateLongLongString();
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertArrayEquals(longToBytes(correlationId.longValue()), messageProperties.getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdUUIDConversion()
    {
        final UUID correlationId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final byte[] expectedBytes =
                longToBytes(correlationId.getMostSignificantBits(), correlationId.getLeastSignificantBits());

        assertArrayEquals(expectedBytes, messageProperties.getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationIdBinaryConversion()
    {
        final String testCorrelationId = "testCorrelationId";
        final Binary correlationId = new Binary(testCorrelationId.getBytes(UTF_8));
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertArrayEquals(testCorrelationId.getBytes(), messageProperties.getCorrelationId(), "Unexpected correlationId");
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
        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(replyTo);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final ReplyTo convertedReplyTo = convertedMessage.getHeader().getMessageProperties().getReplyTo();

        assertEquals("", convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(replyTo, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenExchangeIsSpecified()
    {
        final String replyTo = "myTestExchange";
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(replyTo);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(replyTo), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final ReplyTo convertedReplyTo = convertedMessage.getHeader().getMessageProperties().getReplyTo();

        assertEquals(replyTo, convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals("", convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreSpecified()
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s/%s", exchangeName, routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final ReplyTo convertedReplyTo = convertedMessage.getHeader().getMessageProperties().getReplyTo();

        assertEquals(exchangeName, convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(routingKey, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenExchangeAndRoutingKeyAreSpecifiedAndGlobalPrefixIsUsed()
    {
        final String exchangeName = "testExchnageName";
        final String routingKey = "testRoutingKey";
        final String globalPrefix = "/testPrefix";
        final String replyTo = String.format("%s/%s/%s", globalPrefix, exchangeName, routingKey);
        when(_namedAddressSpace.getLocalAddress(replyTo)).thenReturn(exchangeName + "/" + routingKey);
        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(exchangeName);
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(exchangeName), anyBoolean());
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final ReplyTo convertedReplyTo = convertedMessage.getHeader().getMessageProperties().getReplyTo();

        assertEquals(exchangeName, convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(routingKey, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void replyToConversionWhenReplyToCannotBeResolved()
    {
        final String replyTo = "direct://amq.direct//test?routingkey='test'";
        final Properties properties = new Properties();
        properties.setReplyTo(replyTo);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final ReplyTo convertedReplyTo = convertedMessage.getHeader().getMessageProperties().getReplyTo();

        assertEquals("", convertedReplyTo.getExchange(), "Unexpected exchange");
        assertEquals(replyTo, convertedReplyTo.getRoutingKey(), "Unexpected routing key");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(ttl, deliveryProperties.getTtl(), "Unexpected ttl");
        assertEquals(expectedExpiration, deliveryProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void absoluteExpiryTimeConversion()
    {
        final long ttl = 10000;
        final long arrivalTime = System.currentTimeMillis();
        final long expiryTime = arrivalTime + ttl;
        final Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(expiryTime));
        final Message_1_0 message = createTestMessage(properties, arrivalTime);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(ttl, deliveryProperties.getTtl(), "Unexpected ttl");
        assertEquals(expiryTime, deliveryProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void conversionOfTtlTakesPrecedenceOverAbsoluteExpiryTime()
    {
        final long ttl = 10000;
        final long time = System.currentTimeMillis();
        final long absoluteExpiryTime = time + ttl;
        final long arrivalTime = time + 1;
        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        final Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(new Date(absoluteExpiryTime));
        final Message_1_0 message =
                createTestMessage(header, properties, new ApplicationProperties(Map.of()), arrivalTime, null);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(ttl, deliveryProperties.getTtl(), "Unexpected ttl");
        assertEquals(arrivalTime + ttl, deliveryProperties.getExpiration(), "Unexpected expiration");
    }

    @Test
    void messageIdStringConversion()
    {
        final String messageId = "testMessageId";
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(UUID.nameUUIDFromBytes(messageId.getBytes(UTF_8)), messageProperties.getMessageId(),
                "Unexpected messageId");
    }

    @Test
    void messageIdUuidAsStringConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId(messageId.toString());
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdUUIDConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdStringifiedUUIDConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId(messageId.toString());
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdPrefixedStringifiedUUIDConversion()
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId("ID:" + messageId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(messageId, messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdUnsignedLongConversion()
    {
        final UnsignedLong messageId = UnsignedLong.valueOf(-1);
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(UUID.nameUUIDFromBytes(longToBytes(messageId.longValue())), messageProperties.getMessageId(),
                "Unexpected messageId");
    }

    @Test
    void messageIdBinaryConversion()
    {
        final String messageId = "testMessageId";
        final Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId.getBytes(UTF_8)));
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(UUID.nameUUIDFromBytes(messageId.getBytes(UTF_8)), messageProperties.getMessageId(),
                "Unexpected messageId");
    }

    @Test
    void messageIdByteArrayConversion()
    {
        final byte[] messageId = "testMessageId".getBytes(UTF_8);
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(UUID.nameUUIDFromBytes(messageId), messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageIdBinaryConversionWhenNonUtf8()
    {
        final byte[] messageId = new byte[]{(byte) 0xc3, 0x28};
        final Properties properties = new Properties();
        properties.setMessageId(new Binary(messageId));
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(UUID.nameUUIDFromBytes(messageId), messageProperties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void creationTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 10000;
        final long arrivalTime = timestamp + 1;
        final Properties properties = new Properties();
        properties.setCreationTime(new Date(timestamp));
        final Message_1_0 message =
                createTestMessage(new Header(), properties, new ApplicationProperties(Map.of()), arrivalTime, null);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(timestamp, deliveryProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void arrivalTimeConversion()
    {
        final long arrivalTime = System.currentTimeMillis() - 10000;
        final Message_1_0 message = createTestMessage(new Header(), arrivalTime);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(arrivalTime, deliveryProperties.getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "test-userId";
        final Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertArrayEquals(userId.getBytes(UTF_8), messageProperties.getUserId(), "Unexpected user-id");
    }

    @Test
    void userIdConversionWhenLengthExceeds16Bit()
    {
        final String userId = generateLongLongString();
        final Properties properties = new Properties();
        properties.setUserId(new Binary(userId.getBytes()));
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertNull(messageProperties.getUserId(), "Unexpected user-id");
    }

    @Test
    void groupIdConversion()
    {
        final String testGroupId = generateLongString();
        final Properties properties = new Properties();
        properties.setGroupId(testGroupId);
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();

        assertEquals(testGroupId, applicationHeaders.get("JMSXGroupID"), "Unexpected group-id");
    }

    @Test
    void groupIdDoesNotReplaceApplicationPropertiesJMSXGroupID()
    {
        final String testGroupId = "group1";
        final Properties properties = new Properties();
        properties.setGroupId(testGroupId);
        final String JMSXGroupID = "group2";
        final Map<String, Object> applicationPropertiesMap = Map.of("JMSXGroupID", JMSXGroupID);
        final ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);
        final Message_1_0 message =
                createTestMessage(new Header(), properties, applicationProperties, 0, null);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();

        assertEquals(JMSXGroupID, applicationHeaders.get("JMSXGroupID"), "Unexpected group-id");
    }

    @Test
    void groupSequenceConversion()
    {
        final int testGroupSequence = 1;
        final Properties properties = new Properties();
        properties.setGroupSequence(UnsignedInteger.valueOf(testGroupSequence));
        final Message_1_0 message = createTestMessage(properties);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();

        assertEquals(testGroupSequence, applicationHeaders.get("JMSXGroupSeq"), "Unexpected group-id");
    }

    @Test
    void groupSequenceDoesNotReplaceApplicationPropertiesJMSXGroupSeq()
    {
        final int testGroupSequence = 1;
        final Properties properties = new Properties();
        properties.setGroupSequence(UnsignedInteger.valueOf(testGroupSequence));
        final int JMSXGroupSeq = 2;
        final Map<String, Object> applicationPropertiesMap = Map.of("JMSXGroupSeq", JMSXGroupSeq);
        final ApplicationProperties applicationProperties = new ApplicationProperties(applicationPropertiesMap);
        final Message_1_0 message =
                createTestMessage(new Header(), properties, applicationProperties, 0, null);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();
        final Map<String, Object> applicationHeaders = messageProperties.getApplicationHeaders();

        assertEquals(JMSXGroupSeq, applicationHeaders.get("JMSXGroupSeq"), "Unexpected JMSXGroupSeq");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testRoutingKey, deliveryProperties.getRoutingKey(), "Unexpected routing key");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals("", deliveryProperties.getRoutingKey(), "Unexpected routing key");
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
        doReturn(exchange).when(_namedAddressSpace).getAttainedMessageDestination(eq(testExchange), anyBoolean());
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals(testExchange, deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testRoutingKey, deliveryProperties.getRoutingKey(), "Unexpected routing key");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testQueue, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsUnknown()
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;
        final Properties properties = new Properties();
        properties.setTo(globalAddress);
        final Message_1_0 message = createTestMessage(properties);
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(globalAddress, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void toConversionWhenGlobalAddressIsKnown()
    {
        final String queueName = "testQueue";
        final String prefix = "/testPrefix";
        final String globalAddress = prefix + "/" + queueName;
        final Properties properties = new Properties();
        properties.setTo(globalAddress);
        final Message_1_0 message = createTestMessage(properties);
        final Queue<?> queue = mock(Queue.class);
        when(queue.getName()).thenReturn(queueName);
        when(_namedAddressSpace.getLocalAddress(globalAddress)).thenReturn(queueName);
        doReturn(queue).when(_namedAddressSpace).getAttainedMessageDestination(eq(queueName), anyBoolean());
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(queueName, deliveryProperties.getRoutingKey(), "Unexpected routing key");
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
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final DeliveryProperties deliveryProperties =
                convertedMessage.getStoredMessage().getMetaData().getDeliveryProperties();

        assertEquals("", deliveryProperties.getExchange(), "Unexpected exchange");
        assertEquals(testDestination, deliveryProperties.getRoutingKey(), "Unexpected routing key");
    }

    @Test
    void contentToContentLengthConversion()
    {
        final byte[] content = new byte[]{0x31, 0x00, 0x10};
        final Message_1_0 message =
                createTestMessage(new Header(), new Properties(), new ApplicationProperties(Map.of()), 0, content);
        final MessageTransferMessage convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final MessageProperties messageProperties =
                convertedMessage.getStoredMessage().getMetaData().getMessageProperties();

        assertEquals(content.length, messageProperties.getContentLength(), "Unexpected content length");
    }

    private Message_1_0 createTestMessage(final Header header)
    {
        return createTestMessage(header, 0);
    }

    private Message_1_0 createTestMessage(final Header header, final long arrivalTime)
    {
        return createTestMessage(header, new Properties(), new ApplicationProperties(Map.of()), arrivalTime, null);
    }

    private Message_1_0 createTestMessage(final Properties properties)
    {
        return createTestMessage(properties, 0L);
    }

    private Message_1_0 createTestMessage(final Properties properties, final long arrivalTime)
    {
        return createTestMessage(new Header(), properties, new ApplicationProperties(Map.of()), arrivalTime, null);
    }

    private Message_1_0 createTestMessage(final ApplicationProperties applicationProperties)
    {
        return createTestMessage(new Header(), new Properties(), applicationProperties, 0, null);
    }

    private Message_1_0 createTestMessage(final Header header,
                                          final Properties properties,
                                          final ApplicationProperties applicationProperties,
                                          final long arrivalTime,
                                          final byte[] content)
    {
        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        final MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                DELIVERY_ANNOTATIONS.createEncodingRetainingSection(),
                MSG_ANNOTATIONS.createEncodingRetainingSection(),
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

    private String generateLongString()
    {
        return "x".repeat(256);
    }

    private String generateLongLongString()
    {
        return "x".repeat(1 << 16);
    }

    private byte[] longToBytes(final long... x)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * x.length);
        for (final long l : x)
        {
            buffer.putLong(l);
        }
        return buffer.array();
    }
}
