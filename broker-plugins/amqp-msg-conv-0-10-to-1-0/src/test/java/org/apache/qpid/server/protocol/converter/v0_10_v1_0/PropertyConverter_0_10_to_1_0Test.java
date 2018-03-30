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

package org.apache.qpid.server.protocol.converter.v0_10_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.ReplyTo;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyConverter_0_10_to_1_0Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_10_to_1_0 _messageConverter;

    @Before
    public void setUp() throws Exception
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_10_to_1_0();
    }

    @Test
    public void testContentTypeConversion()
    {
        String contentType = "test-content-type";

        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(contentType);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected content type", contentType, properties.getContentType().toString());
    }


    @Test
    public void testContentTypeJavaObjectStreamConversion()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType("application/java-object-stream");
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected content type",
                            "application/x-java-serialized-object",
                            properties.getContentType().toString());

    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentEncoding(contentEncoding);
        MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), messageProperties, new byte[]{(byte) 1}, 0);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected content encoding", contentEncoding, properties.getContentEncoding().toString());
    }

    @Test
    public void testHeaderConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("intProperty", 1);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertEquals("Unexpected applicationProperties", headers, new HashMap<>(applicationProperties));
    }

    @Test
    public void testHeaderConversionWhenQpidSubjectIsPresent()
    {
        String testSubject = "testSubject";
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("qpid.subject", testSubject);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected subject", testSubject, properties.getSubject());
        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected subject in application properties",
                           applicationProperties.containsKey("qpid.subject"));

    }


    @Test
    public void testPersistentDeliveryModeConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertTrue("Unexpected durable header", header.getDurable());
    }

    @Test
    public void testNonPersistentDeliveryModeConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertFalse("Unexpected durable header", header.getDurable());
    }

    @Test
    public void testPriorityConversion()
    {
        final byte priority = 5;
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(priority));
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertEquals("Unexpected priority", (long) priority, (long) header.getPriority().byteValue());
    }

    @Test
    public void testCorrelationIdConversion()
    {
        final String correlationId = "testCorrelationId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId.getBytes(UTF_8));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected correlationId", correlationId, properties.getCorrelationId());
    }

    @Test
    public void testBinaryCorrelationIdConversion()
    {
        final byte[] correlationId = new byte[]{0x00, (byte) 0xff, (byte) 0xc3};
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setCorrelationId(correlationId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        final boolean condition = properties.getCorrelationId() instanceof Binary;
        assertTrue(String.format("Unexpected correlationId type. expected 'Binary' actual '%s'",
                                        properties.getCorrelationId().getClass().getSimpleName()), condition);
        assertArrayEquals("Unexpected correlationId", correlationId, ((Binary) properties.getCorrelationId()).getArray());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeySpecified()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "amq.direct/test_routing_key", properties.getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenExchangeSpecified()
    {
        final String exchangeName = "amq.direct";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(exchangeName, null));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", exchangeName, properties.getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", routingKey, properties.getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenExchangeIsEmptyStringAndRoutingKeySpecified()
    {
        final String routingKey = "test_routing_key";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo("", routingKey));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "test_routing_key", properties.getReplyTo());
    }

    @Test
    public void testReplyToConversionWhenExchangeAndRoutingKeyAreNull()
    {
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setReplyTo(new ReplyTo(null, null));
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertNull("Unexpected reply-to", properties.getReplyTo());
    }

    @Test
    public void testExpirationConversion()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;
        final long expiration = timestamp + ttl;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExpiration(expiration);
        MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertNull("Unexpected expiration", properties.getAbsoluteExpiryTime());

        Header header = convertedMessage.getHeaderSection().getValue();
        assertEquals("Unexpected TTL", (long) ttl, (long) header.getTtl().intValue());
    }

    @Test
    public void testTTLConversion()
    {
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;

        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setTtl(ttl);
        MessageTransferMessage message =
                createTestMessage(deliveryProperties, new MessageProperties(), null, timestamp);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertEquals("Unexpected TTL", (long) ttl, header.getTtl().longValue());

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertNull("Unexpected expiration", properties.getAbsoluteExpiryTime());
    }

    @Test
    public void testMessageIdConversion()
    {
        UUID messageId = UUID.randomUUID();
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setMessageId(messageId);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected messageId", messageId, properties.getMessageId());
    }

    @Test
    public void testTimestampConversion()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        final long timestamp = System.currentTimeMillis() - 1000;
        deliveryProperties.setTimestamp(timestamp);
        MessageTransferMessage message = createTestMessage(deliveryProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected creation timestamp", timestamp, properties.getCreationTime().getTime());
    }

    @Test
    public void testArrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 1000;
        MessageTransferMessage message =
                createTestMessage(new DeliveryProperties(), new MessageProperties(), null, timestamp);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected creation timestamp", timestamp, properties.getCreationTime().getTime());
    }

    @Test
    public void testJmsTypeConversion()
    {
        final String type = "test-type";
        final Map<String, Object> headers = Collections.singletonMap("x-jms-type", type);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected subject", type, properties.getSubject());
        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected x-jms-type in application properties",
                           applicationProperties.containsKey("x-jms-type"));
    }

    @Test
    public void testQpidSubjectTakesPrecedenceOverJmsType()
    {
        final String jmsType = "test-jms-type";
        final String qpidSubjectType = "test-qpid-type";
        final Map<String, Object> headers = new HashMap<>();
        headers.put("x-jms-type", jmsType);
        headers.put("qpid.subject", qpidSubjectType);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected subject", qpidSubjectType, properties.getSubject());
        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertTrue("Unexpected entries in application properties", applicationProperties.isEmpty());
    }

    @Test
    public void testUserIdConversion()
    {
        final String userId = "test-userId";
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setUserId(userId.getBytes());
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected user-id", userId, new String(properties.getUserId().getArray(), UTF_8));
    }

    @Test
    public void testHeaderJMSXGroupIdConversion()
    {
        String testGroupId = "testGroupId";
        Map<String, Object> headers = Collections.singletonMap("JMSXGroupID", testGroupId);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected group-id", testGroupId, properties.getGroupId());

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected JMSXGroupID in application properties",
                           applicationProperties.containsKey("JMSXGroupID"));
    }

    @Test
    public void testHeaderJMSXGroupSeqConversion()
    {
        int testGroupSequenceNumber = 1;
        Map<String, Object> headers = Collections.singletonMap("JMSXGroupSeq", testGroupSequenceNumber);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected group-sequence",
                            (long) testGroupSequenceNumber,
                            (long) properties.getGroupSequence().intValue());


        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected JMSXGroupSeq in application properties",
                           applicationProperties.containsKey("JMSXGroupSeq"));
    }

    @Test
    public void testHeaderJMSXGroupSeqConversionWhenWrongType()
    {
        short testGroupSequenceNumber = (short) 1;
        Map<String, Object> headers = Collections.singletonMap("JMSXGroupSeq", testGroupSequenceNumber);
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected group-sequence", null, properties.getGroupSequence());

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertTrue("JMSXGroupSeq was removed from application properties",
                          applicationProperties.containsKey("JMSXGroupSeq"));
    }

    @Test
    public void testHeaderWithMapValueConversionFails()
    {
        Map<String, Object> headers = Collections.singletonMap("mapHeader", Collections.emptyMap());
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

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
    public void testHeaderWithListValueConversionFails()
    {
        Map<String, Object> headers = Collections.singletonMap("listHeader", Collections.emptyList());
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

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
    public void testHeaderWithArrayValueConversionFails()
    {
        Map<String, Object> headers = Collections.singletonMap("listHeader", new int[]{1});
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setApplicationHeaders(headers);
        MessageTransferMessage message = createTestMessage(messageProperties);

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
    public void testExchangeRoutingKeyConversion()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setExchange(testExchange);
        deliveryProperties.setRoutingKey(testRoutingKey);
        MessageTransferMessage message = createTestMessage(deliveryProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected to", testExchange + "/" + testRoutingKey, properties.getTo());
    }

    private MessageTransferMessage createTestMessage(final DeliveryProperties deliveryProperties)
    {
        return createTestMessage(deliveryProperties, new MessageProperties(), null, 0);
    }

    private MessageTransferMessage createTestMessage(final MessageProperties messageProperties)
    {
        return createTestMessage(new DeliveryProperties(), messageProperties, null, 0);
    }

    private MessageTransferMessage createTestMessage(final DeliveryProperties deliveryProperties,
                                                     final MessageProperties messageProperties,
                                                     final byte[] content,
                                                     final long arrivalTime)
    {
        int bodySize = content == null ? 0 : content.length;
        final org.apache.qpid.server.protocol.v0_10.transport.Header header =
                new org.apache.qpid.server.protocol.v0_10.transport.Header(deliveryProperties, messageProperties);
        final MessageMetaData_0_10 metaData = new MessageMetaData_0_10(header, bodySize, arrivalTime);

        final StoredMessage<MessageMetaData_0_10> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData()).thenReturn(metaData);

        if (content != null)
        {
            when(storedMessage.getContentSize()).thenReturn(content.length);
            when(storedMessage.getContent(0, content.length)).thenReturn(QpidByteBuffer.wrap(content));
        }
        else
        {
            when(storedMessage.getContentSize()).thenReturn(0);
            when(storedMessage.getContent(0, 0)).thenReturn(QpidByteBuffer.emptyQpidByteBuffer());
        }
        return new MessageTransferMessage(storedMessage, null);
    }
}
