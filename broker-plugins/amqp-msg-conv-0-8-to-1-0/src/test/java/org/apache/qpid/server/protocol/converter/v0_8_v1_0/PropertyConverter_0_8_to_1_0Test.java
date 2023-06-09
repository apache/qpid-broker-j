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
package org.apache.qpid.server.protocol.converter.v0_8_v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

class PropertyConverter_0_8_to_1_0Test extends UnitTestBase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_8_to_1_0 _messageConverter;

    @BeforeAll
    void setUp()
    {
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_8_to_1_0();
    }

    @ParameterizedTest
    @CsvSource(
    {
            "test-content-type,test-content-type", "application/java-object-stream,application/x-java-serialized-object"
    })
    void contentTypesConversion(final String contentType, final String expectedContentType)
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setContentType(contentType);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(expectedContentType, properties.getContentType().toString(), "Unexpected content type");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setEncoding(contentEncoding);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties, new byte[]{(byte) 1}, 0);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(contentEncoding, properties.getContentEncoding().toString(), "Unexpected content encoding");
    }

    @Test
    void headerConversion()
    {
        final Map<String, Object> headers = Map.of("testProperty1", "testProperty1Value", "intProperty", 1);
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(headers, new HashMap<>(applicationProperties), "Unexpected applicationProperties");
    }

    @Test
    void headerConversionWhenQpidSubjectIsPresent()
    {
        final String testSubject = "testSubject";
        final Map<String, Object> headers = Map.of("testProperty1", "testProperty1Value", "qpid.subject", testSubject);
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(testSubject, properties.getSubject(), "Unexpected subject");
        assertFalse(applicationProperties.containsKey("qpid.subject"), "Unexpected subject in application properties");
    }

    @Test
    void persistentDeliveryModeConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertTrue(header.getDurable(), "Unexpected durable header");
    }

    @Test
    void nonPersistentDeliveryModeConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertFalse(header.getDurable(), "Unexpected durable header");
    }

    @Test
    void priorityConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte priority = 5;
        basicContentHeaderProperties.setPriority(priority);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();

        assertEquals(priority, (long) header.getPriority().byteValue(), "Unexpected priority");
    }

    @Test
    void correlationIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String correlationId = "testCorrelationId";
        basicContentHeaderProperties.setCorrelationId(AMQShortString.valueOf(correlationId));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(correlationId, properties.getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void correlationUuidIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID correlationId = UUID.randomUUID();
        basicContentHeaderProperties.setCorrelationId(AMQShortString.valueOf("ID:" + correlationId));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(correlationId, properties.getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct://amq.direct/destination_name/queue_name?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("amq.direct/test_routing_key", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed2()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct://amq.direct//queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("amq.direct/queue_name", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed3()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct:////queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("queue_name", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenBindingURLFormatIsUsed4()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "direct:////?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("test_routing_key", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void replyToConversionWhenNonBindingURLFormatIsUsed()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String replyTo = "test";
        basicContentHeaderProperties.setReplyTo(replyTo);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("test", properties.getReplyTo(), "Unexpected reply-to");
    }

    @Test
    void expirationConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis();
        final int ttl = 100000;
        final long expiration = timestamp + ttl;
        basicContentHeaderProperties.setExpiration(expiration);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties, timestamp);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Header header = convertedMessage.getHeaderSection().getValue();
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(ttl, header.getTtl().longValue(), "Unexpected TTL");
        assertNull(properties.getAbsoluteExpiryTime(), "Unexpected expiration");
    }

    @Test
    void messageIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String messageId = "testMessageId";
        basicContentHeaderProperties.setMessageId(messageId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(messageId, properties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void messageUuidConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final UUID messageId = UUID.randomUUID();
        basicContentHeaderProperties.setMessageId("ID:" + messageId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(messageId, properties.getMessageId(), "Unexpected messageId");
    }

    @Test
    void timestampConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis() - 10000;
        basicContentHeaderProperties.setTimestamp(timestamp);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(timestamp, properties.getCreationTime().getTime(), "Unexpected creation timestamp");
    }

    @Test
    void arrivalTimeConversion()
    {
        final long timestamp = System.currentTimeMillis() - 10000;
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties(), null, timestamp);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(timestamp, properties.getCreationTime().getTime(), "Unexpected creation timestamp");
    }

    @Test
    void typeConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String type = "test-type";
        basicContentHeaderProperties.setType(type);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(type, properties.getSubject(), "Unexpected subject");
    }

    @Test
    void userIdConversion()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String userId = "test-userId";
        basicContentHeaderProperties.setUserId(userId);
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(userId, new String(properties.getUserId().getArray(), UTF_8), "Unexpected user-id");
    }

    @Test
    void headerJMSXGroupIdConversion()
    {
        final String testGroupId = "testGroupId";
        final Map<String, Object> headers = Map.of("JMSXGroupID", testGroupId, "intProperty", 1);
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(testGroupId, properties.getGroupId(), "Unexpected group-id");
        assertFalse(applicationProperties.containsKey("JMSXGroupID"), "Unexpected JMSXGroupID in application properties");
    }

    @Test
    void headerJMSXGroupSeqConversion()
    {
        final int testGroupSequenceNumber = 1;
        final Map<String, Object> headers = Map.of("JMSXGroupSeq", testGroupSequenceNumber, "intProperty", 1);
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertEquals(testGroupSequenceNumber, (long) properties.getGroupSequence().intValue(), "Unexpected group-sequence");
        assertFalse(applicationProperties.containsKey("JMSXGroupSeq"), "Unexpected JMSXGroupSeq in application properties");
    }

    @Test
    void headerJMSXGroupSeqConversionWhenWrongType()
    {
        final short testGroupSequenceNumber = (short) 1;
        final Map<String, Object> headers = Map.of("JMSXGroupSeq", testGroupSequenceNumber, "intProperty", 1);
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();
        final Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertNull(properties.getGroupSequence(), "Unexpected group-sequence");
        assertTrue(applicationProperties.containsKey("JMSXGroupSeq"), "JMSXGroupSeq was removed from application properties");

    }

    @Test
    void headerWithFiledTableValueConversionFails()
    {
        final Map<String, Object> headers = Map.of("mapHeader", Map.of());
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is expected");
    }

    @Test
    void headerWithFieldArrayValueConversionFails()
    {
        final Map<String, Object> headers = Map.of("listHeader", List.of());
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        final AMQMessage message = createTestMessage(basicContentHeaderProperties);

        assertThrows(MessageConversionException.class,
                () -> _messageConverter.convert(message, _namedAddressSpace),
                "Exception is expected");
    }

    @Test
    void exchangeRoutingKeyConversion()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        final AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setExchange(AMQShortString.valueOf(testExchange));
        message.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));
        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);
        final Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals(testExchange + "/" + testRoutingKey, properties.getTo(), "Unexpected to");
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties)
    {
        return createTestMessage(basicContentHeaderProperties, null, 0);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         final long arrivalTime)
    {
        return createTestMessage(basicContentHeaderProperties, null, arrivalTime);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         final byte[] content,
                                         final long arrivalTime)
    {
        final ContentHeaderBody contentHeaderBody = mock(ContentHeaderBody.class);
        when(contentHeaderBody.getProperties()).thenReturn(basicContentHeaderProperties);

        final StoredMessage<MessageMetaData> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData())
                .thenReturn(new MessageMetaData(new MessagePublishInfo(), contentHeaderBody, arrivalTime));

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

        return new AMQMessage(storedMessage);
    }
}
