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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
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
import org.apache.qpid.test.utils.QpidTestCase;

public class PropertyConverter_0_8_to_1_0Test extends QpidTestCase
{
    private NamedAddressSpace _namedAddressSpace;
    private MessageConverter_0_8_to_1_0 _messageConverter;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _namedAddressSpace = mock(NamedAddressSpace.class);
        _messageConverter = new MessageConverter_0_8_to_1_0();
    }

    public void testContentTypeConversion()
    {
        String contentType = "test-content-type";
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setContentType(contentType);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected content type", contentType, properties.getContentType().toString());
    }

    public void testContentTypeJavaObjectStreamConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setContentType("application/java-object-stream");
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected content type",
                     "application/x-java-serialized-object",
                     properties.getContentType().toString());
    }

    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setEncoding(contentEncoding);
        AMQMessage message = createTestMessage(basicContentHeaderProperties, new byte[]{(byte) 1}, 0);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected content encoding", contentEncoding, properties.getContentEncoding().toString());
    }

    public void testHeaderConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("intProperty", 1);
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertEquals("Unexpected application applicationProperties", headers, new HashMap<>(applicationProperties));
    }

    public void testHeaderConversionWhenQpidSubjectIsPresent()
    {
        String testSubject = "testSubject";
        Map<String, Object> headers = new HashMap<>();
        headers.put("testProperty1", "testProperty1Value");
        headers.put("qpid.subject", testSubject);
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected subject", testSubject, properties.getSubject());
        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected subject in application properties", applicationProperties.containsKey("qpid.subject"));
    }

    public void testPersistentDeliveryModeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertTrue("Unexpected durable header", header.getDurable());
    }

    public void testNonPersistentDeliveryModeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertFalse("Unexpected durable header", header.getDurable());
    }

    public void testPriorityConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final byte priority = 5;
        basicContentHeaderProperties.setPriority(priority);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertEquals("Unexpected priority", priority, header.getPriority().byteValue());
    }

    public void testCorrelationIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String correlationId = "testCorrelationId";
        basicContentHeaderProperties.setCorrelationId(AMQShortString.valueOf(correlationId));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected correlationId", correlationId, properties.getCorrelationId());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct://amq.direct/destination_name/queue_name?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "amq.direct/test_routing_key", properties.getReplyTo());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed2()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct://amq.direct//queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "amq.direct/queue_name", properties.getReplyTo());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed3()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct:////queue_name";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "queue_name", properties.getReplyTo());
    }

    public void testReplyToConversionWhenBindingURLFormatIsUsed4()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "direct:////?routingkey='test_routing_key'";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "test_routing_key", properties.getReplyTo());
    }

    public void testReplyToConversionWhenNonBindingURLFormatIsUsed()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();

        final String replyTo = "test";
        basicContentHeaderProperties.setReplyTo(replyTo);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected reply-to", "test", properties.getReplyTo());
    }

    public void testExpirationConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        long timestamp = System.currentTimeMillis();
        int ttl = 100000;
        final long expiration = timestamp + ttl;
        basicContentHeaderProperties.setExpiration(expiration);
        basicContentHeaderProperties.setTimestamp(timestamp);
        AMQMessage message = createTestMessage(basicContentHeaderProperties, timestamp);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Header header = convertedMessage.getHeaderSection().getValue();
        assertEquals("Unexpected TTL", ttl, header.getTtl().longValue());

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected expiration", expiration, properties.getAbsoluteExpiryTime().getTime());
    }

    public void testMessageIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String messageId = "testMessageId";
        basicContentHeaderProperties.setMessageId(messageId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected messageId", messageId, properties.getMessageId());
    }

    public void testTimestampConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final long timestamp = System.currentTimeMillis() - 10000;
        basicContentHeaderProperties.setTimestamp(timestamp);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected creation timestamp", timestamp, properties.getCreationTime().getTime());
    }

    public void testTypeConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String type = "test-type";
        basicContentHeaderProperties.setType(String.valueOf(type));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected subject", type, properties.getSubject());
    }

    public void testUserIdConversion()
    {
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        final String userId = "test-userId";
        basicContentHeaderProperties.setUserId(userId);
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();
        assertEquals("Unexpected user-id", userId, new String(properties.getUserId().getArray(), UTF_8));
    }

    public void testHeaderJMSXGroupIdConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        String testGroupId = "testGroupId";
        headers.put("JMSXGroupID", testGroupId);
        headers.put("intProperty", 1);
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected group-id", testGroupId, properties.getGroupId());

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected JMSXGroupID in application properties",
                    applicationProperties.containsKey("JMSXGroupID"));
    }

    public void testHeaderJMSXGroupSeqConversion()
    {
        Map<String, Object> headers = new HashMap<>();
        int testGroupSequenceNumber = 1;
        headers.put("JMSXGroupSeq", testGroupSequenceNumber);
        headers.put("intProperty", 1);
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected group-sequence", testGroupSequenceNumber, properties.getGroupSequence().intValue());

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();
        assertFalse("Unexpected JMSXGroupSeq in application properties",
                    applicationProperties.containsKey("JMSXGroupSeq"));
    }

    public void testHeaderJMSXGroupSeqConversionWhenWrongType()
    {
        Map<String, Object> headers = new HashMap<>();
        short testGroupSequenceNumber = (short) 1;
        headers.put("JMSXGroupSeq", testGroupSequenceNumber);
        headers.put("intProperty", 1);
        BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(FieldTable.convertToFieldTable(headers));
        AMQMessage message = createTestMessage(basicContentHeaderProperties);

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected group-sequence", null, properties.getGroupSequence());

        Map<String, Object> applicationProperties = convertedMessage.getApplicationPropertiesSection().getValue();

        assertTrue("JMSXGroupSeq was removed from application properties",
                   applicationProperties.containsKey("JMSXGroupSeq"));
    }

    public void testExchangeRoutingKeyConversion()
    {
        final String testExchange = "testExchange";
        final String testRoutingKey = "testRoutingKey";
        AMQMessage message = createTestMessage(new BasicContentHeaderProperties());
        message.getMessagePublishInfo().setExchange(AMQShortString.valueOf(testExchange));
        message.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));

        final Message_1_0 convertedMessage = _messageConverter.convert(message, _namedAddressSpace);

        Properties properties = convertedMessage.getPropertiesSection().getValue();

        assertEquals("Unexpected to", testExchange + "/" + testRoutingKey, properties.getTo());
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties)
    {
        return createTestMessage(basicContentHeaderProperties, null, 0);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         long arrivalTime)
    {
        return createTestMessage(basicContentHeaderProperties, null, arrivalTime);
    }

    private AMQMessage createTestMessage(final BasicContentHeaderProperties basicContentHeaderProperties,
                                         final byte[] content, final long arrivalTime)
    {
        final ContentHeaderBody contentHeaderBody = mock(ContentHeaderBody.class);
        when(contentHeaderBody.getProperties()).thenReturn(basicContentHeaderProperties);

        final StoredMessage<MessageMetaData> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData()).thenReturn(new MessageMetaData(new MessagePublishInfo(),
                                                                         contentHeaderBody,
                                                                         arrivalTime));

        if (content != null)
        {
            when(storedMessage.getContentSize()).thenReturn(content.length);
            when(storedMessage.getContent(0, content.length)).thenReturn(Collections.singleton(QpidByteBuffer.wrap(
                    content)));
        }

        return new AMQMessage(storedMessage);
    }
}
