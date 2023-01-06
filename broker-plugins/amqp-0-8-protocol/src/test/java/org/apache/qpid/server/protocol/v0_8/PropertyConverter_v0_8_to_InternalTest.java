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

package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyConverter_v0_8_to_InternalTest extends UnitTestBase
{
    private MessageConverter_v0_8_to_Internal _messageConverter;
    private NamedAddressSpace _addressSpace;

    @BeforeEach
    public void setUp() throws Exception
    {
        _messageConverter = new MessageConverter_v0_8_to_Internal();
        _addressSpace = mock(NamedAddressSpace.class);
    }

    @Test
    public void testDeliveryModePersistentConversion()
    {
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    public void testDeliveryModeNonPersistentConversion()
    {
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    public void testPriorityConversion()
    {
        byte priority = (byte) 7;
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setPriority(priority);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(priority, (long) convertedMessage.getMessageHeader().getPriority(), "Unexpected priority");

    }

    @Test
    public void testExpirationConversion()
    {
        long ttl = 10000;
        long arrivalTime = System.currentTimeMillis();
        long expiryTime = arrivalTime + ttl;
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setExpiration(expiryTime);
        final AMQMessage originalMessage = createTestMessage(header, arrivalTime);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(expiryTime, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    public void testContentEncodingConversion()
    {
        String contentEncoding = "my-test-encoding";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setEncoding(contentEncoding);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentEncoding, convertedMessage.getMessageHeader().getEncoding(), "Unexpected content encoding");

    }

    @Test
    public void testMessageIdConversion()
    {
        final String messageId = "testMessageId";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setMessageId(messageId);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId, convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    public void testCorrelationIdStringConversion()
    {
        final String correlationId = "testMessageCorrelationId";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setCorrelationId(correlationId);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(correlationId, convertedMessage.getMessageHeader().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    public void testUserIdConversion()
    {
        final String userId = "testUserId";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setUserId(userId);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(userId, convertedMessage.getMessageHeader().getUserId(), "Unexpected userId");
    }

    @Test
    public void testReplyToConversionForDirectExchangeAndRoutingKey()
    {
        String exchangeName = "amq.direct";
        String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s://%s//?routingkey='%s'", "direct", exchangeName, routingKey);
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(exchangeName + "/" + routingKey, convertedMessage.getMessageHeader().getReplyTo(),
                "Unexpected replyTo");
    }

    @Test
    public void testReplyToConversionForFanoutExchange()
    {
        String exchangeName = "amq.fanout";
        final String replyTo = String.format("%s://%s//", "fanout", exchangeName);
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(exchangeName, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    public void testReplyToConversionForDefaultDestination()
    {
        String exchangeName = "";
        String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s://%s//?routingkey='%s'", "direct", exchangeName, routingKey);
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(routingKey, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    public void testReplyToNonBurl()
    {
        final String replyTo = "test/routing";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(replyTo, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    public void testTimestampConversion()
    {
        final long creationTime = System.currentTimeMillis();
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setTimestamp(creationTime);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(creationTime, convertedMessage.getMessageHeader().getTimestamp(), "Unexpected timestamp");
    }

    @Test
    public void testHeadersConversion()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty1", "testProperty1Value");
        properties.put("intProperty", 1);
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setHeaders(FieldTable.convertToFieldTable(properties));
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> headers = convertedMessage.getMessageHeader().getHeaderMap();
        assertEquals(properties, new HashMap<>(headers), "Unexpected headers");
    }

    @Test
    public void testContentTypeConversion()
    {
        final String contentType = "text/json";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setContentType(contentType);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected content type");
    }

    @Test
    public void testTypeConversion()
    {
        final String type = "JMSType";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setType(type);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(type, convertedMessage.getMessageHeader().getType(), "Unexpected type");
    }

    @Test
    public void testApplicationIdConversion()
    {
        final String applicationId = "appId";
        BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setAppId(applicationId);
        final AMQMessage originalMessage = createTestMessage(header);

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(applicationId, convertedMessage.getMessageHeader().getAppId(), "Unexpected applicationId");
    }

    @Test
    public void testBasicPublishConversion()
    {
        final String exchangeName = "amq.direct";
        final String testRoutingKey = "test-routing-key";

        final AMQMessage originalMessage = createTestMessage(new BasicContentHeaderProperties());
        originalMessage.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));
        originalMessage.getMessagePublishInfo().setExchange(AMQShortString.valueOf(exchangeName));

        InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(exchangeName, convertedMessage.getTo(), "Unexpected to");

        // TODO: QPID-7868 : add test for initialRoutingAddress
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
