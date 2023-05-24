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

class PropertyConverter_v0_8_to_InternalTest extends UnitTestBase
{
    private MessageConverter_v0_8_to_Internal _messageConverter;
    private NamedAddressSpace _addressSpace;

    @BeforeEach
    void setUp() throws Exception
    {
        _messageConverter = new MessageConverter_v0_8_to_Internal();
        _addressSpace = mock(NamedAddressSpace.class);
    }

    @Test
    void deliveryModePersistentConversion()
    {
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertTrue(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertTrue(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void deliveryModeNonPersistentConversion()
    {
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setDeliveryMode(BasicContentHeaderProperties.NON_PERSISTENT);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertFalse(convertedMessage.isPersistent(), "Unexpected persistence of message");
        assertFalse(convertedMessage.getStoredMessage().getMetaData().isPersistent(),
                "Unexpected persistence of meta data");
    }

    @Test
    void priorityConversion()
    {
        final byte priority = (byte) 7;
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setPriority(priority);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(priority, (long) convertedMessage.getMessageHeader().getPriority(), "Unexpected priority");
    }

    @Test
    void expirationConversion()
    {
        final long ttl = 10000;
        final long arrivalTime = System.currentTimeMillis();
        final long expiryTime = arrivalTime + ttl;
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setExpiration(expiryTime);
        final AMQMessage originalMessage = createTestMessage(header, arrivalTime);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(expiryTime, convertedMessage.getMessageHeader().getExpiration(), "Unexpected expiration");
    }

    @Test
    void contentEncodingConversion()
    {
        final String contentEncoding = "my-test-encoding";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setEncoding(contentEncoding);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentEncoding, convertedMessage.getMessageHeader().getEncoding(), "Unexpected content encoding");

    }

    @Test
    void messageIdConversion()
    {
        final String messageId = "testMessageId";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setMessageId(messageId);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(messageId, convertedMessage.getMessageHeader().getMessageId(), "Unexpected messageId");
    }

    @Test
    void correlationIdStringConversion()
    {
        final String correlationId = "testMessageCorrelationId";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setCorrelationId(correlationId);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(correlationId, convertedMessage.getMessageHeader().getCorrelationId(), "Unexpected correlationId");
    }

    @Test
    void userIdConversion()
    {
        final String userId = "testUserId";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setUserId(userId);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(userId, convertedMessage.getMessageHeader().getUserId(), "Unexpected userId");
    }

    @Test
    void replyToConversionForDirectExchangeAndRoutingKey()
    {
        final String exchangeName = "amq.direct";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s://%s//?routingkey='%s'", "direct", exchangeName, routingKey);
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(exchangeName + "/" + routingKey, convertedMessage.getMessageHeader().getReplyTo(),
                "Unexpected replyTo");
    }

    @Test
    void replyToConversionForFanoutExchange()
    {
        final String exchangeName = "amq.fanout";
        final String replyTo = String.format("%s://%s//", "fanout", exchangeName);
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(exchangeName, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    void replyToConversionForDefaultDestination()
    {
        final String exchangeName = "";
        final String routingKey = "testRoutingKey";
        final String replyTo = String.format("%s://%s//?routingkey='%s'", "direct", exchangeName, routingKey);
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(routingKey, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    void replyToNonBurl()
    {
        final String replyTo = "test/routing";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setReplyTo(replyTo);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(replyTo, convertedMessage.getMessageHeader().getReplyTo(), "Unexpected replyTo");
    }

    @Test
    void timestampConversion()
    {
        final long creationTime = System.currentTimeMillis();
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setTimestamp(creationTime);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(creationTime, convertedMessage.getMessageHeader().getTimestamp(), "Unexpected timestamp");
    }

    @Test
    void headersConversion()
    {
        final Map<String, Object> properties = Map.of("testProperty1", "testProperty1Value",
                "intProperty", 1);
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setHeaders(FieldTable.convertToFieldTable(properties));
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        final Map<String, Object> headers = convertedMessage.getMessageHeader().getHeaderMap();
        assertEquals(properties, new HashMap<>(headers), "Unexpected headers");
    }

    @Test
    void contentTypeConversion()
    {
        final String contentType = "text/json";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setContentType(contentType);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(contentType, convertedMessage.getMessageHeader().getMimeType(), "Unexpected content type");
    }

    @Test
    void typeConversion()
    {
        final String type = "JMSType";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setType(type);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(type, convertedMessage.getMessageHeader().getType(), "Unexpected type");
    }

    @Test
    void applicationIdConversion()
    {
        final String applicationId = "appId";
        final BasicContentHeaderProperties header = new BasicContentHeaderProperties();
        header.setAppId(applicationId);
        final AMQMessage originalMessage = createTestMessage(header);

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(applicationId, convertedMessage.getMessageHeader().getAppId(), "Unexpected applicationId");
    }

    @Test
    void basicPublishConversion()
    {
        final String exchangeName = "amq.direct";
        final String testRoutingKey = "test-routing-key";

        final AMQMessage originalMessage = createTestMessage(new BasicContentHeaderProperties());
        originalMessage.getMessagePublishInfo().setRoutingKey(AMQShortString.valueOf(testRoutingKey));
        originalMessage.getMessagePublishInfo().setExchange(AMQShortString.valueOf(exchangeName));

        final InternalMessage convertedMessage = _messageConverter.convert(originalMessage, _addressSpace);

        assertEquals(exchangeName, convertedMessage.getTo(), "Unexpected to");

        // TODO: QPID-7868 : add test for initialRoutingAddress
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
