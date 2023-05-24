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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Footer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.FooterSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class ConsumerTarget_1_0Test extends UnitTestBase
{
    private static final AMQPDescribedTypeRegistry AMQP_DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer()
            .registerExtensionSoleconnLayer();

    private ConsumerTarget_1_0 _consumerTarget;
    private SendingLinkEndpoint _sendingLinkEndpoint;

    @BeforeAll
    void setUp()
    {
        final AMQPConnection_1_0 connection = mock(AMQPConnection_1_0.class);
        final Session_1_0 session = mock(Session_1_0.class);
        _sendingLinkEndpoint = mock(SendingLinkEndpoint.class);
        when(_sendingLinkEndpoint.getSession()).thenReturn(session);
        when(_sendingLinkEndpoint.isAttached()).thenReturn(true);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getConnection()).thenReturn(connection);
        when(connection.getDescribedTypeRegistry()).thenReturn(AMQP_DESCRIBED_TYPE_REGISTRY);
        when(connection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD)).thenReturn(10000L);

        _consumerTarget = new ConsumerTarget_1_0(_sendingLinkEndpoint, true);
    }

    @Test
    void TTLAdjustedOnSend() throws Exception
    {
        final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);

        final long ttl = 2000L;
        final long arrivalTime = System.currentTimeMillis() - 1000L;

        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        final Message_1_0 message = createTestMessage(header, arrivalTime);
        final MessageInstance messageInstance = mock(MessageInstance.class);
        when(messageInstance.getMessage()).thenReturn(message);

        final AtomicReference<QpidByteBuffer> payloadRef = new AtomicReference<>();
        doAnswer(invocation ->
        {
            final Object[] args = invocation.getArguments();
            final Transfer transfer = (Transfer) args[0];

            final QpidByteBuffer transferPayload = transfer.getPayload();

            final QpidByteBuffer payloadCopy = transferPayload.duplicate();
            payloadRef.set(payloadCopy);
            return null;
        }).when(_sendingLinkEndpoint).transfer(any(Transfer.class), anyBoolean());

        _consumerTarget.doSend(consumer, messageInstance, false);

        verify(_sendingLinkEndpoint, times(1)).transfer(any(Transfer.class), anyBoolean());

        final List<EncodingRetainingSection<?>> sections;
        try (final QpidByteBuffer payload = payloadRef.get())
        {
            sections = new SectionDecoderImpl(AMQP_DESCRIBED_TYPE_REGISTRY.getSectionDecoderRegistry()).parseAll(payload);
        }
        Header sentHeader = null;
        for (final EncodingRetainingSection<?> section : sections)
        {
            if (section instanceof HeaderSection)
            {
                sentHeader = ((HeaderSection) section).getValue();
            }
        }

        assertNotNull(sentHeader, "Header is not found");
        assertNotNull(sentHeader.getTtl(), "Ttl is not set");
        assertTrue(sentHeader.getTtl().longValue() <= 1000, "Unexpected ttl");
    }

    private Message_1_0 createTestMessage(final Header header, long arrivalTime)
    {
        final DeliveryAnnotationsSection deliveryAnnotations =
                new DeliveryAnnotations(Map.of()).createEncodingRetainingSection();
        final MessageAnnotationsSection messageAnnotations =
                new MessageAnnotations(Map.of()).createEncodingRetainingSection();
        final ApplicationPropertiesSection applicationProperties =
                new ApplicationProperties(Map.of()).createEncodingRetainingSection();
        final FooterSection footer = new Footer(Map.of()).createEncodingRetainingSection();
        final MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
                deliveryAnnotations,
                messageAnnotations,
                new Properties().createEncodingRetainingSection(),
                applicationProperties,
                footer,
                arrivalTime,
                0);

        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getContent(eq(0), anyInt())).thenReturn(QpidByteBuffer.emptyQpidByteBuffer());
        when(storedMessage.getMetaData()).thenReturn(metaData);
        return new Message_1_0(storedMessage);
    }
}
