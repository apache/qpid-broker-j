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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

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

public class ConsumerTarget_1_0Test extends UnitTestBase
{
    private final AMQPDescribedTypeRegistry _describedTypeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                              .registerTransportLayer()
                                                                                              .registerMessagingLayer()
                                                                                              .registerTransactionLayer()
                                                                                              .registerSecurityLayer()
                                                                                              .registerExtensionSoleconnLayer();
    private ConsumerTarget_1_0 _consumerTarget;
    private SendingLinkEndpoint _sendingLinkEndpoint;

    @Before
    public void setUp() throws Exception
    {
        final AMQPConnection_1_0 connection = mock(AMQPConnection_1_0.class);
        final Session_1_0 session = mock(Session_1_0.class);
        _sendingLinkEndpoint = mock(SendingLinkEndpoint.class);
        when(_sendingLinkEndpoint.getSession()).thenReturn(session);
        when(_sendingLinkEndpoint.isAttached()).thenReturn(true);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getConnection()).thenReturn(connection);
        when(connection.getDescribedTypeRegistry()).thenReturn(_describedTypeRegistry);
        when(connection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD)).thenReturn(10000L);

        _consumerTarget = new ConsumerTarget_1_0(_sendingLinkEndpoint, true);
    }

    @Test
    public void testTTLAdjustedOnSend() throws Exception
    {
        final MessageInstanceConsumer comsumer = mock(MessageInstanceConsumer.class);

        long ttl = 2000L;
        long arrivalTime = System.currentTimeMillis() - 1000L;

        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        final Message_1_0 message = createTestMessage(header, arrivalTime);
        final MessageInstance messageInstance = mock(MessageInstance.class);
        when(messageInstance.getMessage()).thenReturn(message);

        AtomicReference<QpidByteBuffer> payloadRef = new AtomicReference<>();
        doAnswer(invocation ->
                 {
                     final Object[] args = invocation.getArguments();
                     Transfer transfer = (Transfer) args[0];

                     QpidByteBuffer transferPayload = transfer.getPayload();

                     QpidByteBuffer payloadCopy = transferPayload.duplicate();
                     payloadRef.set(payloadCopy);
                     return null;
                 }).when(_sendingLinkEndpoint).transfer(any(Transfer.class), anyBoolean());

        _consumerTarget.doSend(comsumer, messageInstance, false);

        verify(_sendingLinkEndpoint, times(1)).transfer(any(Transfer.class), anyBoolean());

        final List<EncodingRetainingSection<?>> sections;
        try (QpidByteBuffer payload = payloadRef.get())
        {
            sections = new SectionDecoderImpl(_describedTypeRegistry.getSectionDecoderRegistry()).parseAll(payload);
        }
        Header sentHeader = null;
        for (EncodingRetainingSection<?> section : sections)
        {
            if (section instanceof HeaderSection)
            {
                sentHeader = ((HeaderSection) section).getValue();
            }
        }

        assertNotNull("Header is not found", sentHeader);
        assertNotNull("Ttl is not set", sentHeader.getTtl());
        assertTrue("Unexpected ttl", sentHeader.getTtl().longValue() <= 1000);
    }

    private Message_1_0 createTestMessage(final Header header, long arrivalTime)
    {
        DeliveryAnnotationsSection deliveryAnnotations =
                new DeliveryAnnotations(Collections.emptyMap()).createEncodingRetainingSection();
        MessageAnnotationsSection messageAnnotations =
                new MessageAnnotations(Collections.emptyMap()).createEncodingRetainingSection();
        ApplicationPropertiesSection applicationProperties =
                new ApplicationProperties(Collections.emptyMap()).createEncodingRetainingSection();
        FooterSection footer = new Footer(Collections.emptyMap()).createEncodingRetainingSection();
        MessageMetaData_1_0 metaData = new MessageMetaData_1_0(header.createEncodingRetainingSection(),
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
