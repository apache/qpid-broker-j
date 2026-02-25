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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.junit.jupiter.api.BeforeEach;
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

    @BeforeEach
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
        assertNotNull(sentHeader.getTtl(), "TTL is not set");
        assertTrue(sentHeader.getTtl().longValue() <= 1000, "Unexpected TTL");
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

    @Test
    void bodyOnlyMessageIsSentAsSingleDataSection() throws Exception
    {
        final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);
        final byte[] body = new byte[]{10, 11, 12};
        final Message_1_0 message = createBodyOnlyMessage(body);

        final MessageInstance messageInstance = mock(MessageInstance.class);
        when(messageInstance.getMessage()).thenReturn(message);
        when(messageInstance.getDeliveryCount()).thenReturn(0);

        final AtomicReference<QpidByteBuffer> payloadRef = new AtomicReference<>();
        doAnswer(invocation ->
        {
            final Transfer transfer = (Transfer) invocation.getArguments()[0];
            try (QpidByteBuffer transferPayload = transfer.getPayload())
            {
                payloadRef.set(transferPayload == null ? null : transferPayload.duplicate());
            }
            return null;
        }).when(_sendingLinkEndpoint).transfer(any(Transfer.class), anyBoolean());

        _consumerTarget.doSend(consumer, messageInstance, false);

        try (final QpidByteBuffer payload = payloadRef.get())
        {
            final List<EncodingRetainingSection<?>> sections =
                    new SectionDecoderImpl(AMQP_DESCRIBED_TYPE_REGISTRY.getSectionDecoderRegistry()).parseAll(payload);

            assertEquals(1, sections.size());
            assertInstanceOf(DataSection.class, sections.get(0));
            final DataSection dataSection = (DataSection) sections.get(0);
            assertArrayEquals(body, dataSection.getValue().getArray());
            sections.forEach(EncodingRetainingSection::dispose);
        }
    }

    @Test
    void headerAndBodyOnlyMessageIncludesHeaderSection() throws Exception
    {
        final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);
        final byte[] body = new byte[]{1, 2, 3, 4};

        final Header header = new Header();
        header.setDurable(true);
        header.setPriority(UnsignedByte.valueOf((byte) 7));

        final Message_1_0 message = createHeaderAndBodyOnlyMessage(header, body, System.currentTimeMillis());

        final MessageInstance messageInstance = mock(MessageInstance.class);
        when(messageInstance.getMessage()).thenReturn(message);
        when(messageInstance.getDeliveryCount()).thenReturn(0);

        final AtomicReference<QpidByteBuffer> payloadRef = new AtomicReference<>();
        doAnswer(invocation ->
        {
            final Transfer transfer = (Transfer) invocation.getArguments()[0];
            try (QpidByteBuffer transferPayload = transfer.getPayload())
            {
                payloadRef.set(transferPayload == null ? null : transferPayload.duplicate());
            }
            return null;
        }).when(_sendingLinkEndpoint).transfer(any(Transfer.class), anyBoolean());

        _consumerTarget.doSend(consumer, messageInstance, false);

        try (final QpidByteBuffer payload = payloadRef.get())
        {
            final List<EncodingRetainingSection<?>> sections =
                    new SectionDecoderImpl(AMQP_DESCRIBED_TYPE_REGISTRY.getSectionDecoderRegistry()).parseAll(payload);

            assertEquals(2, sections.size(), "Unexpected number of sections");

            Header sentHeader = null;
            Binary sentBody = null;
            for (final EncodingRetainingSection<?> section : sections)
            {
                if (section instanceof HeaderSection)
                {
                    sentHeader = ((HeaderSection) section).getValue();
                }
                else if (section instanceof DataSection)
                {
                    sentBody = ((DataSection) section).getValue();
                }
            }

            assertNotNull(sentHeader, "Header is not found");
            assertEquals(Boolean.TRUE, sentHeader.getDurable());
            assertEquals(UnsignedInteger.valueOf(7).intValue(), sentHeader.getPriority().intValue());
            assertNotNull(sentBody, "Body is not found");
            assertArrayEquals(body, sentBody.getArray());

            sections.forEach(EncodingRetainingSection::dispose);
        }
    }

    @Test
    void deliveryCountAndTtlAreAppliedForHeaderAndBodyOnlyMessage() throws Exception
    {
        final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);
        final byte[] body = new byte[]{10, 20, 30};

        final long ttl = 2000L;
        final long arrivalTime = System.currentTimeMillis() - 1000L;

        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));

        final Message_1_0 message = createHeaderAndBodyOnlyMessage(header, body, arrivalTime);

        final int deliveryCount = 5;
        final MessageInstance messageInstance = mock(MessageInstance.class);
        when(messageInstance.getMessage()).thenReturn(message);
        when(messageInstance.getDeliveryCount()).thenReturn(deliveryCount);

        final AtomicReference<QpidByteBuffer> payloadRef = new AtomicReference<>();
        doAnswer(invocation ->
        {
            final Transfer transfer = (Transfer) invocation.getArguments()[0];
            try (QpidByteBuffer transferPayload = transfer.getPayload())
            {
                payloadRef.set(transferPayload == null ? null : transferPayload.duplicate());
            }
            return null;
        }).when(_sendingLinkEndpoint).transfer(any(Transfer.class), anyBoolean());

        _consumerTarget.doSend(consumer, messageInstance, false);

        final List<EncodingRetainingSection<?>> sections;
        try (final QpidByteBuffer payload = payloadRef.get())
        {
            sections = new SectionDecoderImpl(AMQP_DESCRIBED_TYPE_REGISTRY.getSectionDecoderRegistry()).parseAll(payload);
        }

        Header sentHeader = null;
        Binary sentBody = null;
        for (final EncodingRetainingSection<?> section : sections)
        {
            if (section instanceof HeaderSection)
            {
                sentHeader = ((HeaderSection) section).getValue();
            }
            else if (section instanceof DataSection)
            {
                sentBody = ((DataSection) section).getValue();
            }
        }

        assertNotNull(sentHeader, "Header is not found");
        assertEquals(UnsignedInteger.valueOf(deliveryCount), sentHeader.getDeliveryCount(), "Delivery count not set");
        assertNotNull(sentHeader.getTtl(), "TTL is not set");
        assertTrue(sentHeader.getTtl().longValue() <= 1000, "Unexpected TTL");

        assertNotNull(sentBody, "Body is not found");
        assertArrayEquals(body, sentBody.getArray());

        sections.forEach(EncodingRetainingSection::dispose);
    }

    @Test
    void nullBodyContentDoesNotSetPayloadAndDoesNotThrow()
    {
        final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);
        final Message_1_0 message = mock(Message_1_0.class);
        when(message.getContent()).thenReturn(null);
        when(message.getHeaderSection()).thenReturn(null);
        when(message.getDeliveryAnnotationsSection()).thenReturn(null);
        when(message.getMessageAnnotationsSection()).thenReturn(null);
        when(message.getPropertiesSection()).thenReturn(null);
        when(message.getApplicationPropertiesSection()).thenReturn(null);
        when(message.getFooterSection()).thenReturn(null);
        when(message.getSize()).thenReturn(0L);

        final MessageInstance messageInstance = mock(MessageInstance.class);
        when(messageInstance.getMessage()).thenReturn(message);
        when(messageInstance.getDeliveryCount()).thenReturn(0);

        final AtomicReference<QpidByteBuffer> payloadRef = new AtomicReference<>();
        doAnswer(invocation ->
        {
            final Transfer transfer = (Transfer) invocation.getArguments()[0];
            payloadRef.set(transfer.getPayload());
            return null;
        }).when(_sendingLinkEndpoint).transfer(any(Transfer.class), anyBoolean());

        _consumerTarget.doSend(consumer, messageInstance, false);

        assertNull(payloadRef.get(), "Payload must remain null when body content is null");
    }

    private Message_1_0 createHeaderAndBodyOnlyMessage(final Header header, final byte[] body, final long arrivalTime)
    {
        final DataSection dataSection = new Data(new Binary(body)).createEncodingRetainingSection();
        final byte[] encodedBody;
        try (final QpidByteBuffer encoded = dataSection.getEncodedForm())
        {
            encodedBody = new byte[encoded.remaining()];
            encoded.copyTo(encodedBody);
        }
        finally
        {
            dataSection.dispose();
        }

        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData()).thenReturn(new MessageMetaData_1_0(
                header.createEncodingRetainingSection(), null, null, null, null, null, arrivalTime, encodedBody.length));
        when(storedMessage.getContentSize()).thenReturn(encodedBody.length);
        when(storedMessage.isInContentInMemory()).thenReturn(true);
        when(storedMessage.getContent(anyInt(), anyInt())).thenAnswer(inv ->
        {
            final int offset = inv.getArgument(0);
            final int length = inv.getArgument(1);
            return QpidByteBuffer.wrap(encodedBody, offset, length);
        });

        return new Message_1_0(storedMessage);
    }

    private Message_1_0 createBodyOnlyMessage(final byte[] body)
    {
        final DataSection dataSection = new Data(new Binary(body)).createEncodingRetainingSection();
        final byte[] encodedBody;
        try (final QpidByteBuffer encoded = dataSection.getEncodedForm())
        {
            encodedBody = new byte[encoded.remaining()];
            encoded.copyTo(encodedBody);
        }
        finally
        {
            dataSection.dispose();
        }

        final StoredMessage<MessageMetaData_1_0> storedMessage = mock(StoredMessage.class);
        when(storedMessage.getMetaData()).thenReturn(new MessageMetaData_1_0(
                null, null, null, null, null, null, System.currentTimeMillis(), encodedBody.length));
        when(storedMessage.getContentSize()).thenReturn(encodedBody.length);
        when(storedMessage.isInContentInMemory()).thenReturn(true);
        when(storedMessage.getContent(anyInt(), anyInt())).thenAnswer(inv ->
        {
            final int offset = inv.getArgument(0);
            final int length = inv.getArgument(1);
            return QpidByteBuffer.wrap(encodedBody, offset, length);
        });

        return new Message_1_0(storedMessage);
    }
}
