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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Producer;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistryImpl;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"unchecked"})
public class StandardReceivingLinkEndpointTest extends UnitTestBase
{
    @Test
    public void linkAddedAndRemovedToExchange()
    {
        final SectionDecoderRegistry sectionDecoderRegistry = mock(SectionDecoderRegistry.class);

        final AMQPDescribedTypeRegistry amqpDescribedTypeRegistry = mock(AMQPDescribedTypeRegistry.class);
        doReturn(sectionDecoderRegistry).when(amqpDescribedTypeRegistry).getSectionDecoderRegistry();

        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        doReturn(amqpDescribedTypeRegistry).when(connection).getDescribedTypeRegistry();
        doReturn(AMQPConnection_1_0.DEFAULT_MAX_TRANSFERS_PER_DELIVERY).when(connection)
                .getContextValue(Integer.class, AMQPConnection_1_0.CONNECTION_MAX_TRANSFERS_PER_DELIVERY);

        final Session_1_0 session = mock(Session_1_0.class);
        doReturn(connection).when(session).getConnection();

        final Link_1_0<Source, Target> link = mock(Link_1_0.class);
        doReturn("test-link").when(link).getName();

        final StandardReceivingLinkEndpoint standardReceivingLinkEndpoint =
                new StandardReceivingLinkEndpoint(session, link);

        final Exchange<?> exchange = mock(Exchange.class);

        final ReceivingDestination receivingDestination = mock(ReceivingDestination.class);
        doReturn(exchange).when(receivingDestination).getMessageDestination();

        standardReceivingLinkEndpoint.setDestination(receivingDestination);

        verify(session).addProducer(any(PublishingLink.class), eq(exchange));
        verify(exchange).linkAdded(any(MessageSender.class), any(PublishingLink.class));

        standardReceivingLinkEndpoint.destroy();

        verify(session).removeProducer(any(PublishingLink.class));
        verify(exchange).linkRemoved(any(MessageSender.class), any(PublishingLink.class));
    }

    @Test
    public void linkAddedAndRemovedToQueue()
    {
        final SectionDecoderRegistry sectionDecoderRegistry = mock(SectionDecoderRegistry.class);

        final AMQPDescribedTypeRegistry amqpDescribedTypeRegistry = mock(AMQPDescribedTypeRegistry.class);
        doReturn(sectionDecoderRegistry).when(amqpDescribedTypeRegistry).getSectionDecoderRegistry();

        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        doReturn(amqpDescribedTypeRegistry).when(connection).getDescribedTypeRegistry();
        doReturn(AMQPConnection_1_0.DEFAULT_MAX_TRANSFERS_PER_DELIVERY).when(connection)
                .getContextValue(Integer.class, AMQPConnection_1_0.CONNECTION_MAX_TRANSFERS_PER_DELIVERY);

        final Session_1_0 session = mock(Session_1_0.class);
        doReturn(connection).when(session).getConnection();

        final Link_1_0<Source, Target> link = mock(Link_1_0.class);
        doReturn("test-link").when(link).getName();

        final StandardReceivingLinkEndpoint standardReceivingLinkEndpoint =
                new StandardReceivingLinkEndpoint(session, link);

        final Queue<?> queue = mock(Queue.class);

        final ReceivingDestination receivingDestination = mock(ReceivingDestination.class);
        doReturn(queue).when(receivingDestination).getMessageDestination();

        standardReceivingLinkEndpoint.setDestination(receivingDestination);

        verify(session).addProducer(any(PublishingLink.class), eq(queue));
        verify(queue).linkAdded(any(MessageSender.class), any(PublishingLink.class));

        standardReceivingLinkEndpoint.destroy();

        verify(session).removeProducer(any(PublishingLink.class));
        verify(queue).linkRemoved(any(MessageSender.class), any(PublishingLink.class));
    }

    @Test
    public void linkAddedAndRemovedAnonymously()
    {
        final SectionDecoderRegistry sectionDecoderRegistry = mock(SectionDecoderRegistry.class);

        final AMQPDescribedTypeRegistry amqpDescribedTypeRegistry = mock(AMQPDescribedTypeRegistry.class);
        doReturn(sectionDecoderRegistry).when(amqpDescribedTypeRegistry).getSectionDecoderRegistry();

        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        doReturn(amqpDescribedTypeRegistry).when(connection).getDescribedTypeRegistry();
        doReturn(CurrentThreadTaskExecutor.newStartedInstance()).when(connection).getChildExecutor();
        doReturn(BrokerModel.getInstance()).when(connection).getModel();
        doReturn(new Subject()).when(connection).getSubject();
        doReturn(mock(Broker.class)).when(connection).getBroker();
        doReturn(mock(NamedAddressSpace.class)).when(connection).getAddressSpace();
        doReturn(mock(EventLogger.class)).when(connection).getEventLogger();
        doReturn(0L).when(connection).getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT);
        doReturn(0).when(connection).getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE);
        doReturn(AMQPConnection_1_0.DEFAULT_MAX_TRANSFERS_PER_DELIVERY).when(connection)
                .getContextValue(Integer.class, AMQPConnection_1_0.CONNECTION_MAX_TRANSFERS_PER_DELIVERY);
        doReturn(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS).when(connection)
                .getContextValue(Integer.class, AMQPConnection_1_0.ECHO_FLOW_COALESCE_INTERVAL_MS);

        final Begin begin = mock(Begin.class);
        doReturn(UnsignedInteger.valueOf(0)).when(begin).getNextOutgoingId();
        final Session_1_0 session = spy(new Session_1_0(connection, begin, 0, 0, 1000L));

        final Link_1_0<Source, Target> link = mock(Link_1_0.class);
        doReturn("test-link").when(link).getName();

        final StandardReceivingLinkEndpoint standardReceivingLinkEndpoint =
                new StandardReceivingLinkEndpoint(session, link);

        final ReceivingDestination receivingDestination = mock(ReceivingDestination.class);
        doReturn(null).when(receivingDestination).getMessageDestination();

        assertEquals(0, session.getProducerCount());
        assertEquals(0, session.getChildren(Producer.class).size());

        standardReceivingLinkEndpoint.setDestination(receivingDestination);

        verify(session).addProducer(any(PublishingLink.class), eq(null));
        assertEquals(1, session.getProducerCount());
        assertEquals(1, session.getChildren(Producer.class).size());

        standardReceivingLinkEndpoint.destroy();

        verify(session).removeProducer(any(PublishingLink.class));
        assertEquals(0, session.getProducerCount());
        assertEquals(0, session.getChildren(Producer.class).size());
    }

    @Test
    public void createMessageUsesEndpointSectionDecoderForAmqpOneZeroFormat()
    {
        final SectionDecoderRegistry sectionDecoderRegistry = mock(SectionDecoderRegistry.class);

        final AMQPDescribedTypeRegistry amqpDescribedTypeRegistry = mock(AMQPDescribedTypeRegistry.class);
        doReturn(sectionDecoderRegistry).when(amqpDescribedTypeRegistry).getSectionDecoderRegistry();

        final MessageStore messageStore = mock(MessageStore.class);
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        doReturn(messageStore).when(addressSpace).getMessageStore();

        final Object connectionReference = new Object();
        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        doReturn(amqpDescribedTypeRegistry).when(connection).getDescribedTypeRegistry();
        doReturn(addressSpace).when(connection).getAddressSpace();
        doReturn(connectionReference).when(connection).getReference();

        final Session_1_0 session = mock(Session_1_0.class);
        doReturn(connection).when(session).getConnection();

        final Link_1_0<Source, Target> link = mock(Link_1_0.class);
        doReturn("test-link").when(link).getName();

        final StandardReceivingLinkEndpoint standardReceivingLinkEndpoint =
                new StandardReceivingLinkEndpoint(session, link);
        final RecordingMessageFormat messageFormat = new RecordingMessageFormat();

        try (QpidByteBuffer payload = QpidByteBuffer.emptyQpidByteBuffer())
        {
            standardReceivingLinkEndpoint.createMessage(messageFormat, payload);
        }

        assertSame(standardReceivingLinkEndpoint.getSectionDecoder(), messageFormat.getRecordedSectionDecoder());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    public void transferLimitExceededCleansIncompleteDeliveryState(final int configuredMaxTransfersPerDelivery)
    {
        final SectionDecoderRegistry sectionDecoderRegistry = mock(SectionDecoderRegistry.class);

        final AMQPDescribedTypeRegistry amqpDescribedTypeRegistry = mock(AMQPDescribedTypeRegistry.class);
        doReturn(sectionDecoderRegistry).when(amqpDescribedTypeRegistry).getSectionDecoderRegistry();

        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        doReturn(amqpDescribedTypeRegistry).when(connection).getDescribedTypeRegistry();
        doReturn(configuredMaxTransfersPerDelivery).when(connection)
                .getContextValue(Integer.class, AMQPConnection_1_0.CONNECTION_MAX_TRANSFERS_PER_DELIVERY);
        doReturn(Long.MAX_VALUE).when(connection).getMaxMessageSize();

        final DeliveryRegistryImpl deliveryRegistry = new DeliveryRegistryImpl();
        final Session_1_0 session = mock(Session_1_0.class);
        doReturn(connection).when(session).getConnection();
        doReturn(deliveryRegistry).when(session).getIncomingDeliveryRegistry();

        final Link_1_0<Source, Target> link = mock(Link_1_0.class);
        final StandardReceivingLinkEndpoint standardReceivingLinkEndpoint =
                new StandardReceivingLinkEndpoint(session, link);
        standardReceivingLinkEndpoint.setLinkCredit(UnsignedInteger.valueOf(1));
        standardReceivingLinkEndpoint.setDeliveryCount(new SequenceNumber(0));

        final Binary deliveryTag = new Binary(new byte[]{0x01});
        final Transfer initialTransfer = new Transfer();
        initialTransfer.setDeliveryId(UnsignedInteger.ZERO);
        initialTransfer.setDeliveryTag(deliveryTag);
        initialTransfer.setMore(true);

        standardReceivingLinkEndpoint.receiveTransfer(initialTransfer);

        assertEquals(1, deliveryRegistry.size());
        assertEquals(1, standardReceivingLinkEndpoint._unsettled.size());

        final Transfer overLimitTransfer = new Transfer();
        overLimitTransfer.setMore(true);

        standardReceivingLinkEndpoint.receiveTransfer(overLimitTransfer);

        assertEquals(0, deliveryRegistry.size());
        assertEquals(0, standardReceivingLinkEndpoint._unsettled.size());
    }

    private static final class RecordingMessageFormat extends MessageFormat_1_0
    {
        private SectionDecoder _recordedSectionDecoder;

        @Override
        Message_1_0 createMessage(final QpidByteBuffer payload,
                                  final MessageStore store,
                                  final Object connectionReference,
                                  final SectionDecoder sectionDecoder)
        {
            _recordedSectionDecoder = sectionDecoder;
            return null;
        }

        private SectionDecoder getRecordedSectionDecoder()
        {
            return _recordedSectionDecoder;
        }
    }

    @Test
    public void rapidEchoesOnReceivingLinkCoalesceToOneDelayedFlow() throws Exception
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final Session_1_0 session = createEchoSession(clock, scheduler);
        final CountingStandardReceivingLinkEndpoint endpoint = createReceivingLinkEndpoint(session, "echo-link-1");

        attachReceivingLink(endpoint, "echo-link-1", UnsignedInteger.ZERO);
        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);

        final Flow flow = new Flow();
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setEcho(Boolean.TRUE);

        endpoint.receiveFlow(flow);
        endpoint.receiveFlow(flow);

        assertEquals(1, endpoint.getSendFlowCount());
        assertSentFlowHandles(session, UnsignedInteger.ZERO);
        assertEquals(1, scheduler.getScheduledTaskCount());

        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        scheduler.runNext();

        assertEquals(2, endpoint.getSendFlowCount());
        assertSentFlowHandles(session, UnsignedInteger.ZERO, UnsignedInteger.ZERO);
        assertEquals(0, scheduler.getScheduledTaskCount());
    }

    @Test
    public void echoRequestsOnDifferentReceivingLinksRemainIndependent() throws Exception
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final Session_1_0 session = createEchoSession(clock, scheduler);
        final CountingStandardReceivingLinkEndpoint firstEndpoint = createReceivingLinkEndpoint(session, "echo-link-1");
        final CountingStandardReceivingLinkEndpoint secondEndpoint = createReceivingLinkEndpoint(session, "echo-link-2");

        attachReceivingLink(firstEndpoint, "echo-link-1", UnsignedInteger.ZERO);
        attachReceivingLink(secondEndpoint, "echo-link-2", UnsignedInteger.ONE);
        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);

        final Flow flow = new Flow();
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setEcho(Boolean.TRUE);

        firstEndpoint.receiveFlow(flow);
        firstEndpoint.receiveFlow(flow);
        secondEndpoint.receiveFlow(flow);

        assertEquals(1, firstEndpoint.getSendFlowCount());
        assertEquals(1, secondEndpoint.getSendFlowCount());
        assertSentFlowHandles(session, UnsignedInteger.ZERO, UnsignedInteger.ONE);
        assertEquals(1, scheduler.getScheduledTaskCount());

        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        scheduler.runNext();

        assertEquals(2, firstEndpoint.getSendFlowCount());
        assertEquals(1, secondEndpoint.getSendFlowCount());
        assertSentFlowHandles(session, UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.ZERO);
        assertEquals(0, scheduler.getScheduledTaskCount());
    }

    @Test
    public void destroyCancelsPendingReceivingLinkEchoFlow() throws Exception
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final Session_1_0 session = createEchoSession(clock, scheduler);
        final CountingStandardReceivingLinkEndpoint endpoint = createReceivingLinkEndpoint(session, "echo-link-1");

        attachReceivingLink(endpoint, "echo-link-1", UnsignedInteger.ZERO);
        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);

        final Flow flow = new Flow();
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setEcho(Boolean.TRUE);

        endpoint.receiveFlow(flow);
        endpoint.receiveFlow(flow);
        endpoint.destroy();

        assertEquals(1, endpoint.getSendFlowCount());
        assertSentFlowHandles(session, UnsignedInteger.ZERO);
        assertEquals(0, scheduler.getScheduledTaskCount());
    }

    private Session_1_0 createEchoSession(final EchoFlowTestSupport.FakeClock clock,
                                          final EchoFlowTestSupport.FakeScheduler scheduler)
    {
        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        final AMQPDescribedTypeRegistry describedTypeRegistry = AMQPDescribedTypeRegistry.newInstance()
                .registerTransportLayer()
                .registerMessagingLayer()
                .registerTransactionLayer()
                .registerSecurityLayer()
                .registerExtensionSoleconnLayer();
        final TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        doReturn(describedTypeRegistry).when(connection).getDescribedTypeRegistry();
        doReturn(taskExecutor).when(connection).getChildExecutor();
        doReturn(taskExecutor).when(connection).getTaskExecutor();
        doReturn(BrokerModel.getInstance()).when(connection).getModel();
        doReturn(new Subject()).when(connection).getSubject();
        doReturn(mock(Broker.class)).when(connection).getBroker();
        doReturn(mock(NamedAddressSpace.class)).when(connection).getAddressSpace();
        doReturn(mock(EventLogger.class)).when(connection).getEventLogger();
        doReturn(0L).when(connection).getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT);
        doReturn(0).when(connection).getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE);
        doReturn(512).when(connection).getMaxFrameSize();

        final Begin begin = mock(Begin.class);
        doReturn(UnsignedInteger.ZERO).when(begin).getNextOutgoingId();
        return spy(new Session_1_0(connection, begin, 0, 0, 1000L, AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS,
                clock, scheduler, EchoFlowTestSupport.DIRECT_EXECUTOR));
    }

    private CountingStandardReceivingLinkEndpoint createReceivingLinkEndpoint(final Session_1_0 session,
                                                                             final String linkName)
    {
        final Link_1_0<Source, Target> link = mock(Link_1_0.class);
        doReturn(linkName).when(link).getName();
        return new CountingStandardReceivingLinkEndpoint(session, link);
    }

    private void attachReceivingLink(final StandardReceivingLinkEndpoint endpoint,
                                     final String linkName,
                                     final UnsignedInteger handle) throws Exception
    {
        endpoint.setLocalHandle(handle);
        endpoint.setLinkCredit(UnsignedInteger.ZERO);

        final Attach attach = new Attach();
        attach.setHandle(handle);
        attach.setIncompleteUnsettled(false);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        attach.setName(linkName);
        attach.setRole(Role.SENDER);
        attach.setSource(new Source());
        attach.setTarget(new Target());

        endpoint.receiveAttach(attach);
        endpoint.sendAttach();
    }

    private void assertSentFlowHandles(final Session_1_0 session,
                                       final UnsignedInteger... expectedHandles)
    {
        final ArgumentCaptor<Flow> flowCaptor = ArgumentCaptor.forClass(Flow.class);
        verify(session, times(expectedHandles.length)).sendFlow(flowCaptor.capture());
        final List<Flow> sentFlows = flowCaptor.getAllValues();

        assertEquals(expectedHandles.length, sentFlows.size());
        for (int i = 0; i < expectedHandles.length; i++)
        {
            assertEquals(expectedHandles[i], sentFlows.get(i).getHandle());
        }
    }

    private static final class CountingStandardReceivingLinkEndpoint extends StandardReceivingLinkEndpoint
    {
        private int _sendFlowCount;

        private CountingStandardReceivingLinkEndpoint(final Session_1_0 session,
                                                      final Link_1_0<Source, Target> link)
        {
            super(session, link);
        }

        @Override
        public void sendFlow()
        {
            _sendFlowCount++;
            super.sendFlow();
        }

        private int getSendFlowCount()
        {
            return _sendFlowCount;
        }
    }
}
