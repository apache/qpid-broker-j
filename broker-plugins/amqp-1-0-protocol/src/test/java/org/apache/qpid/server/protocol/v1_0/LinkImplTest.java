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
 */
package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistry;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.transport.AMQPConnection;

@SuppressWarnings({"rawtypes", "unchecked"})
public class LinkImplTest
{
    private final static String REMOTE_CONTAINER_ID = "remote-container-id";
    private final static String LINK_NAME = "link-name";

    private LinkRegistry<Source, Target> _linkRegistry;

    @BeforeEach
    public void setUp()
    {
        _linkRegistry = mock(LinkRegistry.class);
    }

    @Test
    public void linkStealing_PublishToQueue() throws Exception
    {
        final Queue<?> queue = mock(Queue.class);
        final Session_1_0 session1 = createSession("principal1", queue);
        final Session_1_0 session2 = createSession("principal2", queue);

        final Attach attach1 = createAttach(Role.SENDER);
        final Attach attach2 = createAttach(Role.SENDER);

        final LinkImpl<Source, Target> link = new LinkImpl<>(REMOTE_CONTAINER_ID, LINK_NAME, Role.RECEIVER, _linkRegistry);

        final ListenableFuture<? extends LinkEndpoint<?, ?>> future = link.attach(session1, attach1);
        final LinkEndpoint<?, ?> linkEndpoint = future.get();
        assertTrue(linkEndpoint instanceof StandardReceivingLinkEndpoint);

        link.attach(session2, attach2);
        verify(queue, times(1)).authorise(eq(Operation.PERFORM_ACTION("publish")));
    }

    @Test
    public void linkStealing_ConsumeQueue() throws Exception
    {
        final Queue<?> queue = mock(Queue.class);
        final Session_1_0 session1 = createSession("principal1", queue);
        final Session_1_0 session2 = createSession("principal2", queue);

        final Attach attach1 = createAttach(Role.RECEIVER);
        final Attach attach2 = createAttach(Role.RECEIVER);

        final LinkImpl<Source, Target> link = new LinkImpl<>(REMOTE_CONTAINER_ID, LINK_NAME, Role.SENDER, _linkRegistry);

        final ListenableFuture<? extends LinkEndpoint<?, ?>> future = link.attach(session1, attach1);
        final LinkEndpoint<?, ?> linkEndpoint = future.get();
        assertTrue(linkEndpoint instanceof SendingLinkEndpoint);

        link.attach(session2, attach2);
        verify(queue, times(1)).authorise(eq(Operation.PERFORM_ACTION("consume")));
    }

    @Test
    public void linkStealing_PublishToExchange() throws Exception
    {
        final Exchange<?> exchange = mock(Exchange.class);
        final Queue<?> queue = mock(Queue.class);
        final Session_1_0 session1 = createSession("principal1", exchange, queue);
        final Session_1_0 session2 = createSession("principal2", exchange, queue);

        final Attach attach1 = createAttach(Role.SENDER);
        final Attach attach2 = createAttach(Role.SENDER);

        final LinkImpl<Source, Target> link = new LinkImpl<>(REMOTE_CONTAINER_ID, LINK_NAME, Role.RECEIVER, _linkRegistry);

        final ListenableFuture<? extends LinkEndpoint<?, ?>> future = link.attach(session1, attach1);
        final LinkEndpoint<?, ?> linkEndpoint = future.get();
        assertTrue(linkEndpoint instanceof StandardReceivingLinkEndpoint);

        link.attach(session2, attach2);
        verify(exchange, times(1)).authorise(eq(Operation.PERFORM_ACTION("publish")));
    }

    @Test
    public void linkStealing_ConsumeExchange() throws Exception
    {
        final Exchange<?> exchange = mock(Exchange.class);
        final Queue<?> queue = mock(Queue.class);
        final Session_1_0 session1 = createSession("principal1", exchange, queue);
        final Session_1_0 session2 = createSession("principal2", exchange, queue);

        final Attach attach1 = createAttach(Role.RECEIVER);
        final Attach attach2 = createAttach(Role.RECEIVER);

        final LinkImpl<Source, Target> link = new LinkImpl<>(REMOTE_CONTAINER_ID, LINK_NAME, Role.SENDER, _linkRegistry);

        final ListenableFuture<? extends LinkEndpoint<?, ?>> future = link.attach(session1, attach1);
        final LinkEndpoint<?, ?> linkEndpoint = future.get();
        assertTrue(linkEndpoint instanceof SendingLinkEndpoint);

        link.attach(session2, attach2);
        verify(queue, times(1)).authorise(eq(Operation.PERFORM_ACTION("consume")));
    }

    private Session_1_0 createSession(final String principal,
                                      final MessageDestination messageDestination) throws Exception
    {
        return createSession(principal, messageDestination, null);
    }

    private Session_1_0 createSession(final String principal,
                                      final MessageDestination messageDestination,
                                      final MessageDestination backup) throws Exception
    {
        final MessageStore messageStore = mock(MessageStore.class);

        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        when(addressSpace.getMessageStore()).thenReturn(messageStore);

        final SectionDecoderRegistry sectionDecoderRegistry = mock(SectionDecoderRegistry.class);

        final AMQPDescribedTypeRegistry amqpDescribedTypeRegistry = mock(AMQPDescribedTypeRegistry.class);
        when(amqpDescribedTypeRegistry.getSectionDecoderRegistry()).thenReturn(sectionDecoderRegistry);

        final AMQPConnection<?> amqpConnection = mock(AMQPConnection.class);
        when(amqpConnection.getContextValue(eq(Long.class), eq(Consumer.SUSPEND_NOTIFICATION_PERIOD)))
                .thenReturn(10_000L);

        final AMQPConnection_1_0<?> connection = mock(AMQPConnection_1_0.class);
        when(connection.getDescribedTypeRegistry()).thenReturn(amqpDescribedTypeRegistry);
        when(connection.getPrincipal()).thenReturn(principal);
        when(connection.getAddressSpace()).thenReturn(addressSpace);

        final DeliveryRegistry deliveryRegistry = mock(DeliveryRegistry.class);

        final ReceivingDestination receivingDestination = mock(ReceivingDestination.class);
        when(receivingDestination.getCapabilities()).thenReturn(new Symbol[] { });
        when(receivingDestination.getMessageDestination()).thenReturn(messageDestination);

        final SendingDestination sendingDestination = messageDestination instanceof Exchange
                ? mock(ExchangeSendingDestination.class)
                : mock(StandardSendingDestination.class);
        when(sendingDestination.getCapabilities()).thenReturn(new Symbol[] { });

        final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);

        if (messageDestination instanceof Exchange)
        {
            final Queue<?> queue = (Queue<?>) backup;
            when(queue.addConsumer(any(),any(), eq(Message_1_0.class), any(), any(), any()))
                    .thenReturn(consumer);

            when(sendingDestination.getMessageSource()).thenReturn(queue);
        }

        if (messageDestination instanceof Queue)
        {
            final Queue<?> queue = (Queue<?>) messageDestination;
            when(queue.addConsumer(any(),any(), eq(Message_1_0.class), any(), any(), any()))
                    .thenReturn(consumer);
            when(sendingDestination.getMessageSource()).thenReturn(queue);
        }

        final Session_1_0 session = mock(Session_1_0.class);
        doReturn(connection).when(session).getConnection();
        doReturn(amqpConnection).when(session).getAMQPConnection();
        when(session.getIncomingDeliveryRegistry()).thenReturn(deliveryRegistry);
        when(session.getOutgoingDeliveryRegistry()).thenReturn(deliveryRegistry);
        when(session.getReceivingDestination(any(LinkImpl.class), any(Target.class))).thenReturn(receivingDestination);
        when(session.getSendingDestination(any(LinkImpl.class), any(Source.class))).thenReturn(sendingDestination);

        return session;
    }

    private Attach createAttach(final Role role)
    {
        final Source source = mock(Source.class);
        when(source.getDistributionMode()).thenReturn(StdDistMode.COPY);

        final Target target = mock(Target.class);

        final Attach attach = mock(Attach.class);
        when(attach.getRole()).thenReturn(role);
        when(attach.getSource()).thenReturn(source);
        when(attach.getTarget()).thenReturn(target);
        when(attach.getInitialDeliveryCount()).thenReturn(UnsignedInteger.ZERO);

        return attach;
    }
}
