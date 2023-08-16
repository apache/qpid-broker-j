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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
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
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
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

        final Begin begin = mock(Begin.class);
        doReturn(new UnsignedInteger(0)).when(begin).getNextOutgoingId();
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
}
