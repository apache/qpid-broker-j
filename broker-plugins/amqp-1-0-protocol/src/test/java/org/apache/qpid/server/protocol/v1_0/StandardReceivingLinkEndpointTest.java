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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"unchecked"})
public class StandardReceivingLinkEndpointTest extends UnitTestBase
{
    @Test
    public void linkAddedAndRemoved()
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
}
