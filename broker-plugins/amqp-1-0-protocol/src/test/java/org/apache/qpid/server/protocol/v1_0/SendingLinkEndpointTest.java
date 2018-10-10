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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistry;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.test.utils.UnitTestBase;

public class SendingLinkEndpointTest extends UnitTestBase
{
    private static final String ADDRESS = "test";

    private SendingLinkEndpoint _sendingLinkEndpoint;

    @Before
    public void setUp() throws Exception
    {
        NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);

        final LinkImpl<Source, Target> link = mock(LinkImpl.class);
        when(link.getSource()).thenReturn(new Source());
        Target target = new Target();
        target.setAddress(ADDRESS);
        when(link.getTarget()).thenReturn(target);

        final AMQPConnection_1_0 connection = mock(AMQPConnection_1_0.class);
        when(connection.getAddressSpace()).thenReturn(addressSpace);
        when(connection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD)).thenReturn(1L);
        final Session_1_0 session = mock(Session_1_0.class);
        when(session.getConnection()).thenReturn(connection);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getOutgoingDeliveryRegistry()).thenReturn(mock(DeliveryRegistry.class));
        final SendingDestination destination = new StandardSendingDestination(mock(MessageSource.class));
        when(session.getSendingDestination(any(Link_1_0.class), any(Source.class))).thenReturn(destination);
        _sendingLinkEndpoint = new SendingLinkEndpoint(session, link);
    }

    @Test
    public void receiveFlow() throws Exception
    {
        receiveAttach(_sendingLinkEndpoint);

        _sendingLinkEndpoint.setDeliveryCount(new SequenceNumber(-1));

        Flow flow = new Flow();
        flow.setDeliveryCount(new SequenceNumber(-1).unsignedIntegerValue());
        flow.setLinkCredit(UnsignedInteger.ONE);

        _sendingLinkEndpoint.receiveFlow(flow);

        UnsignedInteger linkCredit = _sendingLinkEndpoint.getLinkCredit();
        assertThat(linkCredit, is(equalTo(UnsignedInteger.ONE)));
    }

    private void receiveAttach(final SendingLinkEndpoint sendingLinkEndpoint) throws Exception
    {
        Attach attach = new Attach();
        Source source = new Source();
        source.setDurable(TerminusDurability.NONE);
        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        attach.setSource(source);
        Target target = new Target();
        attach.setTarget(target);
        attach.setHandle(new UnsignedInteger(0));
        attach.setIncompleteUnsettled(false);
        attach.setName("test");
        attach.setRole(Role.RECEIVER);
        source.setAddress(ADDRESS);

        sendingLinkEndpoint.receiveAttach(attach);
    }
}
