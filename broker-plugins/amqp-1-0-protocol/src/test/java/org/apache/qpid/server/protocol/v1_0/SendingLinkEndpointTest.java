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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistry;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class SendingLinkEndpointTest extends UnitTestBase
{
    private static final String ADDRESS = "test";
    private static final AMQPDescribedTypeRegistry DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer()
            .registerExtensionSoleconnLayer();

    private EchoFlowTestSupport.FakeClock _echoFlowClock;
    private EchoFlowTestSupport.FakeScheduler _echoFlowScheduler;
    private Session_1_0 _session;
    private CountingSendingLinkEndpoint _sendingLinkEndpoint;

    @BeforeEach
    void setUp() throws Exception
    {
        _echoFlowClock = new EchoFlowTestSupport.FakeClock();
        _echoFlowScheduler = new EchoFlowTestSupport.FakeScheduler();
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);

        final LinkImpl<Source, Target> link = mock(LinkImpl.class);
        when(link.getSource()).thenReturn(new Source());
        final Target target = new Target();
        target.setAddress(ADDRESS);
        when(link.getTarget()).thenReturn(target);

        final AMQPConnection_1_0 connection = mock(AMQPConnection_1_0.class);
        when(connection.getAddressSpace()).thenReturn(addressSpace);
        when(connection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD)).thenReturn(1L);
        when(connection.getDescribedTypeRegistry()).thenReturn(DESCRIBED_TYPE_REGISTRY);
        when(connection.getMaxFrameSize()).thenReturn(4096);
        _session = mock(Session_1_0.class);
        when(_session.getConnection()).thenReturn(connection);
        when(_session.getAMQPConnection()).thenReturn(connection);
        when(_session.getOutgoingDeliveryRegistry()).thenReturn(mock(DeliveryRegistry.class));
        when(_session.getEchoFlowCoalesceIntervalMs())
                .thenReturn((long) AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        when(_session.getEchoFlowClock()).thenReturn(_echoFlowClock);
        when(_session.getEchoFlowScheduler()).thenReturn(_echoFlowScheduler);
        when(_session.getEchoFlowExecutor()).thenReturn(EchoFlowTestSupport.DIRECT_EXECUTOR);
        final SendingDestination destination = new StandardSendingDestination(mock(MessageSource.class));
        when(_session.getSendingDestination(any(Link_1_0.class), any(Source.class))).thenReturn(destination);
        _sendingLinkEndpoint = new CountingSendingLinkEndpoint(_session, link);
    }

    @Test
    void receiveFlow() throws Exception
    {
        receiveAttach(_sendingLinkEndpoint);

        _sendingLinkEndpoint.setDeliveryCount(new SequenceNumber(-1));

        final Flow flow = new Flow();
        flow.setDeliveryCount(new SequenceNumber(-1).unsignedIntegerValue());
        flow.setLinkCredit(UnsignedInteger.ONE);

        _sendingLinkEndpoint.receiveFlow(flow);

        final UnsignedInteger linkCredit = _sendingLinkEndpoint.getLinkCredit();
        assertThat(linkCredit, is(equalTo(UnsignedInteger.ONE)));
    }

    @Test
    void rapidEchoesOnOneLinkCoalesceToOneDelayedFlow()
            throws Exception
    {
        attachLink(_sendingLinkEndpoint, UnsignedInteger.ZERO);
        _echoFlowClock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        clearInvocations(_session);

        final Flow flow = new Flow();
        flow.setLinkCredit(UnsignedInteger.ONE);
        flow.setEcho(Boolean.TRUE);

        _sendingLinkEndpoint.receiveFlow(flow);
        _sendingLinkEndpoint.receiveFlow(flow);

        assertThat(_sendingLinkEndpoint.getSendFlowCount(), is(equalTo(1)));
        assertSentFlowHandles(UnsignedInteger.ZERO);
        assertThat(_echoFlowScheduler.getScheduledTaskCount(), is(equalTo(1)));

        _echoFlowClock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        _echoFlowScheduler.runNext();

        assertThat(_sendingLinkEndpoint.getSendFlowCount(), is(equalTo(2)));
        assertSentFlowHandles(UnsignedInteger.ZERO, UnsignedInteger.ZERO);
        assertThat(_echoFlowScheduler.getScheduledTaskCount(), is(equalTo(0)));
    }

    @Test
    void echoRequestsOnDifferentLinksRemainIndependent()
            throws Exception
    {
        attachLink(_sendingLinkEndpoint, UnsignedInteger.ZERO);
        final CountingSendingLinkEndpoint secondEndpoint = createSendingLinkEndpoint();
        attachLink(secondEndpoint, UnsignedInteger.ONE);
        _echoFlowClock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        clearInvocations(_session);

        final Flow flow = new Flow();
        flow.setLinkCredit(UnsignedInteger.ONE);
        flow.setEcho(Boolean.TRUE);

        _sendingLinkEndpoint.receiveFlow(flow);
        _sendingLinkEndpoint.receiveFlow(flow);
        secondEndpoint.receiveFlow(flow);

        assertThat(_sendingLinkEndpoint.getSendFlowCount(), is(equalTo(1)));
        assertThat(secondEndpoint.getSendFlowCount(), is(equalTo(1)));
        assertSentFlowHandles(UnsignedInteger.ZERO, UnsignedInteger.ONE);
        assertThat(_echoFlowScheduler.getScheduledTaskCount(), is(equalTo(1)));

        _echoFlowClock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        _echoFlowScheduler.runNext();

        assertThat(_sendingLinkEndpoint.getSendFlowCount(), is(equalTo(2)));
        assertThat(secondEndpoint.getSendFlowCount(), is(equalTo(1)));
        assertSentFlowHandles(UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.ZERO);
        assertThat(_echoFlowScheduler.getScheduledTaskCount(), is(equalTo(0)));
    }

    @Test
    void drainResponseSendsImmediatelyAndClearsPendingEcho()
            throws Exception
    {
        attachLink(_sendingLinkEndpoint, UnsignedInteger.ZERO);
        _echoFlowClock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        clearInvocations(_session);

        final Flow echoFlow = new Flow();
        echoFlow.setLinkCredit(UnsignedInteger.ONE);
        echoFlow.setEcho(Boolean.TRUE);

        _sendingLinkEndpoint.receiveFlow(echoFlow);
        _sendingLinkEndpoint.receiveFlow(echoFlow);

        final Flow drainFlow = new Flow();
        drainFlow.setLinkCredit(UnsignedInteger.ZERO);
        drainFlow.setDrain(Boolean.TRUE);

        _sendingLinkEndpoint.receiveFlow(drainFlow);

        assertThat(_sendingLinkEndpoint.getSendFlowCount(), is(equalTo(2)));
        assertSentFlowHandles(UnsignedInteger.ZERO, UnsignedInteger.ZERO);
        assertThat(_echoFlowScheduler.getScheduledTaskCount(), is(equalTo(0)));
    }

    @Test
    void detachCancelsPendingEchoFlow()
            throws Exception
    {
        attachLink(_sendingLinkEndpoint, UnsignedInteger.ZERO);
        _echoFlowClock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        clearInvocations(_session);

        final Flow flow = new Flow();
        flow.setLinkCredit(UnsignedInteger.ONE);
        flow.setEcho(Boolean.TRUE);

        _sendingLinkEndpoint.receiveFlow(flow);
        _sendingLinkEndpoint.receiveFlow(flow);
        _sendingLinkEndpoint.detach();

        assertThat(_echoFlowScheduler.getScheduledTaskCount(), is(equalTo(0)));
        assertThat(_sendingLinkEndpoint.getSendFlowCount(), is(equalTo(1)));
        assertSentFlowHandles(UnsignedInteger.ZERO);
        verify(_session, times(1)).sendDetach(any(Detach.class));
    }

    private void receiveAttach(final SendingLinkEndpoint sendingLinkEndpoint,
                               final UnsignedInteger handle) throws Exception
    {
        final Attach attach = new Attach();
        final Source source = new Source();
        source.setDurable(TerminusDurability.NONE);
        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        attach.setSource(source);
        final Target target = new Target();
        attach.setTarget(target);
        attach.setHandle(handle);
        attach.setIncompleteUnsettled(false);
        attach.setName("test");
        attach.setRole(Role.RECEIVER);
        source.setAddress(ADDRESS);

        sendingLinkEndpoint.receiveAttach(attach);
    }

    private void receiveAttach(final SendingLinkEndpoint sendingLinkEndpoint) throws Exception
    {
        receiveAttach(sendingLinkEndpoint, UnsignedInteger.ZERO);
    }

    private void attachLink(final SendingLinkEndpoint sendingLinkEndpoint,
                            final UnsignedInteger handle) throws Exception
    {
        sendingLinkEndpoint.setLocalHandle(handle);
        receiveAttach(sendingLinkEndpoint, handle);
        sendingLinkEndpoint.sendAttach();
    }

    private CountingSendingLinkEndpoint createSendingLinkEndpoint()
    {
        final LinkImpl<Source, Target> link = mock(LinkImpl.class);
        when(link.getSource()).thenReturn(new Source());
        final Target target = new Target();
        target.setAddress(ADDRESS);
        when(link.getTarget()).thenReturn(target);
        return new CountingSendingLinkEndpoint(_session, link);
    }

    private void assertSentFlowHandles(final UnsignedInteger... expectedHandles)
    {
        final ArgumentCaptor<Flow> flowCaptor = ArgumentCaptor.forClass(Flow.class);
        verify(_session, times(expectedHandles.length)).sendFlow(flowCaptor.capture());
        final List<Flow> sentFlows = flowCaptor.getAllValues();

        assertThat(sentFlows.size(), is(equalTo(expectedHandles.length)));
        for (int i = 0; i < expectedHandles.length; i++)
        {
            assertThat(sentFlows.get(i).getHandle(), is(equalTo(expectedHandles[i])));
        }
    }

    private static final class CountingSendingLinkEndpoint extends SendingLinkEndpoint
    {
        private int _sendFlowCount;

        private CountingSendingLinkEndpoint(final Session_1_0 session,
                                            final LinkImpl<Source, Target> link)
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
