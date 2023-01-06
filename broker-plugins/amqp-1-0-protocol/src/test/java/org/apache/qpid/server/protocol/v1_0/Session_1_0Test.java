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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class Session_1_0Test extends UnitTestBase
{
    private static final AMQPDescribedTypeRegistry DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                                      .registerTransportLayer()
                                                                                                      .registerMessagingLayer()
                                                                                                      .registerTransactionLayer()
                                                                                                      .registerSecurityLayer()
                                                                                                      .registerExtensionSoleconnLayer();

    private static final String TOPIC_NAME = "testTopic";
    private static final String QUEUE_NAME = "testQueue";
    private static final Symbol TOPIC_CAPABILITY = Symbol.getSymbol("topic");
    private static final Symbol QUEUE_CAPABILITY = Symbol.getSymbol("queue");
    private static final Symbol JMS_SELECTOR_FILTER = Symbol.getSymbol("jms-selector");
    private AMQPConnection_1_0 _connection;
    private VirtualHost<?> _virtualHost;
    private Session_1_0 _session;
    private int _handle;
    private CurrentThreadTaskExecutor _taskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        Map<String, Object> virtualHostAttributes = new HashMap<>();
        virtualHostAttributes.put(QueueManagingVirtualHost.NAME, "testVH");
        virtualHostAttributes.put(QueueManagingVirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        _virtualHost = BrokerTestHelper.createVirtualHost(virtualHostAttributes, this);
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        _connection = createAmqpConnection_1_0("testContainerId");
        this._session = createSession_1_0(_connection, 0);
    }

    @AfterEach
    public void tearDown()
    {
        _taskExecutor.stop();
    }

    @Test
    public void testReceiveAttachTopicNonDurableNoContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(false, linkName, address, true);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
    }

    @Test
    public void testReceiveAttachTopicNonDurableWithContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(false, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
    }

    @Test
    public void testReceiveAttachTopicDurableNoContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(true, linkName, address, true);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);
    }

    @Test
    public void testReceiveAttachTopicDurableWithContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(true, linkName+ "|1", address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        AMQPConnection_1_0 secondConnection = createAmqpConnection_1_0("testContainerId2");
        Session_1_0 secondSession = createSession_1_0(secondConnection, 0);
        Attach attach2 = createTopicAttach(true, linkName + "|2", address, false);
        secondSession.receiveAttach(attach2);

        assertAttachSent(secondConnection, secondSession, attach2);
        Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(2, (long) queues.size(),
                "Unexpected number of queues after second subscription with the same subscription name but different " +
                "container id ");

    }

    @Test
    public void testReceiveAttachSharedTopicNonDurableNoContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createSharedTopicAttach(false, linkName, address, true);
        Attach attach2 = createSharedTopicAttach(false, linkName, address, true);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        AMQPConnection_1_0 secondConnection = createAmqpConnection_1_0();
        Session_1_0 secondSession = createSession_1_0(secondConnection, 0);

        secondSession.receiveAttach(attach2);

        assertAttachSent(secondConnection, secondSession, attach2);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after attach");
        Queue<?> queue = queues.iterator().next();

        Collection<QueueConsumer<?,?>> consumers = queue.getConsumers();
        assertEquals(2, (long) consumers.size(), "Unexpected number of consumers");
    }

    @Test
    public void testReceiveAttachSharedTopicNonDurableWithContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createSharedTopicAttach(false, linkName, address, false);
        Attach attach2 = createSharedTopicAttach(false, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        AMQPConnection_1_0 secondConnection = createAmqpConnection_1_0("testContainerId2");
        Session_1_0 secondSession = createSession_1_0(secondConnection, 0);

        secondSession.receiveAttach(attach2);

        assertAttachSent(secondConnection, secondSession, attach2);

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(2, (long) queues.size(), "Unexpected number of queues after attach");

        for (Queue<?> queue : queues)
        {
            Collection<QueueConsumer<?,?>> consumers = queue.getConsumers();
            assertEquals(1, (long) consumers.size(),
                    "Unexpected number of consumers on queue " + queue.getName());
        }
    }

    @Test
    public void testSeparateSubscriptionNameSpaces()
    {
        AMQPConnection_1_0 secondConnection = createAmqpConnection_1_0();
        Session_1_0 secondSession = createSession_1_0(secondConnection, 0);

        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;

        Attach durableSharedWithContainerId = createSharedTopicAttach(true, linkName + "|1", address, false);
        _session.receiveAttach(durableSharedWithContainerId);
        assertAttachSent(_connection, _session, durableSharedWithContainerId, 0);

        Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        Attach durableNonSharedWithContainerId = createTopicAttach(true, linkName, address, false);
        _session.receiveAttach(durableNonSharedWithContainerId);
        assertAttachFailed(_connection, _session, durableNonSharedWithContainerId, 1);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        Attach nonDurableSharedWithContainerId = createSharedTopicAttach(false, linkName + "|3", address, false);
        _session.receiveAttach(nonDurableSharedWithContainerId);
        assertAttachSent(_connection, _session, nonDurableSharedWithContainerId, 3);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(2, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        Attach durableSharedWithoutContainerId = createSharedTopicAttach(true, linkName + "|4", address, true);
        secondSession.receiveAttach(durableSharedWithoutContainerId);
        assertAttachSent(secondConnection, secondSession, durableSharedWithoutContainerId, 0);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(3, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        Attach nonDurableSharedWithoutContainerId = createSharedTopicAttach(false, linkName + "|5", address, true);
        secondSession.receiveAttach(nonDurableSharedWithoutContainerId);
        assertAttachSent(secondConnection, secondSession, nonDurableSharedWithoutContainerId, 1);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(4, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        Attach nonDurableNonSharedWithoutContainerId = createTopicAttach(false, linkName + "|6", address, true);
        secondSession.receiveAttach(nonDurableNonSharedWithoutContainerId);
        assertAttachSent(secondConnection, secondSession, nonDurableNonSharedWithoutContainerId, 2);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(5, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        Attach nonDurableNonSharedWithContainerId = createTopicAttach(false, linkName + "|6", address, false);
        _session.receiveAttach(nonDurableNonSharedWithContainerId);
        assertAttachSent(_connection, _session, nonDurableNonSharedWithContainerId, 4);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(6, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");
    }

    @Test
    public void testReceiveAttachForInvalidUnsubscribe()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;

        Attach unsubscribeAttach = createTopicAttach(true, linkName, address, false);
        unsubscribeAttach.setSource(null);

        _session.receiveAttach(unsubscribeAttach);
        assertAttachFailed(_connection, _session, unsubscribeAttach);

        Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(0, (long) queues.size(), "Unexpected number of queues after unsubscribe");
    }

    @Test
    public void testNullSourceLookup()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(true, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), false);

        Attach nullSourceAttach = createTopicAttach(true, linkName, address, false);
        nullSourceAttach.setSource(null);

        _session.receiveAttach(nullSourceAttach);
        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection, times(3)).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        Attach sentAttach = (Attach) frameCapture.getAllValues().get(2);

        assertEquals(nullSourceAttach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
        assertNotNull(sentAttach.getSource(), "Unexpected source");
        Source source = (Source)sentAttach.getSource();
        assertEquals(address, source.getAddress(), "Unexpected address");
        assertTrue(Arrays.asList(source.getCapabilities()).contains(Symbol.valueOf("topic")),
                "Unexpected source capabilities");


        Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after unsubscribe");
    }

    @Test
    public void testReceiveDetachClosed()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(true, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), true);

        Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(0, (long) queues.size(), "Unexpected number of queues after unsubscribe");
    }

    @Test
    public void testReceiveAttachToExistingQueue()
    {
        final String linkName = "testLink";
        Attach attach = createQueueAttach(false, linkName, QUEUE_NAME);

        Queue<?> queue = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, QUEUE_NAME));
        Exchange<?> exchange = _virtualHost.getChildByName(Exchange.class, "amq.direct");
        exchange.bind(QUEUE_NAME, QUEUE_NAME, Collections.emptyMap(), false);

        _session.receiveAttach(attach);

        assertAttachActions(queue, attach);
    }

    @Test
    public void testReceiveAttachToNonExistingQueue()
    {
        final String linkName = "testLink";
        Attach attach = createQueueAttach(false, linkName, QUEUE_NAME);
        _session.receiveAttach(attach);
        assertAttachFailed(_connection, _session, attach);
    }

    @Test
    public void testReceiveAttachRebindingQueueNoActiveLinks()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createSharedTopicAttach(true, linkName, address, true);
        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), false);

        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection, times(2)).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        final boolean condition = frameCapture.getAllValues().get(1) instanceof Detach;
        assertTrue(condition);

        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        String topicName2 = TOPIC_NAME + "2";
        final String address2 = "amq.direct/" + topicName2;
        Attach attach2 = createSharedTopicAttach(true, linkName + "|2", address2, true);

        _session.receiveAttach(attach2);
        assertAttachSent(_connection, _session, attach2, 2);

        assertQueues(topicName2, LifetimePolicy.PERMANENT);
    }

    @Test
    public void testReceiveReattachRebindingQueueNoActiveLinks()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createSharedTopicAttach(true, linkName, address, true);
        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), false);

        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection, times(2)).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        final boolean condition = frameCapture.getAllValues().get(1) instanceof Detach;
        assertTrue(condition);

        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        String topicName2 = TOPIC_NAME + "2";
        final String address2 = "amq.direct/" + topicName2;
        Attach attach2 = createSharedTopicAttach(true, linkName, address2, true);

        _session.receiveAttach(attach2);
        assertAttachSent(_connection, _session, attach2, 2);

        assertQueues(topicName2, LifetimePolicy.PERMANENT);
    }

    @Test
    public void testReceiveAttachTopicNonDurableNoContainerWithInvalidSelector()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        Attach attach = createTopicAttach(false, linkName, address, true);
        setSelector(attach, "invalid selector");

        _session.receiveAttach(attach);

        assertAttachFailed(_connection, _session, attach);
        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals((long) 0, (long) queues.size(), "Unexpected number of queues after attach");
    }

    @Test
    public void testReceiveAttachTopicNonDurableNoContainerWithValidSelector()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final String selectorExpression = "test='test'";
        Attach attach = createTopicAttach(false, linkName, address, true);
        setSelector(attach, selectorExpression);

        _session.receiveAttach(attach);

        Attach sentAttach = captureAttach(_connection, _session, 0);

        assertEquals(attach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
        assertFilter(sentAttach, selectorExpression);

        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        Binding binding = findBinding("amq.direct", TOPIC_NAME);
        assertNotNull(binding, "Binding is not found");
        Map<String,Object> arguments = binding.getArguments();
        assertNotNull(arguments, "Unexpected arguments");
        assertEquals(selectorExpression, arguments.get(AMQPFilterTypes.JMS_SELECTOR.toString()),
                "Unexpected filter on binding");
    }

    @Test
    public void testLinkStealing()
    {
        _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, QUEUE_NAME));
        Attach attach = createQueueAttach(true, getTestName(), QUEUE_NAME);

        AMQPConnection_1_0 connection1 = _connection;
        Session_1_0 session1 = _session;
        session1.receiveAttach(attach);

        Link_1_0<?,?> link = _virtualHost.getSendingLink(connection1.getRemoteContainerId(), attach.getName());
        assertNotNull(link, "Link is not created");
        assertAttachSent(connection1, session1, attach);

        AMQPConnection_1_0 connection2 = createAmqpConnection_1_0(connection1.getRemoteContainerId());
        Session_1_0 session2 = createSession_1_0(connection2, 0);
        session2.receiveAttach(attach);

        assertDetachSent(connection1, session1, LinkError.STOLEN, 1);
        assertAttachSent(connection2, session2, attach);
    }

    @Test
    public void testAttachSourceDynamicWithLifeTimePolicyDeleteOnClose()
    {
        final Attach attach = createReceiverAttach(getTestName());
        final Source source = createDynamicSource(new DeleteOnClose());
        attach.setSource(source);

        _session.receiveAttach(attach);

        assertQueueDurability(getDynamicNodeAddressFromAttachResponse(), false);
    }

    @Test
    public void testAttachSourceDynamicWithLifeTimePolicyDeleteOnCloseAndExpiryPolicyNever()
    {
        final Attach attach = createReceiverAttach(getTestName());
        final Source source = createDynamicSource(new DeleteOnClose());
        source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
        attach.setSource(source);

        _session.receiveAttach(attach);

        assertQueueDurability(getDynamicNodeAddressFromAttachResponse(), true);
    }

    private Source createDynamicSource(final DeleteOnClose lifetimePolicy)
    {
        final Source source = new Source();
        source.setDynamic(true);
        source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, lifetimePolicy));
        return source;
    }

    private String getDynamicNodeAddressFromAttachResponse()
    {
        final Attach sentAttach = captureAttach(_connection, _session, 0);
        assertTrue(sentAttach.getSource() instanceof Source);
        return ((Source) (sentAttach.getSource())).getAddress();
    }

    public void assertQueueDurability(final String queueName, final boolean expectedDurability)
    {
        final Queue queue = _virtualHost.getChildByName(Queue.class, queueName);
        assertNotNull(queue, "Queue not found");
        assertEquals(queue.isDurable(), expectedDurability, "Unexpected durability");
    }

    private void assertFilter(final Attach sentAttach, final String selectorExpression)
    {
        Source source = (Source)sentAttach.getSource();
        Map<Symbol, Filter> filter = source.getFilter();
        assertNotNull(filter, "Filter is not set in response");
        assertEquals(1, filter.size(), "Unexpected filter size");
        assertTrue(filter.containsKey(JMS_SELECTOR_FILTER), "Selector is not found");
        Filter jmsSelectorFilter = filter.get(JMS_SELECTOR_FILTER);
        final boolean condition = jmsSelectorFilter instanceof JMSSelectorFilter;
        assertTrue(condition, "Unexpected selector filter");
        assertEquals(selectorExpression, ((JMSSelectorFilter) jmsSelectorFilter).getValue(), "Unexpected selector");
    }

    private Binding findBinding(final String exchangeName, final String bindingName)
    {
        Exchange exchange = _virtualHost.findConfiguredObject(Exchange.class, exchangeName);
        Collection<Binding> bindings = exchange.getBindings();
        Binding binding = null;
        for (Binding b: bindings)
        {
            if (bindingName.equals(b.getName()))
            {
                binding = b;
                break;
            }
        }
        return binding;
    }

    private void setSelector(final Attach attach, final String selectorExpression)
    {
        JMSSelectorFilter selector = new JMSSelectorFilter(selectorExpression);
        final Map<Symbol, Filter>
                filter = Collections.singletonMap(Symbol.getSymbol("jms-selector"), selector);
        ((Source)attach.getSource()).setFilter(filter);
    }

    private void assertAttachActions(final Queue<?> queue, final Attach receivedAttach)
    {
        Collection<QueueConsumer<?,?>> consumers = queue.getConsumers();
        assertEquals(1, (long) consumers.size(), "Unexpected consumers size");

        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        Attach sentAttach = (Attach) frameCapture.getValue();

        assertEquals(receivedAttach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");

        Source receivedSource = (Source) receivedAttach.getSource();
        Source sentSource = (Source) sentAttach.getSource();
        assertEquals(receivedSource.getAddress(), sentSource.getAddress(), "Unexpected source address");
        assertArrayEquals(receivedSource.getCapabilities(), sentSource.getCapabilities(),
                "Unexpected source capabilities");
        assertEquals(receivedSource.getDurable(), sentSource.getDurable(), "Unexpected source durability");
        assertEquals(receivedSource.getExpiryPolicy(), sentSource.getExpiryPolicy(),
                "Unexpected source expiry policy");
        assertEquals(receivedSource.getDynamic(), sentSource.getDynamic(), "Unexpected source dynamic flag");

        Target receivedTarget = (Target) receivedAttach.getTarget();
        Target sentTarget = (Target) sentAttach.getTarget();
        assertEquals(receivedTarget.getAddress(), sentTarget.getAddress(), "Unexpected target address");
        assertArrayEquals(receivedTarget.getCapabilities(), sentTarget.getCapabilities(),
                "Unexpected target capabilities");
        assertEquals(receivedTarget.getDurable(), sentTarget.getDurable(), "Unexpected target durability");
        assertEquals(receivedTarget.getExpiryPolicy(), sentTarget.getExpiryPolicy(),
                "Unexpected target expiry policy");
        assertEquals(receivedTarget.getDynamic(), sentTarget.getDynamic(), "Unexpected target dynamic flag");

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after attach");
    }

    private void assertDetachSent(final AMQPConnection_1_0 connection,
                                  final Session_1_0 session,
                                  final ErrorCondition expectedError,
                                  final int invocationOffset)
    {
        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(connection, times(invocationOffset + 1)).sendFrame(eq(session.getChannelId()), frameCapture.capture());
        List<FrameBody> sentFrames = frameCapture.getAllValues();

        final boolean condition = sentFrames.get(invocationOffset) instanceof Detach;
        assertTrue(condition, "unexpected Frame sent");
        Detach sentDetach = (Detach) sentFrames.get(invocationOffset);
        assertTrue(sentDetach.getClosed(), "Unexpected closed state");
        assertEquals(expectedError, sentDetach.getError().getCondition(), "Closed with unexpected error condition");
    }

    private void assertAttachSent(final AMQPConnection_1_0 connection,
                                  final Session_1_0 session,
                                  final Attach receivedAttach)
    {
        assertAttachSent(connection, session, receivedAttach, 0);
    }

    private void assertAttachSent(final AMQPConnection_1_0 connection,
                                  final Session_1_0 session,
                                  final Attach receivedAttach,
                                  final int invocationOffset)
    {
        Attach sentAttach = captureAttach(connection, session, invocationOffset);

        assertEquals(receivedAttach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
    }

    private Attach captureAttach(final AMQPConnection_1_0 connection,
                                 final Session_1_0 session,
                                 final int invocationOffset)
    {
        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(connection, times(invocationOffset + 1)).sendFrame(eq(session.getChannelId()),
                                                                  frameCapture.capture());
        return (Attach) frameCapture.getAllValues().get(invocationOffset);
    }

    private void assertQueues(final String publishingLinkName, final LifetimePolicy expectedLifetimePolicy)
    {
        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after attach");
        Queue<?> queue = queues.iterator().next();
        assertEquals(expectedLifetimePolicy, queue.getLifetimePolicy(), "Unexpected queue durability");

        Collection<PublishingLink> queuePublishingLinks = queue.getPublishingLinks();
        assertEquals(1, (long) queuePublishingLinks.size(), "Unexpected number of publishing links");
        assertEquals(publishingLinkName, queuePublishingLinks.iterator().next().getName(), "Unexpected link name");

        Exchange<?> exchange = _virtualHost.getChildByName(Exchange.class, "amq.direct");
        assertTrue(exchange.hasBinding(publishingLinkName, queue), "Binding should exist");
    }

    private void assertAttachFailed(final AMQPConnection_1_0 connection, final Session_1_0 session, final Attach attach, int invocationOffset)
    {
        ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(connection, times(invocationOffset + 2)).sendFrame(eq(session.getChannelId()), frameCapture.capture());
        List<FrameBody> sentFrames = frameCapture.getAllValues();

        final boolean condition1 = sentFrames.get(invocationOffset) instanceof Attach;
        assertTrue(condition1, "unexpected Frame sent");
        Attach sentAttach = (Attach) sentFrames.get(invocationOffset);
        assertEquals(attach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
        assertNull(sentAttach.getSource(), "Unexpected source");

        final boolean condition = sentFrames.get(invocationOffset + 1) instanceof Detach;
        assertTrue(condition, "unexpected Frame sent");
        Detach sentDetach = (Detach) sentFrames.get(invocationOffset + 1);
        assertTrue(sentDetach.getClosed(), "Unexpected closed state");
    }

    private void assertAttachFailed(final AMQPConnection_1_0 connection, final Session_1_0 session, final Attach attach)
    {
        assertAttachFailed(connection, session, attach, 0);
    }

    private Attach createSharedTopicAttach(final boolean durable,
                                           final String linkName,
                                           final String address,
                                           final boolean isGlobal)
    {
        return createAttach(durable, linkName, address, TOPIC_CAPABILITY, isGlobal, true);
    }

    private Attach createTopicAttach(final boolean durable,
                                     final String linkName,
                                     final String address,
                                     final boolean isGlobal)
    {
        return createAttach(durable, linkName, address, TOPIC_CAPABILITY, isGlobal, false);
    }

    private Attach createQueueAttach(final boolean durable,
                                     final String linkName,
                                     final String address)
    {
        return createAttach(durable, linkName, address, QUEUE_CAPABILITY, false, false);
    }

    private Attach createAttach(final boolean durable,
                                final String linkName,
                                final String address,
                                final Symbol destinationTypeCapability,
                                final boolean isGlobal,
                                final boolean isShared)
    {
        Attach attach = createReceiverAttach(linkName);
        Source source = new Source();

        List<Symbol> capabilities = new ArrayList<>();
        if (isGlobal)
        {
            capabilities.add(Symbol.getSymbol("global"));
        }
        if (isShared)
        {
            capabilities.add(Symbol.getSymbol("shared"));
        }
        capabilities.add(destinationTypeCapability);


        source.setCapabilities(capabilities.toArray(new Symbol[capabilities.size()]));
        if (durable)
        {
            source.setDurable(TerminusDurability.CONFIGURATION);
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
        }
        else
        {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }
        source.setAddress(address);
        attach.setSource(source);
        return attach;
    }

    private Attach createReceiverAttach(String linkName)
    {
        final Attach attach = new Attach();
        Target target = new Target();
        attach.setTarget(target);
        attach.setHandle(new UnsignedInteger(_handle++));
        attach.setIncompleteUnsettled(false);
        attach.setName(linkName);
        attach.setRole(Role.RECEIVER);
        return attach;
    }

    private AMQPConnection_1_0 createAmqpConnection_1_0()
    {
        return createAmqpConnection_1_0(null);
    }

    private AMQPConnection_1_0 createAmqpConnection_1_0(String containerId)
    {
        AMQPConnection_1_0 connection = BrokerTestHelper.mockAsSystemPrincipalSource(AMQPConnection_1_0.class);
        Subject subject =
                new Subject(true, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        when(connection.getSubject()).thenReturn(subject);
        when(connection.getAddressSpace()).thenReturn(_virtualHost);
        when(connection.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(connection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD)).thenReturn(1L);
        when(connection.getChildExecutor()).thenReturn(_taskExecutor);
        when(connection.getTaskExecutor()).thenReturn(_taskExecutor);
        when(connection.getModel()).thenReturn(BrokerModel.getInstance());
        when(connection.getContextValue(Long.class, Session.PRODUCER_AUTH_CACHE_TIMEOUT)).thenReturn(Session.PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT);
        when(connection.getContextValue(Integer.class, Session.PRODUCER_AUTH_CACHE_SIZE)).thenReturn(Session.PRODUCER_AUTH_CACHE_SIZE_DEFAULT);
        when(connection.getContextValue(Long.class, Connection.MAX_UNCOMMITTED_IN_MEMORY_SIZE)).thenReturn(Connection.DEFAULT_MAX_UNCOMMITTED_IN_MEMORY_SIZE);
        when(connection.getDescribedTypeRegistry()).thenReturn(DESCRIBED_TYPE_REGISTRY);
        when(connection.getMaxFrameSize()).thenReturn(512);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(connection.doOnIOThreadAsync(runnableCaptor.capture())).thenAnswer((Answer<ListenableFuture<Void>>) invocation ->
        {
            runnableCaptor.getValue().run();
            return Futures.immediateFuture(null);
        });
        AggregateTicker mockTicker = mock(AggregateTicker.class);
        when(connection.getAggregateTicker()).thenReturn(mockTicker);
        if (containerId != null)
        {
            when(connection.getRemoteContainerId()).thenReturn(containerId);
        }
        else
        {
            final String randomContainerId = UUID.randomUUID().toString();
            when(connection.getRemoteContainerId()).thenReturn(randomContainerId);
        }
        return connection;
    }

    private Session_1_0 createSession_1_0(final AMQPConnection_1_0 connection, int channelId)
    {
        Begin begin = mock(Begin.class);
        when(begin.getNextOutgoingId()).thenReturn(new UnsignedInteger(channelId));
        return new Session_1_0(connection, begin, channelId, channelId, 2048);
    }

    private void sendDetach(final Session_1_0 session,
                            final UnsignedInteger handle,
                            final boolean closed)
    {
        final Detach detach = new Detach();
        detach.setHandle(handle);
        detach.setClosed(closed);
        session.receiveDetach(detach);
    }
}
