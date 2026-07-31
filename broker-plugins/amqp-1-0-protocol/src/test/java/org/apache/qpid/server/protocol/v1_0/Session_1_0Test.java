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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistry;
import org.apache.qpid.server.protocol.v1_0.delivery.UnsettledDelivery;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Terminus;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class Session_1_0Test extends UnitTestBase
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

    private AMQPConnection_1_0<?> _connection;
    private VirtualHost<?> _virtualHost;
    private Session_1_0 _session;
    private int _handle;
    private TaskExecutor _taskExecutor;

    @BeforeEach
    void setUp()
    {
        final Map<String, Object> virtualHostAttributes = Map.of(QueueManagingVirtualHost.NAME, "testVH",
                QueueManagingVirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        _virtualHost = BrokerTestHelper.createVirtualHost(virtualHostAttributes, this);
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _connection = createAmqpConnection_1_0("testContainerId");
        this._session = createSession_1_0(_connection, 0);
    }

    @AfterEach
    void tearDown()
    {
        _taskExecutor.stop();
    }

    @Test
    void receiveAttachTopicNonDurableNoContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(false, linkName, address, true);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
    }

    @Test
    void createDynamicDestinationUsesSystemSubject() throws Exception
    {
        final AMQPConnection_1_0<?> connection = createAmqpConnection_1_0();
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class, withSettings().extraInterfaces(ConfiguredObject.class));
        when(((ConfiguredObject) addressSpace).newToken(any())).thenReturn(mock(SecurityToken.class));

        final AtomicReference<Subject> capturedSubject = new AtomicReference<>();
        when(addressSpace.createMessageDestination(any(), any())).thenAnswer(invocation ->
        {
            capturedSubject.set(SubjectExecutionContext.currentSubject());
            return mock(MessageDestination.class);
        });
        when(connection.getAddressSpace()).thenReturn(addressSpace);

        final Session_1_0 session = createSession_1_0(connection, 0);
        final Link_1_0 link = mock(Link_1_0.class);
        when(link.getRole()).thenReturn(Role.SENDER);
        when(link.getRemoteContainerId()).thenReturn(getTestName());
        when(link.getName()).thenReturn("link");

        final Target terminus = new Target();
        final Method createDynamicDestination =
                Session_1_0.class.getDeclaredMethod("createDynamicDestination", Link_1_0.class, Terminus.class);
        createDynamicDestination.setAccessible(true);
        createDynamicDestination.invoke(session, link, terminus);

        final Subject subject = capturedSubject.get();
        assertNotNull(subject, "Subject not captured");
        final Principal systemPrincipal =
                ((BrokerTestHelper.TestableSystemPrincipalSource) connection).getSystemPrincipal();
        assertTrue(subject.getPrincipals().contains(systemPrincipal), "System principal missing");
    }

    @Test
    void constructorLogsCreateWithinSessionSubject()
    {
        final AMQPConnection_1_0<?> connection = createAmqpConnection_1_0();
        final EventLogger eventLogger = connection.getEventLogger();
        final AtomicReference<Subject> capturedSubject = new AtomicReference<>();

        doAnswer(invocation ->
        {
            capturedSubject.set(SubjectExecutionContext.currentSubject());
            return null;
        }).when(eventLogger).message(any(LogMessage.class));

        final Session_1_0 session = createSession_1_0(connection, 0);

        final Subject subject = capturedSubject.get();
        assertNotNull(subject, "Subject should be set during CREATE logging");
        final Set<SessionPrincipal> sessionPrincipals = subject.getPrincipals(SessionPrincipal.class);
        assertEquals(1, sessionPrincipals.size(), "Session principal should be present");
        assertEquals(session, sessionPrincipals.iterator().next().getSession(), "Unexpected logged session principal");
    }

    @Test
    void receiveAttachTopicNonDurableWithContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(false, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
    }

    @Test
    void receiveAttachTopicDurableNoContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(true, linkName, address, true);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);
    }

    @Test
    void receiveAttachTopicDurableWithContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(true, linkName+ "|1", address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        final AMQPConnection_1_0<?> secondConnection = createAmqpConnection_1_0("testContainerId2");
        final Session_1_0 secondSession = createSession_1_0(secondConnection, 0);
        final Attach attach2 = createTopicAttach(true, linkName + "|2", address, false);
        secondSession.receiveAttach(attach2);

        assertAttachSent(secondConnection, secondSession, attach2);
        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(2, (long) queues.size(),
                "Unexpected number of queues after second subscription with the same subscription name but different " +
                "container id ");

    }

    @Test
    void receiveAttachSharedTopicNonDurableNoContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createSharedTopicAttach(false, linkName, address, true);
        final Attach attach2 = createSharedTopicAttach(false, linkName, address, true);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        final AMQPConnection_1_0<?> secondConnection = createAmqpConnection_1_0();
        final Session_1_0 secondSession = createSession_1_0(secondConnection, 0);

        secondSession.receiveAttach(attach2);

        assertAttachSent(secondConnection, secondSession, attach2);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after attach");
        final Queue<?> queue = queues.iterator().next();

        final Collection<QueueConsumer<?,?>> consumers = queue.getConsumers();
        assertEquals(2, (long) consumers.size(), "Unexpected number of consumers");
    }

    @Test
    void receiveAttachSharedTopicNonDurableWithContainer()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createSharedTopicAttach(false, linkName, address, false);
        final Attach attach2 = createSharedTopicAttach(false, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        final AMQPConnection_1_0<?> secondConnection = createAmqpConnection_1_0("testContainerId2");
        final Session_1_0 secondSession = createSession_1_0(secondConnection, 0);

        secondSession.receiveAttach(attach2);

        assertAttachSent(secondConnection, secondSession, attach2);

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(2, (long) queues.size(), "Unexpected number of queues after attach");

        for (final Queue<?> queue : queues)
        {
            final Collection<QueueConsumer<?,?>> consumers = queue.getConsumers();
            assertEquals(1, (long) consumers.size(),
                    "Unexpected number of consumers on queue " + queue.getName());
        }
    }

    @Test
    void separateSubscriptionNameSpaces()
    {
        final AMQPConnection_1_0<?> secondConnection = createAmqpConnection_1_0();
        final Session_1_0 secondSession = createSession_1_0(secondConnection, 0);

        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;

        final Attach durableSharedWithContainerId = createSharedTopicAttach(true, linkName + "|1", address, false);
        _session.receiveAttach(durableSharedWithContainerId);
        assertAttachSent(_connection, _session, durableSharedWithContainerId, 0);

        Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        final Attach durableNonSharedWithContainerId = createTopicAttach(true, linkName, address, false);
        _session.receiveAttach(durableNonSharedWithContainerId);
        assertAttachFailed(_connection, _session, durableNonSharedWithContainerId, 1);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        final Attach nonDurableSharedWithContainerId = createSharedTopicAttach(false, linkName + "|3", address, false);
        _session.receiveAttach(nonDurableSharedWithContainerId);
        assertAttachSent(_connection, _session, nonDurableSharedWithContainerId, 3);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(2, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        final Attach durableSharedWithoutContainerId = createSharedTopicAttach(true, linkName + "|4", address, true);
        secondSession.receiveAttach(durableSharedWithoutContainerId);
        assertAttachSent(secondConnection, secondSession, durableSharedWithoutContainerId, 0);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(3, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        final Attach nonDurableSharedWithoutContainerId = createSharedTopicAttach(false, linkName + "|5", address, true);
        secondSession.receiveAttach(nonDurableSharedWithoutContainerId);
        assertAttachSent(secondConnection, secondSession, nonDurableSharedWithoutContainerId, 1);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(4, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        final Attach nonDurableNonSharedWithoutContainerId = createTopicAttach(false, linkName + "|6", address, true);
        secondSession.receiveAttach(nonDurableNonSharedWithoutContainerId);
        assertAttachSent(secondConnection, secondSession, nonDurableNonSharedWithoutContainerId, 2);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(5, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");

        final Attach nonDurableNonSharedWithContainerId = createTopicAttach(false, linkName + "|6", address, false);
        _session.receiveAttach(nonDurableNonSharedWithContainerId);
        assertAttachSent(_connection, _session, nonDurableNonSharedWithContainerId, 4);

        queues = _virtualHost.getChildren(Queue.class);
        assertEquals(6, (long) queues.size(),
                "Unexpected number of queues after durable non shared with containerId");
    }

    @Test
    void receiveAttachForInvalidUnsubscribe()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;

        final Attach unsubscribeAttach = createTopicAttach(true, linkName, address, false);
        unsubscribeAttach.setSource(null);

        _session.receiveAttach(unsubscribeAttach);
        assertAttachFailed(_connection, _session, unsubscribeAttach);

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(0, (long) queues.size(), "Unexpected number of queues after unsubscribe");
    }

    @Test
    void nullSourceLookup()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(true, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), false);

        final Attach nullSourceAttach = createTopicAttach(true, linkName, address, false);
        nullSourceAttach.setSource(null);

        _session.receiveAttach(nullSourceAttach);
        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection, times(3)).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        final Attach sentAttach = (Attach) frameCapture.getAllValues().get(2);

        assertEquals(nullSourceAttach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
        assertNotNull(sentAttach.getSource(), "Unexpected source");
        final Source source = (Source)sentAttach.getSource();
        assertEquals(address, source.getAddress(), "Unexpected address");
        assertTrue(Arrays.asList(source.getCapabilities()).contains(Symbol.valueOf("topic")),
                "Unexpected source capabilities");

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after unsubscribe");
    }

    @Test
    void receiveDetachClosed()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(true, linkName, address, false);

        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), true);

        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(0, (long) queues.size(), "Unexpected number of queues after unsubscribe");
    }

    @Test
    void receiveAttachToExistingQueue()
    {
        final String linkName = "testLink";
        final Attach attach = createQueueAttach(false, linkName, QUEUE_NAME);

        final Queue<?> queue = _virtualHost.createChild(Queue.class, Map.of(Queue.NAME, QUEUE_NAME));
        final Exchange<?> exchange = _virtualHost.getChildByName(Exchange.class, "amq.direct");
        exchange.bind(QUEUE_NAME, QUEUE_NAME, Map.of(), false);

        _session.receiveAttach(attach);

        assertAttachActions(queue, attach);
    }

    @Test
    void receiveAttachToNonExistingQueue()
    {
        final String linkName = "testLink";
        final Attach attach = createQueueAttach(false, linkName, QUEUE_NAME);
        _session.receiveAttach(attach);
        assertAttachFailed(_connection, _session, attach);
    }

    @Test
    void receiveAttachRebindingQueueNoActiveLinks()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createSharedTopicAttach(true, linkName, address, true);
        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), false);

        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection, times(2)).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        final boolean condition = frameCapture.getAllValues().get(1) instanceof Detach;
        assertTrue(condition);

        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        final String topicName2 = TOPIC_NAME + "2";
        final String address2 = "amq.direct/" + topicName2;
        final Attach attach2 = createSharedTopicAttach(true, linkName + "|2", address2, true);

        _session.receiveAttach(attach2);
        assertAttachSent(_connection, _session, attach2, 2);

        assertQueues(topicName2, LifetimePolicy.PERMANENT);
    }

    @Test
    void receiveReattachRebindingQueueNoActiveLinks()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createSharedTopicAttach(true, linkName, address, true);
        _session.receiveAttach(attach);

        assertAttachSent(_connection, _session, attach);
        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        sendDetach(_session, attach.getHandle(), false);

        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection, times(2)).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        final boolean condition = frameCapture.getAllValues().get(1) instanceof Detach;
        assertTrue(condition);

        assertQueues(TOPIC_NAME, LifetimePolicy.PERMANENT);

        final String topicName2 = TOPIC_NAME + "2";
        final String address2 = "amq.direct/" + topicName2;
        final Attach attach2 = createSharedTopicAttach(true, linkName, address2, true);

        _session.receiveAttach(attach2);
        assertAttachSent(_connection, _session, attach2, 2);

        assertQueues(topicName2, LifetimePolicy.PERMANENT);
    }

    @Test
    void receiveAttachTopicNonDurableNoContainerWithInvalidSelector()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final Attach attach = createTopicAttach(false, linkName, address, true);
        setSelector(attach, "invalid selector");

        _session.receiveAttach(attach);

        assertAttachFailed(_connection, _session, attach);
        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(0, (long) queues.size(), "Unexpected number of queues after attach");
    }

    @Test
    void receiveAttachTopicNonDurableNoContainerWithValidSelector()
    {
        final String linkName = "testLink";
        final String address = "amq.direct/" + TOPIC_NAME;
        final String selectorExpression = "test='test'";
        final Attach attach = createTopicAttach(false, linkName, address, true);
        setSelector(attach, selectorExpression);

        _session.receiveAttach(attach);

        final Attach sentAttach = captureAttach(_connection, _session, 0);

        assertEquals(attach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
        assertFilter(sentAttach, selectorExpression);

        assertQueues(TOPIC_NAME, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);

        final Binding binding = findBinding("amq.direct", TOPIC_NAME);
        assertNotNull(binding, "Binding is not found");
        final Map<String,Object> arguments = binding.getArguments();
        assertNotNull(arguments, "Unexpected arguments");
        assertEquals(selectorExpression, arguments.get(AMQPFilterTypes.JMS_SELECTOR.toString()),
                "Unexpected filter on binding");
    }

    @Test
    void linkStealing()
    {
        _virtualHost.createChild(Queue.class, Map.of(Queue.NAME, QUEUE_NAME));
        final Attach attach = createQueueAttach(true, getTestName(), QUEUE_NAME);

        final AMQPConnection_1_0<?> connection1 = _connection;
        final Session_1_0 session1 = _session;
        session1.receiveAttach(attach);

        final Link_1_0<?,?> link = _virtualHost.getSendingLink(connection1.getRemoteContainerId(), attach.getName());
        assertNotNull(link, "Link is not created");
        assertAttachSent(connection1, session1, attach);

        final AMQPConnection_1_0<?> connection2 = createAmqpConnection_1_0(connection1.getRemoteContainerId());
        final Session_1_0 session2 = createSession_1_0(connection2, 0);
        session2.receiveAttach(attach);

        assertDetachSent(connection1, session1, LinkError.STOLEN, 1);
        assertAttachSent(connection2, session2, attach);
    }

    @Test
    void attachSourceDynamicWithLifeTimePolicyDeleteOnClose()
    {
        final Attach attach = createReceiverAttach(getTestName());
        final Source source = createDynamicSource(new DeleteOnClose());
        attach.setSource(source);

        _session.receiveAttach(attach);

        assertQueueDurability(getDynamicNodeAddressFromAttachResponse(), false);
    }

    @Test
    void attachSourceDynamicWithLifeTimePolicyDeleteOnCloseAndExpiryPolicyNever()
    {
        final Attach attach = createReceiverAttach(getTestName());
        final Source source = createDynamicSource(new DeleteOnClose());
        source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
        attach.setSource(source);

        _session.receiveAttach(attach);

        assertQueueDurability(getDynamicNodeAddressFromAttachResponse(), true);
    }

    @Test
    void sendTransferDoesNotSetPayloadOnContinuationTransfers()
    {
        // Arrange: payload 25 bytes -> 3 sendFrame calls if "wire" writes 10 bytes per call
        final byte[] bytes = createSequentialBytes(25);

        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            xfr.setPayload(buf);
        }

        when(_connection.sendFrame(eq(0), any(), any(QpidByteBuffer.class))).thenAnswer(inv ->
        {
            final QpidByteBuffer payload = inv.getArgument(2);
            if (payload == null)
            {
                return 0;
            }
            final int chunk = Math.min(10, payload.remaining());
            payload.position(payload.position() + chunk);
            return chunk;
        });

        final ArgumentCaptor<FrameBody> bodyCaptor = ArgumentCaptor.forClass(FrameBody.class);
        final SendingLinkEndpoint endpoint = mock(SendingLinkEndpoint.class);

        _session.sendTransfer(xfr, endpoint);

        verify(_connection, times(3)).sendFrame(eq(0), bodyCaptor.capture(), any(QpidByteBuffer.class));
        final List<FrameBody> bodies = bodyCaptor.getAllValues();
        assertInstanceOf(Transfer.class, bodies.get(0));
        assertNull(((Transfer) bodies.get(1)).getPayload(), "Continuation transfer must not carry payload");
        assertNull(((Transfer) bodies.get(2)).getPayload(), "Continuation transfer must not carry payload");

        xfr.dispose();
    }

    @Test
    void sendTransferChunksPayloadPreservesByteOrderAndSetsMoreFlag()
    {
        // Arrange: payload 25 bytes -> 3 sendFrame calls if the transport writes 10 bytes per call
        final byte[] bytes = createSequentialBytes(25);

        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setRcvSettleMode(ReceiverSettleMode.FIRST);
        xfr.setState(Accepted.INSTANCE);
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            xfr.setPayload(buf);
        }

        final ByteArrayOutputStream capturedPayload = stubSendFrameToWriteAtMostAndCapturePayload(10);
        final ArgumentCaptor<FrameBody> bodyCaptor = ArgumentCaptor.forClass(FrameBody.class);
        final SendingLinkEndpoint endpoint = mock(SendingLinkEndpoint.class);

        // Act
        _session.sendTransfer(xfr, endpoint);

        // Assert
        verify(_connection, times(3)).sendFrame(eq(0), bodyCaptor.capture(), nullable(QpidByteBuffer.class));
        assertArrayEquals(bytes, capturedPayload.toByteArray(), "Payload bytes were not preserved across chunks");

        final List<FrameBody> bodies = bodyCaptor.getAllValues();
        assertEquals(3, bodies.size(), "Unexpected number of frames");

        assertSame(xfr, bodies.get(0), "First frame body should be the original Transfer");
        assertNotSame(xfr, bodies.get(1), "Continuation frame should use a new Transfer instance");
        assertNotSame(xfr, bodies.get(2), "Continuation frame should use a new Transfer instance");

        final Transfer first = (Transfer) bodies.get(0);
        final Transfer second = (Transfer) bodies.get(1);
        final Transfer third = (Transfer) bodies.get(2);

        assertEquals(Boolean.TRUE, first.getMore(), "First transfer should have more=true when payload is chunked");
        assertEquals(Boolean.TRUE, second.getMore(), "Intermediate continuation transfer should have more=true");
        assertNotEquals(Boolean.TRUE, third.getMore(), "Last transfer must not have more=true");

        assertNull(second.getPayload(), "Continuation transfer must not carry payload");
        assertNull(third.getPayload(), "Continuation transfer must not carry payload");

        // Continuations should preserve key delivery settings
        assertEquals(first.getHandle(), second.getHandle(), "Handle must be preserved on continuation transfer");
        assertEquals(first.getHandle(), third.getHandle(), "Handle must be preserved on continuation transfer");
        assertEquals(first.getRcvSettleMode(), second.getRcvSettleMode(), "RcvSettleMode must be preserved");
        assertEquals(first.getRcvSettleMode(), third.getRcvSettleMode(), "RcvSettleMode must be preserved");
        assertEquals(first.getState(), second.getState(), "State must be preserved");
        assertEquals(first.getState(), third.getState(), "State must be preserved");

        // Delivery-id is assigned only to the first transfer
        assertNotNull(first.getDeliveryId(), "DeliveryId should be assigned to the first transfer");
        assertNull(second.getDeliveryId(), "Continuation transfer must not set delivery-id");
        assertNull(third.getDeliveryId(), "Continuation transfer must not set delivery-id");

        xfr.dispose();
    }

    @Test
    void sendTransferSendsSingleFrameWhenPayloadFitsWithinChunk()
    {
        final byte[] bytes = createSequentialBytes(9);

        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            xfr.setPayload(buf);
        }

        final ByteArrayOutputStream capturedPayload = stubSendFrameToWriteAtMostAndCapturePayload(10);
        final ArgumentCaptor<FrameBody> bodyCaptor = ArgumentCaptor.forClass(FrameBody.class);

        _session.sendTransfer(xfr, mock(SendingLinkEndpoint.class));

        verify(_connection, times(1)).sendFrame(eq(0), bodyCaptor.capture(), nullable(QpidByteBuffer.class));
        assertArrayEquals(bytes, capturedPayload.toByteArray(), "Payload bytes were not written as a single chunk");

        final Transfer body = (Transfer) bodyCaptor.getValue();
        assertNotEquals(Boolean.TRUE, body.getMore(), "Single-frame transfer must not have more=true");

        xfr.dispose();
    }

    @Test
    void sendTransferSendsSingleFrameWhenPayloadExactlyEqualsChunk()
    {
        final byte[] bytes = createSequentialBytes(10);

        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            xfr.setPayload(buf);
        }

        final ByteArrayOutputStream capturedPayload = stubSendFrameToWriteAtMostAndCapturePayload(10);
        final ArgumentCaptor<FrameBody> bodyCaptor = ArgumentCaptor.forClass(FrameBody.class);

        _session.sendTransfer(xfr, mock(SendingLinkEndpoint.class));

        verify(_connection, times(1)).sendFrame(eq(0), bodyCaptor.capture(), nullable(QpidByteBuffer.class));
        assertArrayEquals(bytes, capturedPayload.toByteArray(), "Payload bytes were not written as a single chunk");

        final Transfer body = (Transfer) bodyCaptor.getValue();
        assertNotEquals(Boolean.TRUE, body.getMore(), "Single-frame transfer must not have more=true");

        xfr.dispose();
    }

    @Test
    void sendTransferSendsTwoFramesWhenPayloadIsOneByteOverChunk()
    {
        final byte[] bytes = createSequentialBytes(11);

        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            xfr.setPayload(buf);
        }

        final ByteArrayOutputStream capturedPayload = stubSendFrameToWriteAtMostAndCapturePayload(10);
        final ArgumentCaptor<FrameBody> bodyCaptor = ArgumentCaptor.forClass(FrameBody.class);

        _session.sendTransfer(xfr, mock(SendingLinkEndpoint.class));

        verify(_connection, times(2)).sendFrame(eq(0), bodyCaptor.capture(), nullable(QpidByteBuffer.class));
        assertArrayEquals(bytes, capturedPayload.toByteArray(), "Payload bytes were not preserved across two chunks");

        final List<FrameBody> bodies = bodyCaptor.getAllValues();
        final Transfer first = (Transfer) bodies.get(0);
        final Transfer second = (Transfer) bodies.get(1);

        assertEquals(Boolean.TRUE, first.getMore(), "First transfer should have more=true when payload is chunked");
        assertNotEquals(Boolean.TRUE, second.getMore(), "Last transfer must not have more=true");
        assertNull(second.getPayload(), "Continuation transfer must not carry payload");

        xfr.dispose();
    }

    @Test
    void sendTransferWithEmptyPayloadDoesNotSendContinuation()
    {
        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[0]))
        {
            xfr.setPayload(buf);
        }

        final ByteArrayOutputStream capturedPayload = stubSendFrameToWriteAtMostAndCapturePayload(10);
        final ArgumentCaptor<FrameBody> bodyCaptor = ArgumentCaptor.forClass(FrameBody.class);

        _session.sendTransfer(xfr, mock(SendingLinkEndpoint.class));

        verify(_connection, times(1)).sendFrame(eq(0), bodyCaptor.capture(), nullable(QpidByteBuffer.class));
        assertEquals(0, capturedPayload.size(), "Empty payload should not produce any bytes");

        xfr.dispose();
    }

    @Test
    void receiveDispositionWithFirstGreaterThanLastClosesConnection()
    {
        final Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(UnsignedInteger.valueOf(6));
        disposition.setLast(UnsignedInteger.valueOf(5));
        disposition.setSettled(Boolean.TRUE);

        _session.receiveDisposition(disposition);

        final ArgumentCaptor<Error> errorCaptor = ArgumentCaptor.forClass(Error.class);
        verify(_connection).close(errorCaptor.capture());
        assertEquals(AmqpError.NOT_ALLOWED, errorCaptor.getValue().getCondition(),
                "Connection closed with unexpected error condition");
        assertEquals("Illegal disposition delivery range: first '6', last '5'", errorCaptor.getValue().getDescription(),
                "Connection closed with unexpected error description");
        verify(_connection, never()).sendEnd(eq(_session.getChannelId()), any(End.class), eq(true));
    }

    @Test
    void receiveDispositionWithFirstEqualToLastSettlesSingleDelivery()
    {
        final DeliveryRegistry outgoing = _session.getOutgoingDeliveryRegistry();
        final LinkEndpoint<?, ?> linkEndpoint = mock(LinkEndpoint.class);

        final UnsignedInteger deliveryId = UnsignedInteger.valueOf(5);
        outgoing.addDelivery(deliveryId, new UnsettledDelivery(new Binary(new byte[0]), linkEndpoint));

        final Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(deliveryId);
        disposition.setLast(deliveryId);
        disposition.setSettled(Boolean.TRUE);
        disposition.setState(Accepted.INSTANCE);

        _session.receiveDisposition(disposition);

        verify(linkEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        assertEquals(0, outgoing.size(), "Delivery should be removed after settled disposition");
        verify(_connection, never()).sendEnd(eq(_session.getChannelId()), any(End.class), eq(true));
    }

    @Test
    void receiveDispositionWithAntipodalRangeClosesConnection()
    {
        final DeliveryRegistry outgoing = _session.getOutgoingDeliveryRegistry();
        final LinkEndpoint<?, ?> linkEndpoint = mock(LinkEndpoint.class);

        final UnsignedInteger deliveryId = UnsignedInteger.valueOf(5);
        outgoing.addDelivery(deliveryId, new UnsettledDelivery(new Binary(new byte[]{0}), linkEndpoint));

        final Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(deliveryId);
        disposition.setLast(UnsignedInteger.valueOf((deliveryId.longValue() + 0x80000000L) & 0xFFFFFFFFL));
        disposition.setSettled(Boolean.TRUE);
        disposition.setState(new Accepted());

        _session.receiveDisposition(disposition);

        verify(linkEndpoint, never()).receiveDeliveryState(any(Binary.class), any(), any(Boolean.class));
        assertEquals(1, outgoing.size(), "Delivery should not be removed for antipodal range");
        final ArgumentCaptor<Error> errorCaptor = ArgumentCaptor.forClass(Error.class);
        verify(_connection).close(errorCaptor.capture());
        assertEquals(AmqpError.NOT_ALLOWED, errorCaptor.getValue().getCondition(),
                "Connection closed with unexpected error condition");
        assertEquals("Illegal disposition delivery range: first '5', last '2147483653'",
                errorCaptor.getValue().getDescription(), "Connection closed with unexpected error description");
        verify(_connection, never()).sendEnd(eq(_session.getChannelId()), any(End.class), eq(true));
    }

    @Test
    void receiveDispositionWithMaximumValidRangeSettlesDeliveries()
    {
        final DeliveryRegistry outgoing = _session.getOutgoingDeliveryRegistry();
        final LinkEndpoint<?, ?> firstEndpoint = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> lastEndpoint = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> outsideEndpoint = mock(LinkEndpoint.class);
        final UnsignedInteger first = UnsignedInteger.valueOf(5);
        final UnsignedInteger last = UnsignedInteger.valueOf(first.longValue() + 0x7FFFFFFFL);
        final UnsignedInteger outside = UnsignedInteger.valueOf(first.longValue() + 0x80000000L);

        outgoing.addDelivery(first, new UnsettledDelivery(new Binary(new byte[]{1}), firstEndpoint));
        outgoing.addDelivery(last, new UnsettledDelivery(new Binary(new byte[]{2}), lastEndpoint));
        outgoing.addDelivery(outside, new UnsettledDelivery(new Binary(new byte[]{3}), outsideEndpoint));

        final Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(first);
        disposition.setLast(last);
        disposition.setSettled(Boolean.TRUE);
        disposition.setState(Accepted.INSTANCE);

        _session.receiveDisposition(disposition);

        verify(firstEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(lastEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(outsideEndpoint, never()).receiveDeliveryState(any(Binary.class), any(), any(Boolean.class));
        assertEquals(1, outgoing.size(), "Only deliveries outside the range should remain unsettled");
        verify(_connection, never()).close(any(Error.class));
    }

    @Test
    void receiveDispositionWithSparseRangeScansRegistry()
    {
        final DeliveryRegistry outgoing = _session.getOutgoingDeliveryRegistry();
        final LinkEndpoint<?, ?> insideStart = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> insideMiddle = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> insideEnd = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> outside = mock(LinkEndpoint.class);

        outgoing.addDelivery(UnsignedInteger.valueOf(100), new UnsettledDelivery(new Binary(new byte[]{1}), insideStart));
        outgoing.addDelivery(UnsignedInteger.valueOf(450), new UnsettledDelivery(new Binary(new byte[]{2}), insideMiddle));
        outgoing.addDelivery(UnsignedInteger.valueOf(1000), new UnsettledDelivery(new Binary(new byte[]{3}), insideEnd));
        outgoing.addDelivery(UnsignedInteger.valueOf(1001), new UnsettledDelivery(new Binary(new byte[]{4}), outside));

        final Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(UnsignedInteger.valueOf(100));
        disposition.setLast(UnsignedInteger.valueOf(1000));
        disposition.setSettled(Boolean.TRUE);
        disposition.setState(Accepted.INSTANCE);

        _session.receiveDisposition(disposition);

        verify(insideStart).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(insideMiddle).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(insideEnd).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(outside, never()).receiveDeliveryState(any(Binary.class), any(), any(Boolean.class));
        assertEquals(1, outgoing.size(), "Only deliveries outside the range should remain unsettled");
    }

    @Test
    void receiveDispositionWithValidWrapAroundRangeSettlesDeliveries()
    {
        final DeliveryRegistry outgoing = _session.getOutgoingDeliveryRegistry();
        final LinkEndpoint<?, ?> firstEndpoint = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> secondEndpoint = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> thirdEndpoint = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> fourthEndpoint = mock(LinkEndpoint.class);
        final LinkEndpoint<?, ?> outsideEndpoint = mock(LinkEndpoint.class);

        outgoing.addDelivery(UnsignedInteger.valueOf(0xFFFFFFFEL), new UnsettledDelivery(new Binary(new byte[]{1}), firstEndpoint));
        outgoing.addDelivery(UnsignedInteger.valueOf(0xFFFFFFFFL), new UnsettledDelivery(new Binary(new byte[]{2}), secondEndpoint));
        outgoing.addDelivery(UnsignedInteger.ZERO, new UnsettledDelivery(new Binary(new byte[]{3}), thirdEndpoint));
        outgoing.addDelivery(UnsignedInteger.ONE, new UnsettledDelivery(new Binary(new byte[]{4}), fourthEndpoint));
        outgoing.addDelivery(UnsignedInteger.valueOf(2), new UnsettledDelivery(new Binary(new byte[]{5}), outsideEndpoint));

        final Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(UnsignedInteger.valueOf(0xFFFFFFFEL));
        disposition.setLast(UnsignedInteger.ONE);
        disposition.setSettled(Boolean.TRUE);
        disposition.setState(Accepted.INSTANCE);

        _session.receiveDisposition(disposition);

        verify(firstEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(secondEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(thirdEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(fourthEndpoint).receiveDeliveryState(any(Binary.class), any(), eq(Boolean.TRUE));
        verify(outsideEndpoint, never()).receiveDeliveryState(any(Binary.class), any(), any(Boolean.class));
        assertEquals(1, outgoing.size(), "Only deliveries outside the wrap-around range should remain unsettled");
    }

    @Test
    void sendTransferWithNullPayloadDoesNotThrowAndDoesNotSendContinuation()
    {
        final Transfer xfr = new Transfer();
        xfr.setSettled(true);
        xfr.setHandle(UnsignedInteger.valueOf(0));
        xfr.setDeliveryTag(new Binary(new byte[]{0x01}));
        // no payload set

        when(_connection.sendFrame(eq(0), any(), nullable(QpidByteBuffer.class))).thenReturn(0);
        final ArgumentCaptor<QpidByteBuffer> payloadCaptor = ArgumentCaptor.forClass(QpidByteBuffer.class);

        _session.sendTransfer(xfr, mock(SendingLinkEndpoint.class));

        verify(_connection, times(1)).sendFrame(eq(0), any(), payloadCaptor.capture());
        assertNull(payloadCaptor.getValue(), "Expected payload argument to be null");

        xfr.dispose();
    }

    @Test
    void receiveFlowWithRewoundNextOutgoingIdEndsSession() throws Exception
    {
        final Session_1_0 session = createSession_1_0(_connection, 0, 2);
        final UnsignedInteger handle = UnsignedInteger.ZERO;
        final AbstractReceivingLinkEndpoint<?> endpoint = mock(AbstractReceivingLinkEndpoint.class);

        registerInputHandle(session, handle, endpoint);
        session.receiveTransfer(createTransfer(handle, UnsignedInteger.ZERO, (byte) 0x01));
        session.receiveTransfer(createTransfer(handle, UnsignedInteger.ONE, (byte) 0x02));

        final Flow flow = new Flow();
        flow.setIncomingWindow(UnsignedInteger.ONE);
        flow.setNextIncomingId(UnsignedInteger.ZERO);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setOutgoingWindow(UnsignedInteger.ONE);
        flow.setHandle(handle);

        session.receiveFlow(flow);

        final ArgumentCaptor<End> endCaptor = ArgumentCaptor.forClass(End.class);
        verify(_connection).sendEnd(eq(session.getChannelId()), endCaptor.capture(), eq(true));
        assertEquals(SessionError.WINDOW_VIOLATION, endCaptor.getValue().getError().getCondition(),
                "Session should be ended with WINDOW_VIOLATION error");
        verify(endpoint, times(2)).receiveTransfer(any(Transfer.class));
        verify(endpoint, never()).receiveFlow(any(Flow.class));
    }

    @Test
    void receiveFlowAfterSessionEndIsIgnoredWithoutSecondEnd()
    {
        // End the session first via an unknown-handle transfer (UNATTACHED_HANDLE).
        _session.receiveTransfer(createTransfer(UnsignedInteger.valueOf(9999), UnsignedInteger.ZERO, (byte) 0x01));

        // Flow pipelined after the broker has ended the session must be ignored: it must not
        // trigger a second End (which would otherwise carry ILLEGAL_STATE and lose the real cause).
        final Flow flow = new Flow();
        flow.setIncomingWindow(UnsignedInteger.ONE);
        flow.setNextIncomingId(UnsignedInteger.ZERO);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setOutgoingWindow(UnsignedInteger.ONE);
        _session.receiveFlow(flow);

        final ArgumentCaptor<End> endCaptor = ArgumentCaptor.forClass(End.class);
        verify(_connection, times(1)).sendEnd(eq(_session.getChannelId()), endCaptor.capture(), eq(true));
        assertEquals(SessionError.UNATTACHED_HANDLE, endCaptor.getValue().getError().getCondition(),
                "Only the original UNATTACHED_HANDLE End should be sent; the late Flow must not cause a second End");
    }

    @Test
    void receiveTransferBeyondInitialIncomingWindowEndsSession() throws Exception
    {
        final Session_1_0 session = createSession_1_0(_connection, 0, 2);
        final UnsignedInteger handle = UnsignedInteger.ZERO;
        final AbstractReceivingLinkEndpoint<?> endpoint = mock(AbstractReceivingLinkEndpoint.class);

        registerInputHandle(session, handle, endpoint);

        session.receiveTransfer(createTransfer(handle, UnsignedInteger.ZERO, (byte) 0x01));
        session.receiveTransfer(createTransfer(handle, UnsignedInteger.ONE, (byte) 0x02));

        final Transfer violatingTransfer = spy(createTransfer(handle, UnsignedInteger.valueOf(2), (byte) 0x03));
        session.receiveTransfer(violatingTransfer);

        verify(endpoint, times(2)).receiveTransfer(any(Transfer.class));
        verify(violatingTransfer).dispose();
        final ArgumentCaptor<End> endCaptor = ArgumentCaptor.forClass(End.class);
        verify(_connection).sendEnd(eq(session.getChannelId()), endCaptor.capture(), eq(true));
        assertEquals(SessionError.WINDOW_VIOLATION, endCaptor.getValue().getError().getCondition(),
                "Session should be ended with WINDOW_VIOLATION error");
    }

    @Test
    void receiveTransferBeyondFlowAdvertisedIncomingWindowEndsSession() throws Exception
    {
        final Session_1_0 session = createSession_1_0(_connection, 0, 2);
        final UnsignedInteger handle = UnsignedInteger.ZERO;
        final AbstractReceivingLinkEndpoint<?> endpoint = mock(AbstractReceivingLinkEndpoint.class);

        registerInputHandle(session, handle, endpoint);
        session.sendFlow();

        session.receiveTransfer(createTransfer(handle, UnsignedInteger.ZERO, (byte) 0x01));
        session.receiveTransfer(createTransfer(handle, UnsignedInteger.ONE, (byte) 0x02));

        final Transfer violatingTransfer = spy(createTransfer(handle, UnsignedInteger.valueOf(2), (byte) 0x03));
        session.receiveTransfer(violatingTransfer);

        verify(endpoint, times(2)).receiveTransfer(any(Transfer.class));
        verify(violatingTransfer).dispose();
        final ArgumentCaptor<End> endCaptor = ArgumentCaptor.forClass(End.class);
        verify(_connection).sendEnd(eq(session.getChannelId()), endCaptor.capture(), eq(true));
        assertEquals(SessionError.WINDOW_VIOLATION, endCaptor.getValue().getError().getCondition(),
                "Session should be ended with WINDOW_VIOLATION error");
    }

    @Test
    void receiveTransferWithUnknownHandleEndsSessionNotConnection()
    {
        _session.receiveTransfer(createTransfer(UnsignedInteger.valueOf(9999), UnsignedInteger.ZERO, (byte) 0x01));

        final ArgumentCaptor<End> endCaptor = ArgumentCaptor.forClass(End.class);
        verify(_connection).sendEnd(eq(_session.getChannelId()), endCaptor.capture(), eq(true));
        assertEquals(SessionError.UNATTACHED_HANDLE, endCaptor.getValue().getError().getCondition(),
                "Session should be ended with UNATTACHED_HANDLE error");

        verify(_connection, never()).close(any(org.apache.qpid.server.protocol.v1_0.type.transport.Error.class));
    }

    @Test
    void receiveTransferAfterSessionEndIsDisposedAndIgnored()
    {
        final Transfer firstTransfer = createTransfer(UnsignedInteger.valueOf(9999), UnsignedInteger.ZERO, (byte) 0x01);
        final Transfer ignoredTransfer = spy(createTransfer(UnsignedInteger.ZERO, UnsignedInteger.ONE, (byte) 0x02));

        _session.receiveTransfer(firstTransfer);
        _session.receiveTransfer(ignoredTransfer);

        verify(ignoredTransfer).dispose();
        final ArgumentCaptor<End> endCaptor = ArgumentCaptor.forClass(End.class);
        verify(_connection).sendEnd(eq(_session.getChannelId()), endCaptor.capture(), eq(true));
        assertEquals(SessionError.UNATTACHED_HANDLE, endCaptor.getValue().getError().getCondition(),
                "Session should retain the original UNATTACHED_HANDLE end");

        verify(_connection, never()).close(any(org.apache.qpid.server.protocol.v1_0.type.transport.Error.class));
    }

    @Test
    void rapidRepeatedSessionEchoesProduceOneImmediateAndOneDelayedFlow()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final Session_1_0 session = createSession_1_0(_connection, 0, 2048L,
                AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS, clock, scheduler, EchoFlowTestSupport.DIRECT_EXECUTOR);

        final Flow echoFlow = createSessionEchoFlow();

        session.receiveFlow(echoFlow);
        session.receiveFlow(echoFlow);

        verify(_connection, times(1)).sendFrame(eq(session.getChannelId()), any(Flow.class));
        assertEquals(1, scheduler.getScheduledTaskCount());

        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        scheduler.runNext();

        verify(_connection, times(2)).sendFrame(eq(session.getChannelId()), any(Flow.class));
        assertEquals(0, scheduler.getScheduledTaskCount());
    }

    @Test
    void linkFlowSendDoesNotConsumePendingSessionEcho()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final Session_1_0 session = createSession_1_0(_connection, 0, 2048L,
                AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS, clock, scheduler, EchoFlowTestSupport.DIRECT_EXECUTOR);

        session.receiveFlow(createSessionEchoFlow());
        session.receiveFlow(createSessionEchoFlow());

        final Flow linkFlow = new Flow();
        linkFlow.setHandle(UnsignedInteger.ZERO);
        session.sendFlow(linkFlow);

        verify(_connection, times(2)).sendFrame(eq(session.getChannelId()), any(Flow.class));
        assertEquals(1, scheduler.getScheduledTaskCount());

        clock.advanceMillis(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        scheduler.runNext();

        verify(_connection, times(3)).sendFrame(eq(session.getChannelId()), any(Flow.class));
        assertEquals(0, scheduler.getScheduledTaskCount());
    }

    @Test
    void endingSessionCancelsPendingDelayedEchoReply()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final Session_1_0 session = createSession_1_0(_connection, 0, 2048L,
                AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS, clock, scheduler, EchoFlowTestSupport.DIRECT_EXECUTOR);

        session.receiveFlow(createSessionEchoFlow());
        session.receiveFlow(createSessionEchoFlow());
        session.end();

        assertEquals(0, scheduler.getScheduledTaskCount());
        verify(_connection, times(1)).sendFrame(eq(session.getChannelId()), any(Flow.class));
    }

    private static byte[] createSequentialBytes(final int length)
    {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++)
        {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    private ByteArrayOutputStream stubSendFrameToWriteAtMostAndCapturePayload(final int maxBytesPerCall)
    {
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();

        when(_connection.sendFrame(eq(0), any(), nullable(QpidByteBuffer.class))).thenAnswer(inv ->
        {
            final FrameBody body = inv.getArgument(1);
            final QpidByteBuffer payload = inv.getArgument(2);
            if (payload == null)
            {
                return 0;
            }

            final int remaining = payload.remaining();
            final int chunk = Math.min(maxBytesPerCall, remaining);

            if (body instanceof Transfer)
            {
                // Simulate the AMQP transport behaviour: set 'more' when not all payload is written.
                ((Transfer) body).setMore(remaining > chunk);
            }

            // Copy the bytes without affecting the original payload buffer position.
            try (final QpidByteBuffer dup = payload.duplicate())
            {
                final byte[] bytes = new byte[chunk];
                dup.get(bytes);
                captured.write(bytes);
            }

            payload.position(payload.position() + chunk);
            return chunk;
        });

        return captured;
    }

    private Source createDynamicSource(final DeleteOnClose lifetimePolicy)
    {
        final Source source = new Source();
        source.setDynamic(true);
        source.setDynamicNodeProperties(Map.of(Symbols.LIFETIME_POLICY, lifetimePolicy));
        return source;
    }

    private String getDynamicNodeAddressFromAttachResponse()
    {
        final Attach sentAttach = captureAttach(_connection, _session, 0);
        assertTrue(sentAttach.getSource() instanceof Source);
        return ((Source) (sentAttach.getSource())).getAddress();
    }

    void assertQueueDurability(final String queueName, final boolean expectedDurability)
    {
        final Queue<?> queue = _virtualHost.getChildByName(Queue.class, queueName);
        assertNotNull(queue, "Queue not found");
        assertEquals(queue.isDurable(), expectedDurability, "Unexpected durability");
    }

    private void assertFilter(final Attach sentAttach, final String selectorExpression)
    {
        final Source source = (Source)sentAttach.getSource();
        final Map<Symbol, Filter> filter = source.getFilter();
        assertNotNull(filter, "Filter is not set in response");
        assertEquals(1, filter.size(), "Unexpected filter size");
        assertTrue(filter.containsKey(JMS_SELECTOR_FILTER), "Selector is not found");
        final Filter jmsSelectorFilter = filter.get(JMS_SELECTOR_FILTER);
        final boolean condition = jmsSelectorFilter instanceof JMSSelectorFilter;
        assertTrue(condition, "Unexpected selector filter");
        assertEquals(selectorExpression, ((JMSSelectorFilter) jmsSelectorFilter).getValue(), "Unexpected selector");
    }

    private Binding findBinding(final String exchangeName, final String bindingName)
    {
        final Exchange<?> exchange = _virtualHost.findConfiguredObject(Exchange.class, exchangeName);
        return exchange.getBindings().stream()
                .filter(binding -> bindingName.equals(binding.getName()))
                .findFirst().orElse(null);
    }

    private void setSelector(final Attach attach, final String selectorExpression)
    {
        final JMSSelectorFilter selector = new JMSSelectorFilter(selectorExpression);
        final Map<Symbol, Filter> filter = Map.of(Symbol.getSymbol("jms-selector"), selector);
        ((Source)attach.getSource()).setFilter(filter);
    }

    private void assertAttachActions(final Queue<?> queue, final Attach receivedAttach)
    {
        final Collection<QueueConsumer<?,?>> consumers = queue.getConsumers();
        assertEquals(1, (long) consumers.size(), "Unexpected consumers size");

        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(_connection).sendFrame(eq(_session.getChannelId()), frameCapture.capture());
        final Attach sentAttach = (Attach) frameCapture.getValue();

        assertEquals(receivedAttach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");

        final Source receivedSource = (Source) receivedAttach.getSource();
        final Source sentSource = (Source) sentAttach.getSource();
        assertEquals(receivedSource.getAddress(), sentSource.getAddress(), "Unexpected source address");
        assertArrayEquals(receivedSource.getCapabilities(), sentSource.getCapabilities(),
                "Unexpected source capabilities");
        assertEquals(receivedSource.getDurable(), sentSource.getDurable(), "Unexpected source durability");
        assertEquals(receivedSource.getExpiryPolicy(), sentSource.getExpiryPolicy(),
                "Unexpected source expiry policy");
        assertEquals(receivedSource.getDynamic(), sentSource.getDynamic(), "Unexpected source dynamic flag");

        final Target receivedTarget = (Target) receivedAttach.getTarget();
        final Target sentTarget = (Target) sentAttach.getTarget();
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
        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(connection, times(invocationOffset + 1)).sendFrame(eq(session.getChannelId()), frameCapture.capture());
        final List<FrameBody> sentFrames = frameCapture.getAllValues();

        final boolean condition = sentFrames.get(invocationOffset) instanceof Detach;
        assertTrue(condition, "unexpected Frame sent");
        final Detach sentDetach = (Detach) sentFrames.get(invocationOffset);
        assertTrue(sentDetach.getClosed(), "Unexpected closed state");
        assertEquals(expectedError, sentDetach.getError().getCondition(), "Closed with unexpected error condition");
    }

    private void assertAttachSent(final AMQPConnection_1_0<?> connection,
                                  final Session_1_0 session,
                                  final Attach receivedAttach)
    {
        assertAttachSent(connection, session, receivedAttach, 0);
    }

    private void assertAttachSent(final AMQPConnection_1_0<?> connection,
                                  final Session_1_0 session,
                                  final Attach receivedAttach,
                                  final int invocationOffset)
    {
        final Attach sentAttach = captureAttach(connection, session, invocationOffset);

        assertEquals(receivedAttach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
    }

    private Attach captureAttach(final AMQPConnection_1_0<?> connection,
                                 final Session_1_0 session,
                                 final int invocationOffset)
    {
        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(connection, times(invocationOffset + 1))
                .sendFrame(eq(session.getChannelId()), frameCapture.capture());
        return (Attach) frameCapture.getAllValues().get(invocationOffset);
    }

    private void assertQueues(final String publishingLinkName, final LifetimePolicy expectedLifetimePolicy)
    {
        final Collection<Queue> queues = _virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after attach");
        final Queue<?> queue = queues.iterator().next();
        assertEquals(expectedLifetimePolicy, queue.getLifetimePolicy(), "Unexpected queue durability");

        final Collection<PublishingLink> queuePublishingLinks = queue.getPublishingLinks();
        assertEquals(1, (long) queuePublishingLinks.size(), "Unexpected number of publishing links");
        assertEquals(publishingLinkName, queuePublishingLinks.iterator().next().getName(), "Unexpected link name");

        final Exchange<?> exchange = _virtualHost.getChildByName(Exchange.class, "amq.direct");
        assertTrue(exchange.hasBinding(publishingLinkName, queue), "Binding should exist");
    }

    private void assertAttachFailed(final AMQPConnection_1_0<?> connection,
                                    final Session_1_0 session,
                                    final Attach attach,
                                    int invocationOffset)
    {
        final ArgumentCaptor<FrameBody> frameCapture = ArgumentCaptor.forClass(FrameBody.class);
        verify(connection, times(invocationOffset + 2)).sendFrame(eq(session.getChannelId()), frameCapture.capture());
        final List<FrameBody> sentFrames = frameCapture.getAllValues();

        final boolean condition1 = sentFrames.get(invocationOffset) instanceof Attach;
        assertTrue(condition1, "unexpected Frame sent");
        final Attach sentAttach = (Attach) sentFrames.get(invocationOffset);
        assertEquals(attach.getName(), sentAttach.getName(), "Unexpected name");
        assertEquals(Role.SENDER, sentAttach.getRole(), "Unexpected role");
        assertNull(sentAttach.getSource(), "Unexpected source");

        final boolean condition = sentFrames.get(invocationOffset + 1) instanceof Detach;
        assertTrue(condition, "unexpected Frame sent");
        final Detach sentDetach = (Detach) sentFrames.get(invocationOffset + 1);
        assertTrue(sentDetach.getClosed(), "Unexpected closed state");
    }

    private void assertAttachFailed(final AMQPConnection_1_0<?> connection, final Session_1_0 session, final Attach attach)
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
        final Attach attach = createReceiverAttach(linkName);
        final Source source = new Source();

        final List<Symbol> capabilities = new ArrayList<>();
        if (isGlobal)
        {
            capabilities.add(Symbol.getSymbol("global"));
        }
        if (isShared)
        {
            capabilities.add(Symbol.getSymbol("shared"));
        }
        capabilities.add(destinationTypeCapability);

        source.setCapabilities(capabilities.toArray(new Symbol[0]));
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

    private Attach createReceiverAttach(final String linkName)
    {
        final Attach attach = new Attach();
        final Target target = new Target();
        attach.setTarget(target);
        attach.setHandle(new UnsignedInteger(_handle++));
        attach.setIncompleteUnsettled(false);
        attach.setName(linkName);
        attach.setRole(Role.RECEIVER);
        return attach;
    }

    private AMQPConnection_1_0<?> createAmqpConnection_1_0()
    {
        return createAmqpConnection_1_0(null);
    }

    private AMQPConnection_1_0<?> createAmqpConnection_1_0(final String containerId)
    {
        final AMQPConnection_1_0<?> connection = BrokerTestHelper.mockAsSystemPrincipalSource(AMQPConnection_1_0.class);
        final Subject subject = new Subject(true, Set.of(), Set.of(), Set.of());
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
        when(connection.getContextValue(Integer.class, AMQPConnection_1_0.ECHO_FLOW_COALESCE_INTERVAL_MS))
                .thenReturn(AMQPConnection_1_0.DEFAULT_ECHO_FLOW_COALESCE_INTERVAL_MS);
        when(connection.getMaxFrameSize()).thenReturn(512);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(connection.doOnIOThreadAsync(runnableCaptor.capture())).thenAnswer((Answer<CompletableFuture<Void>>) invocation ->
        {
            runnableCaptor.getValue().run();
            return CompletableFuture.completedFuture(null);
        });
        final AggregateTicker mockTicker = mock(AggregateTicker.class);
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

    private Session_1_0 createSession_1_0(final AMQPConnection_1_0<?> connection, final int channelId)
    {
        return createSession_1_0(connection, channelId, 2048);
    }

    private Session_1_0 createSession_1_0(final AMQPConnection_1_0<?> connection,
                                          final int channelId,
                                          final long incomingWindow)
    {
        final Begin begin = mock(Begin.class);
        when(begin.getNextOutgoingId()).thenReturn(new UnsignedInteger(channelId));
        return new Session_1_0(connection, begin, channelId, channelId, incomingWindow);
    }

    private void registerInputHandle(final Session_1_0 session,
                                     final UnsignedInteger handle,
                                     final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint)
            throws Exception
    {
        final Field field = Session_1_0.class.getDeclaredField("_inputHandleToEndpoint");
        field.setAccessible(true);
        final Map<UnsignedInteger, LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> inputHandleToEndpoint =
                (Map<UnsignedInteger, LinkEndpoint<? extends BaseSource, ? extends BaseTarget>>) field.get(session);
        inputHandleToEndpoint.put(handle, endpoint);
    }

    private Transfer createTransfer(final UnsignedInteger handle,
                                    final UnsignedInteger deliveryId,
                                    final byte deliveryTag)
    {
        final Transfer transfer = new Transfer();
        transfer.setHandle(handle);
        transfer.setDeliveryId(deliveryId);
        transfer.setDeliveryTag(new Binary(new byte[]{deliveryTag}));
        return transfer;
    }

    private Session_1_0 createSession_1_0(final AMQPConnection_1_0<?> connection,
                                          final int channelId,
                                          final long incomingWindow,
                                          final int echoFlowCoalesceIntervalMs,
                                          final EchoFlowTestSupport.FakeClock clock,
                                          final EchoFlowTestSupport.FakeScheduler scheduler,
                                          final java.util.concurrent.Executor executor)
    {
        final Begin begin = mock(Begin.class);
        when(begin.getNextOutgoingId()).thenReturn(new UnsignedInteger(channelId));
        return new Session_1_0(connection, begin, channelId, channelId, incomingWindow, echoFlowCoalesceIntervalMs,
                clock, scheduler, executor);
    }

    private Flow createSessionEchoFlow()
    {
        final Flow flow = new Flow();
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setOutgoingWindow(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(2048));
        flow.setEcho(Boolean.TRUE);
        return flow;
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
