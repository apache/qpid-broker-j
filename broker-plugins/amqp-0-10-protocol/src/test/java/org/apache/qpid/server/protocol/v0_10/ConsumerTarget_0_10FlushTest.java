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
package org.apache.qpid.server.protocol.v0_10;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageFlowMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageFlush;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Option;
import org.apache.qpid.server.store.StoredMemoryMessage;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class ConsumerTarget_0_10FlushTest extends UnitTestBase
{
    private static final int MESSAGE_SIZE = 10;

    private QueueManagingVirtualHost<?> _host;
    private Queue<?> _queue;
    private AMQPConnection_0_10 _connection;
    private ServerSession _session;
    private ConsumerTarget_0_10 _target;
    private final List<MessageTransfer> _transfers = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception
    {
        _transfers.clear();
        _target = null;
        _host = BrokerTestHelper.createVirtualHost(getTestName(), this);
        _queue = _host.createChild(Queue.class, Map.of(Queue.NAME, "queue"));
        _connection = mock(AMQPConnection_0_10.class);
        when(_connection.isIOThread()).thenReturn(true);
        when(_connection.getAggregateTicker()).thenReturn(new AggregateTicker());
        when(_connection.getContextValue(Long.class, Consumer.SUSPEND_NOTIFICATION_PERIOD)).thenReturn(0L);
        doAnswer(invocation ->
        {
            ((Runnable) invocation.getArgument(0)).run();
            return CompletableFuture.completedFuture(null);
        }).when(_connection).doOnIOThreadAsync(any(Runnable.class));

        _session = mock(ServerSession.class);
        when(_session.getAMQPConnection()).thenReturn(_connection);
        doReturn(_host).when(_session).getAddressSpace();
        final Session_0_10 model = mock(Session_0_10.class);
        when(model.getAMQPConnection()).thenReturn(_connection);
        when(_session.getModelObject()).thenReturn(model);
        final ServerConnection connection = mock(ServerConnection.class);
        when(connection.getConnectionDelegate()).thenReturn(mock(ServerConnectionDelegate.class));
        when(_session.getConnection()).thenReturn(connection);
        doAnswer(invocation ->
        {
            _transfers.add(invocation.getArgument(0));
            return null;
        }).when(_session).sendMessage(any(MessageTransfer.class), any(Runnable.class));
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            if (_target != null)
            {
                _target.close();
            }
        }
        finally
        {
            _host.close();
        }
    }

    @ParameterizedTest(name = "{0}, blocked={1}, queued={2}, bytes={3}, messages={4}, delivered={5}")
    @MethodSource("flushCases")
    void flushDeliversAvailableMessagesWithinCredit(final MessageFlowMode flowMode,
                                                    final boolean transportBlocked,
                                                    final int queued,
                                                    final long byteCredit,
                                                    final long messageCredit,
                                                    final int expectedDeliveries) throws Exception
    {
        when(_connection.isTransportBlockedForWriting()).thenReturn(transportBlocked);
        createTarget(flowMode, byteCredit, messageCredit);
        enqueueMessages(queued);
        final MessageFlush flush = new MessageFlush("consumer", Option.SYNC);

        new ServerSessionDelegate().command(_session, flush);

        assertEquals(expectedDeliveries, _transfers.size());
        assertEquals(queued - expectedDeliveries, _queue.getAvailableMessages());
        assertFalse(_target.getCreditManager().hasCredit(), "Flush must clear remaining credit");
        assertTrue(_target.isSuspended());
        final InOrder order = inOrder(_session);
        if (expectedDeliveries > 0)
        {
            order.verify(_session, times(expectedDeliveries)).sendMessage(any(MessageTransfer.class), any(Runnable.class));
        }
        else
        {
            verify(_session, never()).sendMessage(any(MessageTransfer.class), any(Runnable.class));
        }
        order.verify(_session).processed(flush);
        order.verify(_session).flushProcessed();

        _target.addCredit(MessageCreditUnit.MESSAGE, 10L);
        _target.addCredit(MessageCreditUnit.BYTE, FlowCreditManager_0_10.INFINITE_CREDIT);
        assertEquals(transportBlocked, _target.isSuspended(),
                "New credit must still respect transport backpressure after the flush");
        verify(_connection, never()).setTransportBlockedForWriting(anyBoolean());
    }

    @ParameterizedTest
    @EnumSource(MessageFlowMode.class)
    void failedFlushRestoresTransportBackpressure(final MessageFlowMode flowMode) throws Exception
    {
        when(_connection.isTransportBlockedForWriting()).thenReturn(true);
        createTarget(flowMode, -1L, 2L);
        enqueueMessages(1);
        final ConnectionScopedRuntimeException failure = new ConnectionScopedRuntimeException("Delivery failed");
        doThrow(failure).when(_session).sendMessage(any(MessageTransfer.class), any(Runnable.class));

        assertSame(failure, assertThrows(ConnectionScopedRuntimeException.class, _target::flush));

        assertTrue(_target.getCreditManager().hasCredit());
        _target.updateNotifyWorkDesired();
        assertTrue(_target.isSuspended(), "A failed flush must not leave the consumer ignoring backpressure");
        when(_connection.isTransportBlockedForWriting()).thenReturn(false);
        _target.transportStateChanged();
        assertFalse(_target.isSuspended());
        verify(_connection, never()).setTransportBlockedForWriting(anyBoolean());
    }

    private static Stream<Arguments> flushCases()
    {
        final List<Arguments> cases = new ArrayList<>();
        for (final MessageFlowMode flowMode : MessageFlowMode.values())
        {
            for (final boolean blocked : new boolean[]{false, true})
            {
                cases.add(Arguments.of(flowMode, blocked, 3, -1L, 2L, 2));
                cases.add(Arguments.of(flowMode, blocked, 3, 2L * MESSAGE_SIZE, 3L, 2));
                cases.add(Arguments.of(flowMode, blocked, 1, -1L, 0L, 0));
                cases.add(Arguments.of(flowMode, blocked, 1, 0L, 3L, 0));
                cases.add(Arguments.of(flowMode, blocked, 0, -1L, 2L, 0));
                cases.add(Arguments.of(flowMode, blocked, 1, -1L, 2L, 1));
            }
        }
        return cases.stream();
    }

    private void createTarget(final MessageFlowMode flowMode,
                              final long byteCredit,
                              final long messageCredit) throws Exception
    {
        final FlowCreditManager_0_10 creditManager = flowMode == MessageFlowMode.CREDIT
                ? new CreditCreditManager(byteCredit, messageCredit)
                : new WindowCreditManager(byteCredit, messageCredit);
        _target = new ConsumerTarget_0_10(_session, "consumer", MessageAcceptMode.EXPLICIT,
                MessageAcquireMode.PRE_ACQUIRED, flowMode, creditManager, Map.of(), false);
        when(_session.getSubscription("consumer")).thenReturn(_target);
        _queue.addConsumer(_target, null, MessageTransferMessage.class, "consumer",
                EnumSet.of(ConsumerOption.ACQUIRES), null);
        _target.updateNotifyWorkDesired();
    }

    private void enqueueMessages(final int count)
    {
        for (int i = 0; i < count; i++)
        {
            final Header header = new Header(new DeliveryProperties(), new MessageProperties());
            final long timeMillis = System.currentTimeMillis();
            final MessageMetaData_0_10 metaData = new MessageMetaData_0_10(header, MESSAGE_SIZE, timeMillis);
            final StoredMemoryMessage<MessageMetaData_0_10> stored = new StoredMemoryMessage<>(i + 1L, metaData);
            try (final QpidByteBuffer content = QpidByteBuffer.wrap(new byte[MESSAGE_SIZE]))
            {
                stored.addContent(content);
            }
            _queue.enqueue(new MessageTransferMessage(stored, null), null, null);
        }
    }
}
