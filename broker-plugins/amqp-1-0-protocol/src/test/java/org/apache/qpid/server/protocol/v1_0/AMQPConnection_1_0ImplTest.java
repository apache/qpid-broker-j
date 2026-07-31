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

import static org.apache.qpid.server.protocol.v1_0.constants.Constants.MIN_MAX_FRAME_SIZE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class AMQPConnection_1_0ImplTest extends UnitTestBase
{
    private Broker<?> _broker;
    private ServerNetworkConnection _network;
    private AmqpPort<?> _port;
    private AggregateTicker _aggregateTicket;
    private QueueManagingVirtualHost<?> _virtualHost;

    @BeforeAll
    void setUp() throws Exception
    {
        _broker = BrokerTestHelper.createBrokerMock();
        final Model model = _broker.getModel();
        final TaskExecutor taskExecutor = _broker.getTaskExecutor();
        _network = mock(ServerNetworkConnection.class);
        when(_network.getLocalAddress()).thenReturn(mock(SocketAddress.class));
        _port = mock(AmqpPort.class);
        when(_port.getModel()).thenReturn(model);
        when(_port.getTaskExecutor()).thenReturn(taskExecutor);
        when(_port.getChildExecutor()).thenReturn(taskExecutor);
        _aggregateTicket = new AggregateTicker();
        _virtualHost = BrokerTestHelper.createVirtualHost("test", _broker, true, this);
    }

    @Test
    void getOpenTransactions()
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        final IdentifiedTransaction tx1 = connection.createIdentifiedTransaction();
        final IdentifiedTransaction tx2 = connection.createIdentifiedTransaction();

        final Iterator<ServerTransaction> iterator = connection.getOpenTransactions();

        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(equalTo(tx1.getServerTransaction())));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(equalTo(tx2.getServerTransaction())));
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    void createIdentifiedTransaction()
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        final IdentifiedTransaction tx1 = connection.createIdentifiedTransaction();
        connection.createIdentifiedTransaction();

        connection.removeTransaction(tx1.getId());

        final IdentifiedTransaction tx3 = connection.createIdentifiedTransaction();
        assertThat(tx1.getId(), is(equalTo(tx3.getId())));
    }

    @Test
    void getTransaction()
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        final IdentifiedTransaction tx1 = connection.createIdentifiedTransaction();
        final IdentifiedTransaction tx2 = connection.createIdentifiedTransaction();

        final ServerTransaction serverTransaction1 = connection.getTransaction(tx1.getId());
        assertThat(tx1.getServerTransaction(), is(equalTo(serverTransaction1)));

        final ServerTransaction serverTransaction2 = connection.getTransaction(tx2.getId());
        assertThat(tx2.getServerTransaction(), is(equalTo(serverTransaction2)));
    }

    @Test
    void getTransactionUnknownId()
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        final IdentifiedTransaction tx1 = connection.createIdentifiedTransaction();

        assertThrows(UnknownTransactionException.class,
                () -> connection.getTransaction(tx1.getId() + 1),
                "UnknownTransactionException is not thrown");
    }

    @Test
    void removeTransaction()
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        final IdentifiedTransaction tx1 = connection.createIdentifiedTransaction();
        connection.removeTransaction(tx1.getId());

        assertThrows(UnknownTransactionException.class,
                () -> connection.getTransaction(tx1.getId()),
                "UnknownTransactionException is not thrown");
    }

    @Test
    void resetStatistics()
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        connection.registerMessageReceived(100L);
        connection.registerMessageDelivered(100L);
        connection.registerTransactedMessageReceived();
        connection.registerTransactedMessageDelivered();

        final Map<String, Object> statisticsBeforeReset = connection.getStatistics();
        assertEquals(100L, statisticsBeforeReset.get("bytesIn"));
        assertEquals(100L, statisticsBeforeReset.get("bytesOut"));
        assertEquals(1L, statisticsBeforeReset.get("messagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("messagesOut"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesOut"));

        connection.resetStatistics();

        final Map<String, Object> statisticsAfterReset = connection.getStatistics();
        assertEquals(0L, statisticsAfterReset.get("bytesIn"));
        assertEquals(0L, statisticsAfterReset.get("bytesOut"));
        assertEquals(0L, statisticsAfterReset.get("messagesIn"));
        assertEquals(0L, statisticsAfterReset.get("messagesOut"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesIn"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesOut"));
    }

    @Test
    void heartbeat() throws Exception
    {
        final int writeDelay = 5000;
        final int readDelay = 12;
        final AtomicLong maxReadIdleMillis = new AtomicLong();
        final AtomicLong maxWriteIdleMillis = new AtomicLong();

        when(_broker.getNetworkBufferSize()).thenReturn(1024);

        final ByteBufferSender sender = mock(ByteBufferSender.class);
        when(sender.isDirectBufferPreferred()).thenReturn(true);
        when(_network.getSender()).thenReturn(sender);
        doAnswer(invocation ->
        {
            maxReadIdleMillis.set(invocation.getArgument(0));
            return null;
        }).when(_network).setMaxReadIdleMillis(anyLong());
        doAnswer(invocation ->
        {
            maxWriteIdleMillis.set(invocation.getArgument(0));
            return null;
        }).when(_network).setMaxWriteIdleMillis(anyLong());

        when(_port.getNetworkBufferSize()).thenReturn(1024);
        when(_port.getHeartbeatDelay()).thenReturn(readDelay);
        when(_port.getAddressSpace("localhost")).thenReturn(_virtualHost);

        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);

        setConnectionState(connection, ConnectionState.AWAIT_OPEN);

        final Open open = mock(Open.class);
        when(open.getContainerId()).thenReturn("container");
        when(open.getHostname()).thenReturn("localhost");
        when(open.getIdleTimeOut()).thenReturn(UnsignedInteger.valueOf(writeDelay));

        connection.receiveOpen(1, open);

        assertEquals(readDelay * 2 * 1000, maxReadIdleMillis.get());
        assertEquals(writeDelay / 2, maxWriteIdleMillis.get());
    }

    @Test
    void defaultTransferLimitAccommodatesDefaultMaxMessageSizeAtMinimumFrameSize()
    {
        final int conservativeMinimumFramePayload = 400;

        assertThat((long) AMQPConnection_1_0.DEFAULT_MAX_TRANSFERS_PER_DELIVERY * conservativeMinimumFramePayload >=
                Connection.DEFAULT_MAX_MESSAGE_SIZE, is(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 256, 511})
    void receiveOpenRejectsMaxFrameSizeBelowMinimum(final int maxFrameSize) throws Exception
    {
        stubNetworkAndPort();

        final AMQPConnection_1_0Impl connection = createConnectionAwaitingOpen();
        connection.receiveOpen(1, createOpen(UnsignedInteger.valueOf(maxFrameSize)));

        assertTrue(connection.isClosed(),
                "Connection should be closed when max-frame-size " + maxFrameSize + " < " + MIN_MAX_FRAME_SIZE);
    }

    @Test
    void receiveOpenAcceptsMaxFrameSizeAtMinimum() throws Exception
    {
        stubNetworkAndPort();
        stubAddressSpaceResolution();

        final AMQPConnection_1_0Impl connection = createConnectionAwaitingOpen();
        connection.receiveOpen(1, createOpen(UnsignedInteger.valueOf(MIN_MAX_FRAME_SIZE)));

        assertEquals(MIN_MAX_FRAME_SIZE, connection.getMaxFrameSize(),
                "max-frame-size should be accepted when equal to the AMQP minimum of " + MIN_MAX_FRAME_SIZE);
    }

    @Test
    void receiveOpenAcceptsNullMaxFrameSize() throws Exception
    {
        stubNetworkAndPort();
        stubAddressSpaceResolution();

        final AMQPConnection_1_0Impl connection = createConnectionAwaitingOpen();
        connection.receiveOpen(1, createOpen(null));

        assertEquals(1024, connection.getMaxFrameSize(),
                "max-frame-size should default to broker buffer size when null");
    }

    private void stubNetworkAndPort()
    {
        when(_broker.getNetworkBufferSize()).thenReturn(1024);

        final ByteBufferSender sender = mock(ByteBufferSender.class);
        when(sender.isDirectBufferPreferred()).thenReturn(true);
        when(_network.getSender()).thenReturn(sender);

        when(_port.getNetworkBufferSize()).thenReturn(1024);
    }

    private void stubAddressSpaceResolution()
    {
        when(_port.getHeartbeatDelay()).thenReturn(0);
        when(_port.getAddressSpace("localhost")).thenReturn(_virtualHost);
    }

    private AMQPConnection_1_0Impl createConnectionAwaitingOpen() throws Exception
    {
        final AMQPConnection_1_0Impl connection =
                new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        setConnectionState(connection, ConnectionState.AWAIT_OPEN);
        return connection;
    }

    private Open createOpen(final UnsignedInteger maxFrameSize)
    {
        final Open open = mock(Open.class);
        when(open.getContainerId()).thenReturn("container");
        when(open.getHostname()).thenReturn("localhost");
        when(open.getMaxFrameSize()).thenReturn(maxFrameSize);
        return open;
    }

    private void setConnectionState(final AMQPConnection_1_0Impl connection,
                                    final ConnectionState state) throws Exception
    {
        final Field field = AMQPConnection_1_0Impl.class.getDeclaredField("_connectionState");
        field.setAccessible(true);
        field.set(connection, state);
    }
}
