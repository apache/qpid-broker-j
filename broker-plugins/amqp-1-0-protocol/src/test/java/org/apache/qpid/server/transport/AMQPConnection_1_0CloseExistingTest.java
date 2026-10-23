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
package org.apache.qpid.server.transport;

import static org.apache.qpid.server.transport.NonBlockingConnectionTestHelper.createPort;
import static org.apache.qpid.server.transport.NonBlockingConnectionTestHelper.createSocketChannel;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.protocol.v1_0.AMQPConnection_1_0Impl;
import org.apache.qpid.server.protocol.v1_0.ProtocolEngineCreator_1_0_0;
import org.apache.qpid.server.protocol.v1_0.constants.Bytes;
import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.StrongConnectionEstablishmentLimiter;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.test.utils.UnitTestBase;

class AMQPConnection_1_0CloseExistingTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 1024;

    private final List<ConnectionFixture> _connections = new ArrayList<>();
    private Broker<?> _broker;
    private AmqpPort<?> _port;

    @BeforeEach
    void setUp()
    {
        _broker = BrokerTestHelper.createNewBrokerMock();
        when(_broker.getNetworkBufferSize()).thenReturn(BUFFER_SIZE);
        when(_broker.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE))
                .thenReturn(Connection.DEFAULT_MAX_MESSAGE_SIZE);
        when(_broker.getContextValue(Boolean.class, Broker.BROKER_MSG_AUTH)).thenReturn(false);
        when(_broker.getContextValue(Integer.class, Broker.MESSAGE_COMPRESSION_THRESHOLD_SIZE)).thenReturn(0);
        _port = createPort(_broker, BUFFER_SIZE, 1000L);
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE))
                .thenReturn(Connection.DEFAULT_MAX_MESSAGE_SIZE);
        doReturn(_broker.getModel()).when(_port).getModel();
        doReturn(_broker.getTaskExecutor()).when(_port).getTaskExecutor();
        doReturn(_broker.getChildExecutor()).when(_port).getChildExecutor();
        when(_port.getContext()).thenReturn(Map.of(Connection.CLOSE_RESPONSE_TIMEOUT, "30000"));
        when(_port.getContextValue(Long.class, Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY)).thenReturn(30000L);

        final AnonymousAuthenticationManager authenticationProvider = mock(AnonymousAuthenticationManager.class);
        when(authenticationProvider.getMechanisms()).thenReturn(List.of(AnonymousAuthenticationManager.MECHANISM_NAME));
        when(authenticationProvider.getAvailableMechanisms(false))
                .thenReturn(List.of(AnonymousAuthenticationManager.MECHANISM_NAME));
        when(authenticationProvider.getAnonymousPrincipal()).thenReturn(() -> "anonymous");
        doReturn(authenticationProvider).when(_port).getAuthenticationProvider();
        final SubjectCreator subjectCreator = mock(SubjectCreator.class);
        when(_port.getSubjectCreator(anyBoolean(), isNull())).thenReturn(subjectCreator);
        when(subjectCreator.createSubjectWithGroups(any())).thenReturn(new Subject(true,
                Set.of(new AuthenticatedPrincipal(() -> "anonymous")), Set.of(), Set.of()));

        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        when(addressSpace.getId()).thenReturn(UUID.randomUUID());
        when(addressSpace.getPrincipal()).thenReturn(() -> "test");
        when(addressSpace.isActive()).thenReturn(true);
        when(addressSpace.authoriseCreateConnection(any())).thenReturn(true);
        when(_port.getAddressSpace("test")).thenReturn(addressSpace);
        final StrongConnectionEstablishmentLimiter limiter = new StrongConnectionEstablishmentLimiter();
        final Map<AMQPConnection<?>, ConnectionSlot> slots = new HashMap<>();
        doAnswer(invocation ->
        {
            final AMQPConnection<?> connection = invocation.getArgument(0);
            slots.put(connection, limiter.register(connection));
            return null;
        }).when(addressSpace).registerConnection(any());
        doAnswer(invocation ->
        {
            final ConnectionSlot slot = slots.remove(invocation.getArgument(0));
            if (slot != null)
            {
                slot.free();
            }
            return null;
        }).when(addressSpace).deregisterConnection(any());
    }

    @AfterEach
    void tearDown()
    {
        for (final ConnectionFixture connection : _connections)
        {
            connection._network.abort();
            connection._network.doWork();
        }
        _connections.clear();
    }

    @Test
    void closeExistingDeadlineAbortsBlockedConnectionAndResumesOpen() throws IOException
    {
        when(_port.getContext()).thenReturn(Map.of(Connection.CLOSE_RESPONSE_TIMEOUT, "0"));
        final ConnectionFixture existing = newConnection();
        existing.open();
        final CompletableFuture<Void> pending = existing.blockOutput();
        final ConnectionFixture replacement = newConnection();

        replacement.open();
        assertFalse(replacement._connection.isOpen());
        assertFalse(existing._connection.isClosing(), "The queued close has not run");

        assertTrue(existing._network.doWork(), "The policy must install a deadline before queuing the close");
        verify(existing._socket).close();
        assertFalse(pending.isDone(), "Backpressure must prevent queued protocol work from running");
        assertFalse(replacement._network.doWork());
        assertTrue(replacement._connection.isOpen(), "Transport closure must release the waiting open");
    }

    @Test
    void transportLossBeforeQueuedCloseResumesOpen() throws IOException
    {
        final ConnectionFixture existing = newConnection();
        existing.open();
        final CompletableFuture<Void> pending = existing.blockOutput();
        final ConnectionFixture replacement = newConnection();
        replacement.open();
        assertFalse(replacement._connection.isOpen());
        assertFalse(existing._network.doWork());
        assertFalse(existing._connection.isClosing(), "Backpressure must prevent the close task from running");

        existing._network.abort();
        assertTrue(existing._network.doWork());
        assertFalse(pending.isDone());
        assertFalse(replacement._network.doWork());
        assertTrue(replacement._connection.isOpen(), "Transport loss must complete the policy's waiting future");
    }

    @Test
    void queuedCloseResumesOpenWithoutWaitingForTransportClosure() throws IOException
    {
        final ConnectionFixture existing = newConnection();
        existing.open();
        final ConnectionFixture replacement = newConnection();
        replacement.open();
        assertFalse(replacement._connection.isOpen());

        assertFalse(existing._network.doWork());
        assertTrue(existing._connection.isClosing());
        assertFalse(existing._connection.isClosed());
        verify(existing._socket, never()).close();
        assertFalse(replacement._network.doWork());
        assertTrue(replacement._connection.isOpen());
    }

    private ConnectionFixture newConnection() throws IOException
    {
        final ConnectionFixture fixture = new ConnectionFixture(_connections.size());
        _connections.add(fixture);
        return fixture;
    }

    private final class ConnectionFixture
    {
        private final AtomicBoolean _writable = new AtomicBoolean(true);
        private final SocketChannel _socket = createSocketChannel(_writable::get);
        private final NonBlockingConnection _network;
        private AMQPConnection_1_0Impl _connection;

        private ConnectionFixture(final long id) throws IOException
        {
            final ProtocolEngineCreator creator = new ProtocolEngineCreator_1_0_0()
            {
                @Override
                public ProtocolEngine newProtocolEngine(final Broker<?> broker,
                                                        final ServerNetworkConnection networkConnection,
                                                        final AmqpPort<?> port,
                                                        final Transport transport,
                                                        final long connectionId,
                                                        final AggregateTicker ticker)
                {
                    _connection = (AMQPConnection_1_0Impl) super.newProtocolEngine(broker, networkConnection, port,
                            transport, connectionId, ticker);
                    return _connection;
                }
            };
            final MultiVersionProtocolEngine engine = new MultiVersionProtocolEngine(_broker,
                    Set.of(Protocol.AMQP_1_0), Protocol.AMQP_1_0, _port, Transport.TCP, id,
                    new ProtocolEngineCreator[]{creator}, null);
            _network = new NonBlockingConnection(_socket, engine, Set.of(TransportEncryption.NONE), () -> { },
                    mock(NetworkConnectionScheduler.class), _port);
            _network.setSelectionTask(mock(SelectorThread.SelectionTask.class));
            engine.setNetworkConnection(_network);
            try (final QpidByteBuffer header = QpidByteBuffer.wrap(Bytes.amqpHeader()))
            {
                engine.received(header);
            }
        }

        private void open()
        {
            final Open open = new Open();
            open.setContainerId("shared-container");
            open.setHostname("test");
            open.setDesiredCapabilities(new Symbol[]{Symbols.SOLE_CONNECTION_FOR_CONTAINER});
            open.setProperties(Map.of(Symbols.SOLE_CONNECTION_ENFORCEMENT_POLICY,
                    SoleConnectionEnforcementPolicy.CLOSE_EXISTING.getValue()));
            _connection.receiveOpen(0, open);
        }

        private CompletableFuture<Void> blockOutput()
        {
            assertTrue(_connection.isOpen());
            _writable.set(false);
            try (final QpidByteBuffer output = QpidByteBuffer.wrap(new byte[2 * BUFFER_SIZE]))
            {
                _network.send(output);
            }
            final CompletableFuture<Void> pending = _connection.doOnIOThreadAsync(() -> { });
            assertFalse(_network.doWork());
            assertTrue(_network.wantsWrite());
            assertFalse(pending.isDone());
            return pending;
        }
    }
}
