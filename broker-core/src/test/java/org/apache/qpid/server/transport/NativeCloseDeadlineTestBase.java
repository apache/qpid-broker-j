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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.test.utils.UnitTestBase;

public abstract class NativeCloseDeadlineTestBase extends UnitTestBase
{
    protected static final int NETWORK_BUFFER_SIZE = 1024;

    protected Broker<?> _broker;
    protected AmqpPort<?> _port;
    protected NonBlockingConnection _network;
    protected AggregateTicker _ticker;

    private final AtomicBoolean _writable = new AtomicBoolean();
    private SocketChannel _socket;
    private TaskExecutor _taskExecutor;
    private MultiVersionProtocolEngine _engine;

    @BeforeEach
    void setUpNativeTransport() throws Exception
    {
        _writable.set(false);
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final SystemConfig<?> systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(new EventLogger());
        _broker = mock(Broker.class);
        doReturn(systemConfig).when(_broker).getParent();
        doReturn(Broker.class).when(_broker).getCategoryClass();
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getNetworkBufferSize()).thenReturn(NETWORK_BUFFER_SIZE);

        _port = createPort(_broker, NETWORK_BUFFER_SIZE, 0L);
        doReturn(Port.class).when(_port).getCategoryClass();
        when(_port.getModel()).thenReturn(BrokerModel.getInstance());
        when(_port.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_port.getChildExecutor()).thenReturn(_taskExecutor);
        setCloseTimeout(30_000);
        final AuthenticationProvider<?> authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getAvailableMechanisms(anyBoolean())).thenReturn(List.of("PLAIN"));
        doReturn(authenticationProvider).when(_port).getAuthenticationProvider();

        _socket = createSocketChannel(_writable::get);
        _engine = new MultiVersionProtocolEngine(_broker, Set.of(), null, _port, Transport.TCP, 1L,
                new ProtocolEngineCreator[0], null);
        _ticker = _engine.getAggregateTicker();
        _network = new NonBlockingConnection(_socket, _engine, Set.of(TransportEncryption.NONE), () -> { },
                mock(NetworkConnectionScheduler.class), _port);
        _network.setSelectionTask(mock(SelectorThread.SelectionTask.class));
    }

    @AfterEach
    void tearDownNativeTransport()
    {
        try
        {
            if (_network != null)
            {
                _writable.set(true);
                _network.close();
                _network.doWork();
            }
        }
        finally
        {
            _taskExecutor.stop();
        }
    }

    protected abstract AMQPConnection<?> createConnection(Protocol protocol) throws Exception;

    protected abstract void receivePeerClose(AMQPConnection<?> connection) throws Exception;

    protected abstract Stream<Protocol> protocols();

    protected void setCloseTimeout(final long timeoutMillis)
    {
        when(_port.getContext()).thenReturn(Map.of(Connection.CLOSE_RESPONSE_TIMEOUT, Long.toString(timeoutMillis)));
    }

    protected Stream<Arguments> protocolsAndSizes()
    {
        return protocols().flatMap(protocol -> Stream.of(NETWORK_BUFFER_SIZE - 1, NETWORK_BUFFER_SIZE,
                2 * NETWORK_BUFFER_SIZE).map(size -> Arguments.of(protocol, size)));
    }

    protected void installProtocol(final ProtocolEngine protocol) throws Exception
    {
        final Field delegate = MultiVersionProtocolEngine.class.getDeclaredField("_delegate");
        delegate.setAccessible(true);
        delegate.set(_engine, protocol);
        final Field listener = MultiVersionProtocolEngine.class.getDeclaredField("_workListener");
        listener.setAccessible(true);
        protocol.setWorkListener(((AtomicReference<Action<ProtocolEngine>>) listener.get(_engine)).get());
    }

    @ParameterizedTest
    @MethodSource("protocolsAndSizes")
    protected void testRequestedCloseHasDeadlineWithBlockedOutput(final Protocol protocol, final int bufferedBytes)
            throws Exception
    {
        setCloseTimeout(0);
        final AMQPConnection<?> connection = createConnection(protocol);
        fillOutput(bufferedBytes);
        connection.sendConnectionCloseAsync(AMQPConnection.CloseReason.MANAGEMENT, "close");

        assertTrue(_network.doWork(), protocol + ": requested close remained pending with " + bufferedBytes +
                " buffered bytes; next ticker=" + _ticker.getTimeToNextTick(System.currentTimeMillis()));
        verify(_socket).close();
    }

    @ParameterizedTest
    @MethodSource("protocols")
    protected void testCloseResumesAfterSocketDrains(final Protocol protocol) throws Exception
    {
        final AMQPConnection<?> connection = createConnection(protocol);
        fillOutput(NETWORK_BUFFER_SIZE);
        connection.sendConnectionCloseAsync(AMQPConnection.CloseReason.MANAGEMENT, "close");
        assertFalse(_network.doWork());
        verify(_socket, never()).close();

        _writable.set(true);
        assertFalse(_network.doWork());
        assertTrue(_ticker.getTimeToNextTick(System.currentTimeMillis()) < Integer.MAX_VALUE,
                "Draining the socket must retain the deadline while awaiting the peer's close response");
        assertTrue(connection.isClosing(), "The protocol close must be sent when output can drain");
        verify(_socket, never()).close();
    }

    @ParameterizedTest
    @MethodSource("protocols")
    protected void testPeerCloseIsBoundedWithBlockedOutput(final Protocol protocol) throws Exception
    {
        setCloseTimeout(0);
        final AMQPConnection<?> connection = createConnection(protocol);
        fillOutput(128);
        _engine.setIOThread(Thread.currentThread());
        try
        {
            receivePeerClose(connection);
        }
        finally
        {
            _engine.setIOThread(null);
        }
        assertEquals(1, getTickers().size(), "Peer close must start the same bounded closing phase");
        assertTrue(_network.doWork(), protocol + ": peer close did not reach native shutdown while output was paused");
        verify(_socket).close();
    }

    @ParameterizedTest
    @MethodSource("protocols")
    protected void testRepeatedCloseKeepsOriginalDeadline(final Protocol protocol) throws Exception
    {
        final AMQPConnection<?> connection = createConnection(protocol);
        fillOutput(NETWORK_BUFFER_SIZE);
        IntStream.range(0, 32).parallel().forEach(i ->
                connection.sendConnectionCloseAsync(AMQPConnection.CloseReason.MANAGEMENT, "First close"));
        final List<Ticker> tickers = getTickers();
        assertEquals(1, tickers.size(), "Closing must arm a deadline before any queued task runs");
        final Ticker original = tickers.get(0);

        IntStream.range(0, 32).parallel().forEach(i ->
                connection.sendConnectionCloseAsync(AMQPConnection.CloseReason.MANAGEMENT, "Repeated close"));
        _writable.set(true);
        _network.doWork();

        assertEquals(1, tickers.size(), "Repeated requests and the close task must not add another timer");
        assertSame(original, tickers.get(0));
    }

    private List<Ticker> getTickers() throws ReflectiveOperationException
    {
        final Field tickers = AggregateTicker.class.getDeclaredField("_tickers");
        tickers.setAccessible(true);
        return (List<Ticker>) tickers.get(_ticker);
    }

    private void fillOutput(final int size)
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[size]))
        {
            _network.send(buffer);
        }
        assertFalse(_network.doWork());
        assertTrue(_network.wantsWrite(), "The native socket must retain unwritten output");
        assertEquals(Integer.MAX_VALUE, _ticker.getTimeToNextTick(System.currentTimeMillis()));
    }
}
