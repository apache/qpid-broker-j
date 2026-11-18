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
package org.apache.qpid.server.transport.websocket.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.websocket.api.WebSocketContainer;
import org.eclipse.jetty.websocket.common.JettyWebSocketFrameHandler;
import org.eclipse.jetty.websocket.common.JettyWebSocketFrameHandlerFactory;
import org.eclipse.jetty.websocket.core.Behavior;
import org.eclipse.jetty.websocket.core.CloseStatus;
import org.eclipse.jetty.websocket.core.ExtensionStack;
import org.eclipse.jetty.websocket.core.Frame;
import org.eclipse.jetty.websocket.core.Negotiated;
import org.eclipse.jetty.websocket.core.WebSocketComponents;
import org.eclipse.jetty.websocket.core.WebSocketCoreSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.transport.ServerNetworkConnection;

class WebSocketTransportCloseTest extends WebSocketTestBase
{
    private final AtomicBoolean _transportOpen = new AtomicBoolean(true);

    private MultiVersionProtocolEngine _protocolEngine;
    private WebSocketComponents _components;
    private WebSocketCoreSession _coreSession;
    private EndPoint _transport;
    private org.eclipse.jetty.websocket.core.WebSocketConnection _jettyConnection;
    private WebSocketConnection _connection;
    private Callback _pendingWrite;

    @BeforeEach
    void setUp()
    {
        _transportOpen.set(true);
        _pendingWrite = null;
        _protocolEngine = mock();
        when(_protocolEngine.getAggregateTicker()).thenReturn(new AggregateTicker());
        final MultiVersionProtocolEngineFactory factory = mock();
        when(factory.newProtocolEngine(any(SocketAddress.class))).thenReturn(_protocolEngine);
        final AmqpWebSocket endpoint = new AmqpWebSocket(factory, _threadPool, _connectionScheduler, SETTINGS, null);
        final WebSocketContainer container = mock();
        _components = new WebSocketComponents(null, null, ByteBufferPool.NON_POOLING, null, null, Runnable::run);
        final JettyWebSocketFrameHandlerFactory handlerFactory =
                new JettyWebSocketFrameHandlerFactory(container, _components);
        final JettyWebSocketFrameHandler handler =
                new JettyWebSocketFrameHandler(container, endpoint, handlerFactory.getMetadata(endpoint.getClass()));
        final ExtensionStack extensions = new ExtensionStack(_components, Behavior.SERVER);
        extensions.negotiate(List.of(), List.of());
        _coreSession = new WebSocketCoreSession(handler, Behavior.SERVER, Negotiated.from(extensions), _components);
        _transport = mock();
        when(_transport.isOpen()).thenAnswer(invocation -> _transportOpen.get());
        when(_transport.getLocalSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10000));
        when(_transport.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10001));
        doAnswer(invocation ->
        {
            closeTransport();
            return null;
        }).when(_transport).close();
        doAnswer(invocation ->
        {
            closeTransport();
            return null;
        }).when(_transport).close(any(Throwable.class));
        doAnswer(invocation ->
        {
            _pendingWrite = invocation.getArgument(0);
            return null;
        }).when(_transport).write(any(Callback.class), any(ByteBuffer[].class));
        _jettyConnection = new org.eclipse.jetty.websocket.core.WebSocketConnection(
                _transport, Runnable::run, mock(Scheduler.class), ByteBufferPool.NON_POOLING, _coreSession);
        _coreSession.setWebSocketConnection(_jettyConnection);
        _coreSession.onOpen();
        final ArgumentCaptor<ServerNetworkConnection> connectionCaptor = ArgumentCaptor.captor();
        verify(_protocolEngine).setNetworkConnection(connectionCaptor.capture());
        _connection = (WebSocketConnection) connectionCaptor.getValue();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        _coreSession.abort();
        _connectionScheduler.shutdown();
        _components.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void incomingCloseCannotDisableSharedDeadline(final boolean pendingBinaryWrite)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        if (pendingBinaryWrite)
        {
            send(_connection, new byte[] {1});
            _connection.doWrite();
            _threadPool.runAll();
            assertNotNull(_pendingWrite);
        }

        final Callback closeCallback = receiveClose();

        assertFalse(_connection.isClosed());
        assertTrue(_connection.isClosing());
        assertTrue(_transport.isOpen());
        assertTrue(_coreSession.isClosed());
        verify(closeCallback, never()).succeeded();
        verify(_transport).write(any(Callback.class), any(ByteBuffer[].class));
        assertEquals(500L, scheduleDueConnections());
        _threadPool.runAll();
        verify(_protocolEngine, never()).closed();

        _currentTime.addAndGet(499L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(499L));
        deadline.setTimeToNextTick(1);
        assertEquals(1L, scheduleDueConnections());
        assertTrue(_transport.isOpen());

        _currentTime.incrementAndGet();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
        deadline.setTimeToNextTick(0);
        scheduleDueConnections();
        _threadPool.runAll();

        assertFalse(_transport.isOpen());
        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    @Test
    void incomingCloseWaitsForTransportClosureBeforeCleanup()
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        send(_connection, new byte[] {1});
        _connection.doWrite();
        _threadPool.runAll();
        receiveClose();

        assertEquals(500L, scheduleDueConnections());
        _threadPool.runAll();
        verify(_protocolEngine, never()).closed();
        _pendingWrite.succeeded();
        verify(_transport, times(2)).write(any(Callback.class), any(ByteBuffer[].class));
        assertTrue(_transport.isOpen());
        assertFalse(_connection.isClosed());

        _pendingWrite.succeeded();
        assertFalse(_transport.isOpen());
        assertTrue(_connection.isClosed());
        scheduleDueConnections();
        _threadPool.runAll();
        verify(_protocolEngine).closed();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    @Test
    void incomingCloseDoesNotRestartFinalWriteTimeout()
    {
        _connection.close();
        _currentTime.addAndGet(750L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(750L));

        receiveClose();

        assertEquals(250L, scheduleDueConnections());
        _threadPool.runAll();
        assertTrue(_transport.isOpen());
        verify(_protocolEngine, never()).closed();

        _currentTime.addAndGet(250L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(250L));
        scheduleDueConnections();
        _threadPool.runAll();

        assertFalse(_transport.isOpen());
        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
    }

    @Test
    void transportClosureWithoutCloseFrameStillCompletesCleanup()
    {
        closeTransport();

        assertTrue(_connection.isClosed());
        scheduleDueConnections();
        _threadPool.runAll();
        verify(_protocolEngine).closed();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    private Callback receiveClose()
    {
        final Frame close = new CloseStatus(CloseStatus.NORMAL).toFrame().setMask(new byte[] {1, 2, 3, 4});
        final Callback callback = mock();
        _coreSession.onFrame(close, callback);
        return callback;
    }

    private void closeTransport()
    {
        if (_transportOpen.compareAndSet(true, false))
        {
            _jettyConnection.onClose(null);
        }
    }
}
