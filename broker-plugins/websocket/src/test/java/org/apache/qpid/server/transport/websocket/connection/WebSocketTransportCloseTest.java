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
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.SslConnection.SslEndPoint;
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
import org.eclipse.jetty.websocket.core.OutgoingEntry;
import org.eclipse.jetty.websocket.core.WebSocketComponents;
import org.eclipse.jetty.websocket.core.WebSocketCoreSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.websocket.connection.WebSocketServerConnector.WebSocketEndPoint;

@ParameterizedClass(name = "TLS={0}")
@ValueSource(booleans = {false, true})
class WebSocketTransportCloseTest extends WebSocketTestBase
{
    private final AtomicBoolean _transportOpen = new AtomicBoolean(true);

    @Parameter
    private boolean _tls;

    private MultiVersionProtocolEngine _protocolEngine;
    private WebSocketComponents _components;
    private WebSocketCoreSession _coreSession;
    private WebSocketEndPoint _transport;
    private org.eclipse.jetty.websocket.core.WebSocketConnection _jettyConnection;
    private WebSocketConnection _connection;
    private QpidByteBuffer _inputBuffer;
    private Callback _pendingWrite;
    private boolean _receiveCloseBeforeSend;
    private Consumer<Runnable> _submitFrame;

    private enum CloseWriteOutcome
    {
        FLUSHED, FAILED, DISCONNECTED, TIMED_OUT
    }

    @BeforeEach
    void setUp()
    {
        _transportOpen.set(true);
        _pendingWrite = null;
        _submitFrame = Runnable::run;
        _protocolEngine = mock();
        when(_protocolEngine.getAggregateTicker()).thenReturn(new AggregateTicker());
        _connectionScheduler = spy(_connectionScheduler);
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
        _coreSession = new DeadlineWebSocketCoreSession(handler, Negotiated.from(extensions), _components,
                Runnable::run)
        {
            @Override
            public void sendFrame(final OutgoingEntry entry)
            {
                if (_receiveCloseBeforeSend)
                {
                    _receiveCloseBeforeSend = false;
                    receiveClose();
                }
                _submitFrame.accept(() -> super.sendFrame(entry));
            }
        };
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
                _tls ? tlsEndPoint() : _transport, Runnable::run, mock(Scheduler.class), ByteBufferPool.NON_POOLING,
                _coreSession);
        _coreSession.setWebSocketConnection(_jettyConnection);
        _inputBuffer = mock();
        try (final MockedStatic<QpidByteBuffer> buffers = mockStatic(QpidByteBuffer.class))
        {
            buffers.when(() -> QpidByteBuffer.allocateDirect(SETTINGS.getBufferSize())).thenReturn(_inputBuffer);
            _coreSession.onOpen();
            buffers.verify(() -> QpidByteBuffer.allocateDirect(SETTINGS.getBufferSize()));
        }
        final ArgumentCaptor<ServerNetworkConnection> connectionCaptor = ArgumentCaptor.captor();
        verify(_protocolEngine).setNetworkConnection(connectionCaptor.capture());
        _connection = (WebSocketConnection) connectionCaptor.getValue();
    }

    private EndPoint tlsEndPoint()
    {
        final SslEndPoint endPoint = mock();
        when(endPoint.unwrap()).thenReturn(_transport);
        when(endPoint.isOpen()).thenAnswer(invocation -> _transport.isOpen());
        when(endPoint.getLocalSocketAddress()).thenAnswer(invocation -> _transport.getLocalSocketAddress());
        when(endPoint.getRemoteSocketAddress()).thenAnswer(invocation -> _transport.getRemoteSocketAddress());
        doAnswer(invocation ->
        {
            _transport.close();
            return null;
        }).when(endPoint).close();
        doAnswer(invocation ->
        {
            _transport.shutdownOutput();
            return null;
        }).when(endPoint).shutdownOutput();
        doAnswer(invocation ->
        {
            _transport.write(invocation.getArgument(0), (ByteBuffer[]) invocation.getRawArguments()[1]);
            return null;
        }).when(endPoint).write(any(Callback.class), any(ByteBuffer[].class));
        return endPoint;
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void jettyCloseEnforcesEarlierDeadlineDuringPendingWrite(final boolean earlierAmqpDeadline)
    {
        final int expectedTimeout = earlierAmqpDeadline ? 250 : 1000;
        final MutableTicker amqpDeadline = new MutableTicker(expectedTimeout);
        if (earlierAmqpDeadline)
        {
            _connection.setCloseDeadline(amqpDeadline);
        }
        send(_connection, new byte[] {1});
        _connection.doWrite();
        _threadPool.runAll();
        assertNotNull(_pendingWrite);

        final QpidByteBuffer queued = mock();
        final QpidByteBuffer duplicate = mock();
        when(queued.remaining()).thenReturn(1);
        when(queued.duplicate()).thenReturn(duplicate);
        _connection.send(queued);

        final Callback close = mock();
        _coreSession.close(new CloseStatus(CloseStatus.NORMAL), close);
        assertTrue(_connection.isClosing());
        assertEquals(expectedTimeout, scheduleDueConnections());
        verify(_transport).write(any(Callback.class), any(ByteBuffer[].class));
        verify(close, never()).succeeded();

        _currentTime.addAndGet(expectedTimeout - 1L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(expectedTimeout - 1L));
        amqpDeadline.setTimeToNextTick(1);
        assertEquals(1L, scheduleDueConnections());
        verify(_transport, never()).closeSocket();
        verify(_protocolEngine, never()).closed();

        _currentTime.incrementAndGet();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
        amqpDeadline.setTimeToNextTick(0);
        scheduleDueConnections();
        verify(_transport).closeSocket();
        _threadPool.runAll();

        assertFalse(_transport.isOpen());
        assertTrue(_connection.isClosed());
        verify(duplicate).dispose();
        verify(_inputBuffer).dispose();
        verify(_protocolEngine).closed();
        verify(_connectionScheduler).unregisterConnection(_connection);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void jettyCloseWithExpiredDeadlineAbortsImmediately(final boolean expiredAmqpDeadline)
    {
        if (expiredAmqpDeadline)
        {
            _connection.setCloseDeadline(new MutableTicker(0));
        }
        else
        {
            doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        }
        final Callback close = mock();
        _coreSession.close(new CloseStatus(CloseStatus.NORMAL), close);

        verify(close).failed(isA(ClosedChannelException.class));
        verify(_transport, never()).write(any(Callback.class), any(ByteBuffer[].class));
        scheduleDueConnections();
        verify(_transport).closeSocket();
        _threadPool.runAll();
        verify(_inputBuffer).dispose();
        verify(_protocolEngine).closed();
        verify(_connectionScheduler).unregisterConnection(_connection);
    }

    @Test
    void jettyCloseKeepsPortDeadlineAfterStartingDuringBrokerClose()
    {
        _connection.close();
        _currentTime.addAndGet(750L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(750L));
        final Callback close = mock();
        _coreSession.close(new CloseStatus(CloseStatus.NORMAL), close);
        assertNotNull(_pendingWrite);
        _pendingWrite.succeeded();
        verify(close).succeeded();

        assertEquals(250L, scheduleDueConnections());
        verify(_transport, never()).closeSocket();
        _currentTime.addAndGet(250L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(250L));
        scheduleDueConnections();
        verify(_transport).closeSocket();
        _threadPool.runAll();
        verify(_connectionScheduler).unregisterConnection(_connection);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void outgoingCloseHonoursSharedDeadlineAfterCloseStarts(final boolean alreadyForceClosing)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        _connection.close();
        assertTrue(_connection.commitWebSocketClose());
        deadline.setTimeToNextTick(0);
        if (alreadyForceClosing)
        {
            assertTrue(_connection.requestForceCloseIfDeadlineExpired());
        }
        assertTrue(_transport.isOpen());

        final Callback close = mock();
        _coreSession.close(new CloseStatus(CloseStatus.NORMAL), close);

        verify(close).failed(isA(ClosedChannelException.class));
        verify(_transport, never()).write(any(Callback.class), any(ByteBuffer[].class));
        assertTrue(_connection.isForceClosing());
        scheduleDueConnections();
        _threadPool.runAll();
        verify(_transport).closeSocket();
        verify(_connectionScheduler).unregisterConnection(_connection);
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true"})
    void idleCloseLogsOnlyWhenItStartsTransportClose(final boolean amqpDeadlineArmed, final boolean alreadyClosing)
    {
        if (amqpDeadlineArmed)
        {
            _connection.setCloseDeadline(new MutableTicker(500));
        }
        if (alreadyClosing)
        {
            _connection.close();
        }
        final Runnable logIdleClose = mock();
        doAnswer(invocation ->
        {
            assertTrue(_connection.isCloseRequested());
            CompletableFuture.runAsync(() -> _connection.setCloseDeadline(new MutableTicker(500)))
                    .get(5L, TimeUnit.SECONDS);
            return null;
        }).when(logIdleClose).run();

        _connection.closeOnReadIdle(logIdleClose);

        verify(logIdleClose, times(amqpDeadlineArmed || alreadyClosing ? 0 : 1)).run();
        assertEquals(amqpDeadlineArmed && !alreadyClosing, _connection.isOpen());
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

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true"})
    void amqpCloseReplyHasOneWriteAttemptAtZeroTimeout(final boolean sharedDeadlineExpires,
                                                      final boolean pendingBinaryWrite)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        if (pendingBinaryWrite)
        {
            send(_connection, new byte[] {1});
            _connection.doWrite();
            _threadPool.runAll();
            assertNotNull(_pendingWrite);
        }

        send(_connection, new byte[] {2});
        _connection.flush();
        send(_connection, new byte[] {3});
        when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        _connection.doWrite();
        if (!pendingBinaryWrite)
        {
            assertEquals(500L, scheduleDueConnections());
            if (sharedDeadlineExpires)
            {
                deadline.setTimeToNextTick(0);
            }
            _threadPool.runNext();
        }

        verify(_transport, times(sharedDeadlineExpires ? 0 : 1)).write(any(Callback.class), any(ByteBuffer[].class));
        scheduleDueConnections();
        verify(_transport).closeSocket();
        _threadPool.runAll();
        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
    }

    @ParameterizedTest
    @CsvSource({"false, false, 2", "false, true, 3", "true, false, 1", "true, true, 1"})
    void synchronousFinalWritesDrainWithinSharedDeadline(final boolean sharedDeadlineExpires,
                                                         final boolean queuedBinaryWrite,
                                                         final int expectedWrites)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        doAnswer(invocation ->
        {
            final Callback callback = invocation.getArgument(0);
            final ByteBuffer[] output = (ByteBuffer[]) invocation.getRawArguments()[1];
            for (final ByteBuffer buffer : output)
            {
                buffer.position(buffer.limit());
            }
            callback.succeeded();
            if (sharedDeadlineExpires)
            {
                deadline.setTimeToNextTick(0);
            }
            return null;
        }).when(_transport).write(any(Callback.class), any(ByteBuffer[].class));

        send(_connection, new byte[] {1});
        if (queuedBinaryWrite)
        {
            _connection.flush();
            send(_connection, new byte[] {2});
        }
        when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        _connection.doWrite();
        _threadPool.runNext();
        verify(_transport, times(expectedWrites)).write(any(Callback.class), any(ByteBuffer[].class));

        scheduleDueConnections();
        _threadPool.runAll();

        verify(_transport).closeSocket();
        assertTrue(_connection.isClosed());
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true"})
    void webSocketCloseHasOneWriteAttemptAtZeroTimeout(final boolean sharedDeadlineExpires,
                                                       final boolean peerCloseBeforeSubmission)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        _receiveCloseBeforeSend = peerCloseBeforeSubmission;

        _connection.doWrite();
        assertEquals(500L, scheduleDueConnections());
        if (sharedDeadlineExpires)
        {
            deadline.setTimeToNextTick(0);
        }
        _threadPool.runNext();
        scheduleDueConnections();
        _threadPool.runAll();

        verify(_transport, times(sharedDeadlineExpires ? 0 : 1)).write(any(Callback.class), any(ByteBuffer[].class));
        verify(_transport).closeSocket();
        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void incomingCloseGetsFirstWriteAtZeroTimeout(final boolean sharedDeadlineExpires)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        final AtomicBoolean scanBeforeReply = new AtomicBoolean(true);
        doAnswer(invocation ->
        {
            if (scanBeforeReply.getAndSet(false))
            {
                verify(_transport, never()).write(any(Callback.class), any(ByteBuffer[].class));
                if (sharedDeadlineExpires)
                {
                    deadline.setTimeToNextTick(0);
                }
                scheduleDueConnections();
                verify(_transport, times(sharedDeadlineExpires ? 1 : 0)).closeSocket();
            }
            return invocation.callRealMethod();
        }).when(_connectionScheduler).wakeup();

        receiveClose();

        verify(_transport, times(sharedDeadlineExpires ? 0 : 1)).write(any(Callback.class), any(ByteBuffer[].class));
        scheduleDueConnections();
        verify(_transport).closeSocket();
        _threadPool.runAll();
        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
    }

    @ParameterizedTest
    @CsvSource({"500, false", "500, true", "-1, false"})
    void peerClosePreservesDeadlineDuringFinalCloseSubmission(final int sharedDeadlineMillis,
                                                              final boolean sharedDeadlineExpires)
    {
        final MutableTicker deadline = new MutableTicker(sharedDeadlineMillis);
        if (sharedDeadlineMillis >= 0)
        {
            _connection.setCloseDeadline(deadline);
        }
        doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        _submitFrame = submit ->
        {
            _submitFrame = Runnable::run;
            submit.run();
            assertNotNull(_pendingWrite);
            final Callback peerClose = receiveClose();
            verify(peerClose, never()).succeeded();
            verify(peerClose, never()).failed(any());
            if (sharedDeadlineExpires)
            {
                deadline.setTimeToNextTick(0);
            }

            final long nextCheckMillis = scheduleDueConnections();
            verify(_transport, times(sharedDeadlineExpires ? 1 : 0)).closeSocket();
            if (!sharedDeadlineExpires)
            {
                assertEquals(sharedDeadlineMillis < 0 ? AmqpPort.DEFAULT_FINAL_WRITE_TIMEOUT : sharedDeadlineMillis,
                        nextCheckMillis);
            }
        };

        _connection.doWrite();
        _threadPool.runNext();

        scheduleDueConnections();
        verify(_transport).closeSocket();
        _threadPool.runAll();
        assertTrue(_connection.isClosed());
    }

    @Test
    void incomingCloseSubmissionHasDeadlineWithoutAmqpClose()
            throws Exception
    {
        doReturn(0L).when(_connectionScheduler).getCloseTimeoutNanos();
        final CountDownLatch submittingReply = new CountDownLatch(1);
        final CountDownLatch returnFromSubmission = new CountDownLatch(1);
        doAnswer(invocation ->
        {
            submittingReply.countDown();
            assertTrue(returnFromSubmission.await(5L, TimeUnit.SECONDS));
            return null;
        }).when(_transport).write(any(Callback.class), any(ByteBuffer[].class));

        final CompletableFuture<Callback> reply = CompletableFuture.supplyAsync(this::receiveClose);
        try
        {
            assertTrue(submittingReply.await(5L, TimeUnit.SECONDS));
            assertEquals(AmqpPort.DEFAULT_FINAL_WRITE_TIMEOUT, scheduleDueConnections());
            verify(_transport, never()).closeSocket();

            _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(AmqpPort.DEFAULT_FINAL_WRITE_TIMEOUT));
            scheduleDueConnections();
            verify(_transport).closeSocket();
        }
        finally
        {
            returnFromSubmission.countDown();
            reply.get(5L, TimeUnit.SECONDS);
        }
        _threadPool.runAll();

        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
        verify(_connectionScheduler).unregisterConnection(_connection);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void incomingCloseCannotReplyAfterPositiveDeadline(final boolean sharedDeadlineExpires)
    {
        final MutableTicker deadline = new MutableTicker(5000);
        _connection.setCloseDeadline(deadline);
        final AtomicBoolean expireBeforeReply = new AtomicBoolean(true);
        doAnswer(invocation ->
        {
            if (expireBeforeReply.getAndSet(false))
            {
                if (sharedDeadlineExpires)
                {
                    deadline.setTimeToNextTick(0);
                }
                else
                {
                    _currentTime.addAndGet(1000L);
                    _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
                }
            }
            return invocation.callRealMethod();
        }).when(_connectionScheduler).wakeup();

        receiveClose();

        verify(_transport, never()).write(any(Callback.class), any(ByteBuffer[].class));
        scheduleDueConnections();
        _threadPool.runAll();
        assertTrue(_connection.isClosed());
        verify(_protocolEngine).closed();
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
        final Logger logger = (Logger) LoggerFactory.getLogger(AmqpWebSocket.class);
        final Level originalLevel = logger.getLevel();
        final Appender<ILoggingEvent> appender = mock();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);
        try
        {
            closeTransport();
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(originalLevel);
        }

        final ArgumentCaptor<ILoggingEvent> event = ArgumentCaptor.captor();
        verify(appender).doAppend(event.capture());
        assertEquals(Level.WARN, event.getValue().getLevel());
        assertEquals(ClosedChannelException.class.getName(), event.getValue().getThrowableProxy().getClassName());
        assertTrue(_connection.isClosed());
        scheduleDueConnections();
        _threadPool.runAll();
        verify(_protocolEngine).closed();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    @ParameterizedTest
    @CsvSource({"false, true", "false, false", "true, true", "true, false"})
    void peerCloseReplyOwnsTransportAfterConcurrentSubmission(final boolean binaryWrite, final boolean flushReply)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        _receiveCloseBeforeSend = true;

        if (binaryWrite)
        {
            send(_connection, new byte[] {1});
        }
        else
        {
            _connection.close();
        }
        _connection.doWrite();
        _threadPool.runNext();

        assertNotNull(_pendingWrite);
        assertEquals(500L, scheduleDueConnections());
        assertTrue(_transport.isOpen());
        verify(_transport).write(any(Callback.class), any(ByteBuffer[].class));
        if (flushReply)
        {
            _pendingWrite.succeeded();
        }
        else
        {
            deadline.setTimeToNextTick(0);
            scheduleDueConnections();
            verify(_transport).closeSocket();
        }
        scheduleDueConnections();
        _threadPool.runAll();
        assertFalse(_transport.isOpen());
        verify(_protocolEngine).closed();
    }

    @ParameterizedTest
    @EnumSource(CloseWriteOutcome.class)
    void simultaneousCloseWaitsForAcceptedCloseWrite(final CloseWriteOutcome outcome)
    {
        final MutableTicker deadline = new MutableTicker(500);
        _connection.setCloseDeadline(deadline);
        _connection.close();
        _connection.doWrite();
        _threadPool.runNext();
        assertNotNull(_pendingWrite);

        final Runnable peerCloseComplete = mock();
        receiveClose(Callback.from(peerCloseComplete, failure -> peerCloseComplete.run()));

        assertTrue(_transport.isOpen(), "The accepted Close frame is still waiting for the socket");
        verify(peerCloseComplete, never()).run();
        verify(_transport, never()).shutdownOutput();
        assertEquals(500L, scheduleDueConnections());
        _threadPool.runAll();
        verify(_protocolEngine, never()).closed();
        switch (outcome)
        {
            case FLUSHED ->
            {
                _pendingWrite.succeeded();
                verify(_transport).shutdownOutput();
            }
            case FAILED -> _pendingWrite.failed(new ClosedChannelException());
            case DISCONNECTED -> closeTransport();
            case TIMED_OUT -> deadline.setTimeToNextTick(0);
        }
        scheduleDueConnections();
        _threadPool.runAll();
        assertFalse(_transport.isOpen());
        assertTrue(_connection.isClosed());
        verify(peerCloseComplete).run();
        verify(_protocolEngine).closed();
    }

    private Callback receiveClose()
    {
        final Callback callback = mock();
        receiveClose(callback);
        return callback;
    }

    private void receiveClose(final Callback callback)
    {
        final Frame close = new CloseStatus(CloseStatus.NORMAL).toFrame().setMask(new byte[] {1, 2, 3, 4});
        _coreSession.onFrame(close, callback);
    }

    private void closeTransport()
    {
        if (_transportOpen.compareAndSet(true, false))
        {
            _jettyConnection.onClose(null);
        }
    }
}
