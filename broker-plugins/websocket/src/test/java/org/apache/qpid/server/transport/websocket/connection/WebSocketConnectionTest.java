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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.network.Ticker;

class WebSocketConnectionTest extends WebSocketTestBase
{
    @Test
    void closeInitiatesWebSocketHandshakeOnce()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);

        connection.close();

        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();
        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
        verify(session, never()).disconnect();

        connection.close();
        assertEquals(1_000L, scheduleDueConnections());
        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
    }

    @Test
    void unansweredWebSocketCloseIsForciblyDisconnected()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();

        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();
        final ArgumentCaptor<Callback> closeCallbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).close(eq(StatusCode.NORMAL), isNull(), closeCallbackCaptor.capture());
        closeCallbackCaptor.getValue().succeed();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(999L));
        assertEquals(1L, scheduleDueConnections());
        verify(session, never()).disconnect();

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();

        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        assertEquals(1_000L, scheduleDueConnections());
        verify(session, times(2)).disconnect();
    }

    @Test
    void repeatedCloseDoesNotExtendDeadline()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(900L));
        connection.close();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(100L));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
        verify(session).disconnect();
    }

    @Test
    void closeDeadlineIsIndependentOfWallClock()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();

        _currentTime.addAndGet(-500_000L);
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
    }

    @Test
    void naturalClosePreventsForcedDisconnect()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();

        assertTrue(connection.webSocketClosed(mock(Runnable.class)));
        assertFalse(connection.webSocketClosed(mock(Runnable.class)));
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2L));

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        verify(session, never()).disconnect();
    }

    @Test
    void finalAmqpBytesCompleteBeforeWebSocketClose()
    {
        final byte[] expected = {1, 2, 3, 4};
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(expected))
        {
            connection.send(buffer);
        }
        connection.close();

        connection.doWrite();
        _threadPool.runNext();

        final ArgumentCaptor<ByteBuffer> dataCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(dataCaptor.capture(), callbackCaptor.capture());
        assertEquals(ByteBuffer.wrap(expected), dataCaptor.getValue());
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        callbackCaptor.getValue().succeed();
        _threadPool.runNext();

        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
    }

    @Test
    void writeFailureForcesDisconnectWithoutWebSocketClose()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }
        connection.close();
        connection.doWrite();
        _threadPool.runNext();

        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(any(ByteBuffer.class), callbackCaptor.capture());
        callbackCaptor.getValue().fail(new IOException("Write failed for test"));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
    }

    @Test
    void nonDrainingCloseDiscardsPendingWrites()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(10);
        when(buffer.limit()).thenReturn(10);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);
        _connectionScheduler.registerConnection(connection);

        connection.close(StatusCode.BAD_DATA, false);

        assertEquals(1_000L, scheduleDueConnections());
        verify(duplicate, never()).dispose();
        _threadPool.runAll();
        verify(duplicate).dispose();
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session).close(eq(StatusCode.BAD_DATA), isNull(), any(Callback.class));
    }

    @Test
    void nonDrainingCloseDiscardsSelectedWriteBeforeSubmission() throws Exception
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final BlockingWriteConnection connection = createBlockingWriteConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }
        connection.doWrite();

        final FutureTask<Void> writeTask = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(writeTask, "selected-write");

        try
        {
            connection.awaitWriteSelected();
            connection.close(StatusCode.BAD_DATA, false);
        }
        finally
        {
            connection.continueWriteSubmission();
            writeTask.get(10L, TimeUnit.SECONDS);
        }

        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        assertEquals(1, _threadPool.getQueueSize());
        _threadPool.runNext();
        verify(session).close(eq(StatusCode.BAD_DATA), isNull(), any(Callback.class));
    }

    @Test
    void expiredDeadlineDiscardsSelectedWriteBeforeSubmission() throws Exception
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final BlockingWriteConnection connection = createBlockingWriteConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }
        _connectionScheduler.registerConnection(connection);
        connection.close();
        connection.doWrite();

        final FutureTask<Void> writeTask = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(writeTask, "selected-write-at-deadline");

        try
        {
            connection.awaitWriteSelected();
            _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        }
        finally
        {
            connection.continueWriteSubmission();
            writeTask.get(10L, TimeUnit.SECONDS);
        }

        assertTrue(connection.isForceClosing());
        assertEquals(0, _threadPool.getQueueSize());
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
    }

    @Test
    void webSocketCloseFailureForcesDisconnectImmediately()
    {
        final Session session = mock(Session.class);
        doThrow(new IllegalStateException("Close failed for test"))
                .when(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();

        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();
        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
    }

    @Test
    void forceCloseFailureUsesBackoff()
    {
        final Session session = mock(Session.class);
        doThrow(new IllegalStateException("Disconnect failed for test")).when(session).disconnect();
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();

        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        assertEquals(1_000L, scheduleDueConnections());
        verify(session, times(2)).disconnect();
    }

    @Test
    void expiredCloseDoesNotStartPendingWrite()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(10);
        when(buffer.limit()).thenReturn(10);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

        connection.doWrite();
        _threadPool.runNext();
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));

        assertEquals(1_000L, scheduleDueConnections());
        verify(duplicate, never()).dispose();
        _threadPool.runAll();
        verify(duplicate).dispose();
        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
    }

    @Test
    void stalledFinalWriteDoesNotExtendCloseDeadline()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }
        _connectionScheduler.registerConnection(connection);
        connection.close();
        connection.doWrite();
        _threadPool.runNext();
        verify(session).sendBinary(any(ByteBuffer.class), any(Callback.class));

        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
    }

    @Test
    void blockedProtocolWorkDoesNotDelayDeadlinesOrHeartbeats() throws Exception
    {
        final CountDownLatch workStarted = new CountDownLatch(1);
        final CountDownLatch releaseWork = new CountDownLatch(1);
        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        final Session blockedSession = mock(Session.class);
        final WebSocketConnection blockedConnection = createConnection(protocolEngine, blockedSession);
        final Runnable closeTask = mock(Runnable.class);
        doAnswer(invocation -> blockedConnection.webSocketClosed(closeTask)).when(blockedSession).disconnect();
        when(protocolEngine.processPendingIterator()).thenReturn(List.of((Runnable) () ->
        {
            blockedConnection.close();
            workStarted.countDown();
            await(releaseWork, "Timed out waiting to release protocol work");
        }).iterator());
        _connectionScheduler.registerConnection(blockedConnection);

        final Session expiringSession = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection expiringConnection = createConnection(ticker, expiringSession);
        _connectionScheduler.registerConnection(expiringConnection);
        final MutableTicker heartbeat = new MutableTicker(-1);
        heartbeat.setOnTick(() -> heartbeat.setTimeToNextTick(FAR_FUTURE_TICK_DELAY_MILLIS));
        _connectionScheduler.registerConnection(createConnection(heartbeat));

        final FutureTask<Void> protocolWork = new FutureTask<>(() ->
        {
            blockedConnection.doWork();
            return null;
        });
        startDaemonThread(protocolWork, "blocked-protocol");
        try
        {
            await(workStarted, "Timed out waiting for protocol work");
            expiringConnection.close();
            _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
            final FutureTask<Long> scan = new FutureTask<>(this::scheduleDueConnections);
            startDaemonThread(scan, "protocol-deadline-scan");

            assertEquals(1_000L, scan.get(5L, TimeUnit.SECONDS).longValue());
            verify(blockedSession).disconnect();
            verify(expiringSession).disconnect();

            final FutureTask<Void> worker = new FutureTask<>(() ->
            {
                _threadPool.runAll();
                return null;
            });
            startDaemonThread(worker, "cleanup-and-heartbeat");
            worker.get(5L, TimeUnit.SECONDS);
            assertEquals(1, heartbeat.getTickCount());
            verify(closeTask, never()).run();
            assertFalse(protocolWork.isDone());

            for (int i = 0; i < SUBMISSION_COUNT; i++)
            {
                scheduleDueConnections();
            }
            assertEquals(0, _threadPool.getQueueSize(), "Blocked cleanup was repeatedly submitted");
        }
        finally
        {
            releaseWork.countDown();
            protocolWork.get(5L, TimeUnit.SECONDS);
        }

        scheduleDueConnections();
        _threadPool.runAll();
        verify(closeTask).run();
    }

    @Test
    void blockedWebSocketWriteDoesNotDelayAnotherConnectionCloseDeadline() throws Exception
    {
        final Session expiringSession = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection expiringConnection = createConnection(ticker, expiringSession);
        _connectionScheduler.registerConnection(expiringConnection);
        expiringConnection.close();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(500L));

        final CountDownLatch sendStarted = new CountDownLatch(1);
        final CountDownLatch allowSendCompletion = new CountDownLatch(1);
        final Session slowSession = mock(Session.class);
        doAnswer(ignored ->
        {
            sendStarted.countDown();
            await(allowSendCompletion, "Timed out waiting to complete the WebSocket write");
            return null;
        }).when(slowSession).sendBinary(any(ByteBuffer.class), any(Callback.class));

        final MutableTicker slowTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection slowConnection = createConnection(slowTicker, slowSession);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            slowConnection.send(buffer);
        }
        _connectionScheduler.registerConnection(slowConnection);
        slowConnection.close();
        slowConnection.doWrite();

        final FutureTask<Void> writeTask = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(writeTask, "blocked-write");

        try
        {
            await(sendStarted, "Timed out waiting for the WebSocket write to start");
            _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(500L));

            final FutureTask<Long> scanTask = new FutureTask<>(this::scheduleDueConnections);
            startDaemonThread(scanTask, "deadline-scan");

            assertEquals(500L, scanTask.get(10L, TimeUnit.SECONDS).longValue());
            verify(expiringSession).disconnect();
            assertEquals(0, _threadPool.getQueueSize());
        }
        finally
        {
            allowSendCompletion.countDown();
            writeTask.get(10L, TimeUnit.SECONDS);
        }
    }

    @Test
    void blockedWebSocketCloseDoesNotPreventForcedDisconnect() throws Exception
    {
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final CountDownLatch allowCloseCompletion = new CountDownLatch(1);
        final Session session = mock(Session.class);
        doAnswer(ignored ->
        {
            closeStarted.countDown();
            await(allowCloseCompletion, "Timed out waiting to complete the WebSocket close");
            return null;
        }).when(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));

        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        connection.doWrite();

        final FutureTask<Void> closeTask = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(closeTask, "blocked-close");

        try
        {
            await(closeStarted, "Timed out waiting for the WebSocket close to start");
            _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

            final FutureTask<Long> scanTask = new FutureTask<>(this::scheduleDueConnections);
            startDaemonThread(scanTask, "close-deadline-scan");

            assertEquals(1_000L, scanTask.get(10L, TimeUnit.SECONDS).longValue());
            verify(session).disconnect();
        }
        finally
        {
            allowCloseCompletion.countDown();
            closeTask.get(10L, TimeUnit.SECONDS);
        }
    }

    @Test
    void expiredDeadlinePreventsWebSocketCloseSubmission() throws Exception
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final BlockingCloseConnection connection = createBlockingCloseConnection(ticker, session);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        connection.doWrite();

        final FutureTask<Void> closeTask = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(closeTask, "selected-close-at-deadline");

        try
        {
            connection.awaitCloseSelected();
            _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        }
        finally
        {
            connection.continueCloseSubmission();
            closeTask.get(10L, TimeUnit.SECONDS);
        }

        assertTrue(connection.isForceClosing());
        assertEquals(0, _threadPool.getQueueSize());
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
    }

    @Test
    void allExpiredClosingConnectionsAreForciblyDisconnected()
    {
        final int connectionCount = 128;
        final List<Session> sessions = new ArrayList<>(connectionCount);
        for (int i = 0; i < connectionCount; i++)
        {
            final Session session = mock(Session.class);
            final WebSocketConnection connection = createConnection(new MutableTicker(-1), session);
            sessions.add(session);
            _connectionScheduler.registerConnection(connection);
            connection.close();
        }

        assertEquals(1_000L, scheduleDueConnections());
        assertEquals(connectionCount, _threadPool.getQueueSize());
        _threadPool.runAll();
        assertEquals(0, _threadPool.getQueueSize());
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

        assertEquals(1_000L, scheduleDueConnections());
        assertEquals(0, _threadPool.getQueueSize());
        for (final Session session : sessions)
        {
            verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
            verify(session).disconnect();
        }
    }

    private BlockingWriteConnection createBlockingWriteConnection(final Ticker ticker, final Session session)
    {
        final AggregateTicker aggregateTicker = new AggregateTicker();
        aggregateTicker.addTicker(ticker);

        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);

        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return new BlockingWriteConnection(_connectionScheduler, session, address, address, protocolEngine,
                _threadPool);
    }

    private BlockingCloseConnection createBlockingCloseConnection(final Ticker ticker, final Session session)
    {
        final AggregateTicker aggregateTicker = new AggregateTicker();
        aggregateTicker.addTicker(ticker);

        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);

        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return new BlockingCloseConnection(_connectionScheduler, session, address, address, protocolEngine,
                _threadPool);
    }

    private static final class BlockingWriteConnection extends WebSocketConnection
    {
        private final CountDownLatch _writeSelected = new CountDownLatch(1);
        private final CountDownLatch _continueWriteSubmission = new CountDownLatch(1);

        private BlockingWriteConnection(final WebSocketConnectionScheduler scheduler,
                                        final Session connection,
                                        final InetSocketAddress localAddress,
                                        final InetSocketAddress remoteAddress,
                                        final MultiVersionProtocolEngine protocolEngine,
                                        final ThreadPool threadPool)
        {
            super(connection, localAddress, remoteAddress, protocolEngine, threadPool, scheduler, SETTINGS);
        }

        @Override
        boolean commitWriteBatch()
        {
            _writeSelected.countDown();
            await(_continueWriteSubmission, "Timed out waiting to continue the WebSocket write");
            return super.commitWriteBatch();
        }

        private void awaitWriteSelected()
        {
            await(_writeSelected, "Timed out waiting for write-batch selection");
        }

        private void continueWriteSubmission()
        {
            _continueWriteSubmission.countDown();
        }
    }

    private static final class BlockingCloseConnection extends WebSocketConnection
    {
        private final CountDownLatch _closeSelected = new CountDownLatch(1);
        private final CountDownLatch _continueCloseSubmission = new CountDownLatch(1);

        private BlockingCloseConnection(final WebSocketConnectionScheduler scheduler,
                                        final Session connection,
                                        final InetSocketAddress localAddress,
                                        final InetSocketAddress remoteAddress,
                                        final MultiVersionProtocolEngine protocolEngine,
                                        final ThreadPool threadPool)
        {
            super(connection, localAddress, remoteAddress, protocolEngine, threadPool, scheduler, SETTINGS);
        }

        @Override
        boolean commitWebSocketClose()
        {
            _closeSelected.countDown();
            await(_continueCloseSubmission, "Timed out waiting to continue the WebSocket close");
            return super.commitWebSocketClose();
        }

        private void awaitCloseSelected()
        {
            await(_closeSelected, "Timed out waiting for WebSocket CLOSE selection");
        }

        private void continueCloseSubmission()
        {
            _continueCloseSubmission.countDown();
        }
    }
}
