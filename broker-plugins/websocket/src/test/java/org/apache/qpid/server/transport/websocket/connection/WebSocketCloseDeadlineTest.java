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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;

class WebSocketCloseDeadlineTest extends WebSocketTestBase
{
    @ParameterizedTest
    @ValueSource(longs = {0L, -1L})
    void expiredConnectionClosingTickerAbortsWithoutDraining(final long timeoutMillis)
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(10);
        when(buffer.limit()).thenReturn(10);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);
        _connectionScheduler.registerConnection(connection);
        final ConnectionClosingTicker deadline = new ConnectionClosingTicker(timeoutMillis, connection);
        connection.setCloseDeadline(deadline);

        deadline.tick(_currentTime.get());

        assertTrue(connection.isForceClosing());
        scheduleDueConnections();
        _threadPool.runAll();
        verify(session).disconnect();
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        verify(duplicate).dispose();

        deadline.tick(_currentTime.get());
        connection.close();
        scheduleDueConnections();
        _threadPool.runAll();
        verify(session).disconnect();
        verify(duplicate).dispose();
    }

    @Test
    void schedulerEnforcesSharedDeadlineWhileTransportOpen()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);

        assertEquals(500L, scheduleDueConnections());
        assertTrue(connection.isOpen());
        assertEquals(0, _threadPool.getQueueSize());

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(500L));
        deadline.setTimeToNextTick(0);
        scheduleDueConnections();

        assertTrue(connection.isForceClosing());
        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void pendingOrRejectedProtocolTickCannotDelaySharedDeadline(final boolean rejectTick)
    {
        final Session session = mock(Session.class);
        final MutableTicker protocolTicker = new MutableTicker(0);
        final WebSocketConnection connection = createConnection(protocolTicker, session);
        final MutableTicker deadline = new MutableTicker(50);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        if (rejectTick)
        {
            _threadPool.rejectNext();
        }

        assertEquals(50L, scheduleDueConnections());
        _currentTime.addAndGet(50L);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(50L));
        deadline.setTimeToNextTick(0);
        scheduleDueConnections();
        _threadPool.runAll();

        verify(session).disconnect();
        assertEquals(0, protocolTicker.getTickCount());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void finalWriteAndWebSocketHandshakeUseRemainingSharedDeadline(final boolean pendingWrite)
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(2_000);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        if (pendingWrite)
        {
            send(connection, new byte[] {1});
        }

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1_750L));
        deadline.setTimeToNextTick(250);
        connection.close();

        assertEquals(250L, scheduleDueConnections());
        _threadPool.runNext();
        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.captor();
        if (pendingWrite)
        {
            verify(session).sendBinary(any(ByteBuffer.class), callbackCaptor.capture());
        }
        else
        {
            verify(session).close(anyInt(), isNull(), callbackCaptor.capture());
        }
        verify(session, never()).disconnect();

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(250L));
        deadline.setTimeToNextTick(0);
        scheduleDueConnections();
        callbackCaptor.getValue().succeed();
        scheduleDueConnections();
        _threadPool.runAll();

        verify(session).disconnect();
        if (pendingWrite)
        {
            verify(session).sendBinary(any(ByteBuffer.class), any(Callback.class));
            verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        }
        else
        {
            verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
            verify(session).close(anyInt(), isNull(), any(Callback.class));
        }
    }

    @Test
    void transportTimeoutCanExpireBeforeSharedDeadline()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(2_000);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        connection.close();

        assertEquals(1_000L, scheduleDueConnections());
        _threadPool.runNext();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1_000L));
        deadline.setTimeToNextTick(1_000);
        scheduleDueConnections();

        verify(session).disconnect();
    }

    @Test
    void repeatedDeadlineDoesNotExtendShutdown()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(450L));
        deadline.setTimeToNextTick(50);
        connection.setCloseDeadline(new MutableTicker(10_000));
        connection.close();

        assertEquals(50L, scheduleDueConnections());
        _threadPool.runNext();
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(50L));
        deadline.setTimeToNextTick(0);
        scheduleDueConnections();

        verify(session).disconnect();
    }

    @Test
    void expiredDeadlinePreventsQueuedWriteSubmission()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        send(connection, new byte[] {1});
        connection.doWrite();

        deadline.setTimeToNextTick(0);
        _threadPool.runNext();
        scheduleDueConnections();
        _threadPool.runAll();

        verify(session).disconnect();
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
    }

    @Test
    void blockedProtocolWorkCannotDelaySharedDeadline() throws Exception
    {
        final CountDownLatch workStarted = new CountDownLatch(1);
        final CountDownLatch releaseWork = new CountDownLatch(1);
        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(new AggregateTicker());
        when(protocolEngine.processPendingIterator()).thenReturn(List.of((Runnable) () ->
        {
            workStarted.countDown();
            await(releaseWork, "Timed out waiting to release protocol work");
        }).iterator());
        final Session session = mock(Session.class);
        final WebSocketConnection connection = createConnection(protocolEngine, session);
        final Runnable closeTask = mock(Runnable.class);
        doAnswer(invocation -> connection.webSocketClosed(closeTask)).when(session).disconnect();
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        final FutureTask<Void> protocolWork = new FutureTask<>(() ->
        {
            connection.doWork();
            return null;
        });
        startDaemonThread(protocolWork, "blocked-protocol-work");

        try
        {
            await(workStarted, "Protocol work did not start");
            assertEquals(500L, scheduleDueConnections());
            _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(450L));
            deadline.setTimeToNextTick(50);
            assertEquals(50L, scheduleDueConnections());
            _threadPool.runAll();
            assertTrue(connection.isOpen());
            verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
            verify(session, never()).disconnect();

            _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(50L));
            deadline.setTimeToNextTick(0);
            scheduleDueConnections();
            _threadPool.runAll();

            verify(session).disconnect();
            assertTrue(connection.isClosed());
            assertFalse(protocolWork.isDone());
            verify(closeTask, never()).run();
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

    @ParameterizedTest
    @ValueSource(longs = {0L, 1_000L, 5_000L})
    void unansweredAmqpCloseUsesFullSharedDeadline(final long finalWriteTimeoutMillis)
    {
        _connectionScheduler = new WebSocketConnectionScheduler(getTestName(), _currentTime::get, _nanoTime::get,
                TimeUnit.MILLISECONDS.toNanos(finalWriteTimeoutMillis));
        final Session session = mock();
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker((int) Connection.DEFAULT_CLOSE_RESPONSE_TIMEOUT);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);

        assertEquals(Connection.DEFAULT_CLOSE_RESPONSE_TIMEOUT, scheduleDueConnections());
        for (final int elapsedMillis : new int[] {1_800, 1_900, 1_999})
        {
            _currentTime.set(INITIAL_TIME + elapsedMillis);
            _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(INITIAL_TIME + elapsedMillis));
            final int remainingMillis = (int) Connection.DEFAULT_CLOSE_RESPONSE_TIMEOUT - elapsedMillis;
            deadline.setTimeToNextTick(remainingMillis);
            assertEquals(remainingMillis, scheduleDueConnections());
            _threadPool.runAll();

            assertTrue(connection.isOpen());
            verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
            verify(session, never()).disconnect();
        }

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
        _currentTime.incrementAndGet();
        deadline.setTimeToNextTick(0);
        scheduleDueConnections();
        _threadPool.runAll();

        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        assertEquals(0, _threadPool.getQueueSize());
    }

    @ParameterizedTest
    @ValueSource(ints = {1_800, 1_900, 1_999})
    void amqpCloseReplyBeforeSharedDeadlineStartsWebSocketClose(final int elapsedMillis)
    {
        final MultiVersionProtocolEngine protocolEngine = mock();
        final Session session = mock();
        final AtomicBoolean closeComplete = new AtomicBoolean();
        when(protocolEngine.getAggregateTicker()).thenReturn(new AggregateTicker());
        when(protocolEngine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());
        when(protocolEngine.isProtocolCloseComplete()).thenAnswer(invocation -> closeComplete.get());
        doAnswer(invocation ->
        {
            final QpidByteBuffer buffer = invocation.getArgument(0);
            buffer.position(buffer.limit());
            closeComplete.set(true);
            return null;
        }).when(protocolEngine).received(any(QpidByteBuffer.class));
        final WebSocketConnection connection = createConnection(protocolEngine, session);
        final MutableTicker deadline = new MutableTicker((int) Connection.DEFAULT_CLOSE_RESPONSE_TIMEOUT);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);

        assertEquals(Connection.DEFAULT_CLOSE_RESPONSE_TIMEOUT, scheduleDueConnections());
        _currentTime.set(INITIAL_TIME + 1_800L);
        _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(INITIAL_TIME + 1_800L));
        deadline.setTimeToNextTick(200);
        assertEquals(200L, scheduleDueConnections());
        _threadPool.runAll();
        assertTrue(connection.isOpen());
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        _currentTime.set(INITIAL_TIME + elapsedMillis);
        _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(INITIAL_TIME + elapsedMillis));
        deadline.setTimeToNextTick((int) Connection.DEFAULT_CLOSE_RESPONSE_TIMEOUT - elapsedMillis);
        final ByteBuffer closeFrame = ByteBuffer.wrap(new byte[] {0, 0, 0, 12, 2, 0, 0, 0, 0, 0x53, 0x18, 0x45});
        final WebSocketReceiver receiver = new WebSocketReceiver(SETTINGS);
        try
        {
            receiver.receiveBinary(closeFrame, connection, protocolEngine);

            assertFalse(closeFrame.hasRemaining());
            verify(protocolEngine).received(any(QpidByteBuffer.class));
            assertTrue(closeComplete.get());
            assertTrue(connection.isCloseRequested());
            _threadPool.runAll();
            verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
            verify(session, never()).disconnect();
        }
        finally
        {
            receiver.dispose();
        }
    }

    @Test
    void pendingBinaryWriteDrainsBeforeWebSocketCloseAfterAmqpClose()
    {
        final MultiVersionProtocolEngine protocolEngine = mock();
        when(protocolEngine.getAggregateTicker()).thenReturn(new AggregateTicker());
        final Session session = mock();
        final WebSocketConnection connection = createConnection(protocolEngine, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        send(connection, new byte[] {1});
        connection.doWrite();
        _threadPool.runAll();
        final ArgumentCaptor<Callback> writeCallback = ArgumentCaptor.captor();
        verify(session).sendBinary(any(ByteBuffer.class), writeCallback.capture());
        send(connection, new byte[] {2});

        deadline.setTimeToNextTick(50);
        when(protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        connection.doWrite();
        assertEquals(50L, scheduleDueConnections());
        assertEquals(0, _threadPool.getQueueSize());
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        writeCallback.getValue().succeed();
        _threadPool.runNext();
        final ArgumentCaptor<Callback> lastWriteCallback = ArgumentCaptor.captor();
        verify(session, times(2)).sendBinary(any(ByteBuffer.class), lastWriteCallback.capture());
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        lastWriteCallback.getValue().succeed();
        _threadPool.runAll();

        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
        verify(session, never()).disconnect();
    }

    @Test
    void blockedWriteDisposesQueuedBuffersAtHardDeadline()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        send(connection, new byte[] {1});
        connection.doWrite();
        _threadPool.runAll();
        final ArgumentCaptor<Callback> writeCallback = ArgumentCaptor.captor();
        verify(session).sendBinary(any(ByteBuffer.class), writeCallback.capture());
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(10);
        when(buffer.limit()).thenReturn(10);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);

        deadline.setTimeToNextTick(50);
        assertEquals(50L, scheduleDueConnections());
        assertTrue(connection.isOpen());
        verify(duplicate, never()).dispose();
        verify(session, never()).disconnect();

        deadline.setTimeToNextTick(0);
        scheduleDueConnections();
        _threadPool.runAll();
        writeCallback.getValue().succeed();
        _threadPool.runAll();

        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        verify(duplicate).dispose();
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void rejectedWebSocketCloseSubmissionAbortsImmediately()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        deadline.setTimeToNextTick(50);
        connection.close();
        _threadPool.rejectNext();

        scheduleDueConnections();
        _threadPool.runAll();

        assertTrue(connection.isForceClosing());
        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void queuedWebSocketCloseCannotBeSubmittedAfterHardDeadline()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        deadline.setTimeToNextTick(50);
        connection.close();
        scheduleDueConnections();
        assertEquals(1, _threadPool.getQueueSize());

        deadline.setTimeToNextTick(0);
        _threadPool.runNext();
        scheduleDueConnections();
        _threadPool.runAll();

        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void explicitAbortBeforeSharedDeadlineRemainsImmediate()
    {
        final Session session = mock(Session.class);
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        final MutableTicker deadline = new MutableTicker(500);
        connection.setCloseDeadline(deadline);
        _connectionScheduler.registerConnection(connection);
        deadline.setTimeToNextTick(50);
        scheduleDueConnections();

        connection.abort();
        scheduleDueConnections();
        _threadPool.runAll();

        assertTrue(connection.isForceClosing());
        verify(session).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        assertEquals(0, _threadPool.getQueueSize());
    }
}
