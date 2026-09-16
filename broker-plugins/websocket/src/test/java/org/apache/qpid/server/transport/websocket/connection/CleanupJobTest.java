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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serial;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

class CleanupJobTest extends WebSocketTestBase
{
    @Test
    void closeCleanupWaitsForInputProcessing()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final QpidByteBuffer inputBuffer = mock(QpidByteBuffer.class);
        _connectionScheduler.registerConnection(connection);
        connection.lockProtocol();
        try
        {
            assertTrue(connection.webSocketClosed(inputBuffer::dispose));
            assertEquals(Long.MAX_VALUE, scheduleDueConnections());
            _threadPool.runAll();
            verify(inputBuffer, never()).dispose();
            assertEquals(Long.MAX_VALUE, scheduleDueConnections());
            assertEquals(0, _threadPool.getQueueSize());
        }
        finally
        {
            connection.unlockProtocol();
        }

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runAll();
        verify(inputBuffer).dispose();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(0, _threadPool.getQueueSize());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleanupResumesWhenProtocolUnlocksBeforeDeferral(final boolean shutdown) throws Exception
    {
        _connectionScheduler = spy(_connectionScheduler);
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final PausedTryLock protocolLock = new PausedTryLock();
        final Field protocolLockField = WebSocketConnection.class.getDeclaredField("_protocolLock");
        protocolLockField.setAccessible(true);
        protocolLockField.set(connection, protocolLock);
        final QpidByteBuffer inputBuffer = mock(QpidByteBuffer.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final CountDownLatch protocolLocked = new CountDownLatch(1);
        final CountDownLatch releaseProtocol = new CountDownLatch(1);
        _connectionScheduler.registerConnection(connection);

        final FutureTask<Void> protocolWork = new FutureTask<>(() ->
        {
            connection.lockProtocol();
            try
            {
                protocolLocked.countDown();
                await(releaseProtocol, "Timed out waiting to release the protocol lock");
            }
            finally
            {
                connection.unlockProtocol();
            }
            return null;
        });
        final Thread protocolThread = startDaemonThread(protocolWork, "protocol-unlock-before-deferral");
        try
        {
            await(protocolLocked, "Timed out waiting for the protocol lock");
            connection.webSocketClosed(() ->
            {
                assertTrue(protocolLock.isHeldByCurrentThread(), "Cleanup must hold the protocol lock");
                inputBuffer.dispose();
                closeFuture.complete(null);
            });
            scheduleDueConnections();
            final FutureTask<Void> cleanup = new FutureTask<>(() ->
            {
                if (shutdown)
                {
                    _connectionScheduler.shutdown();
                }
                else
                {
                    _threadPool.runNext();
                }
                return null;
            });
            startDaemonThread(cleanup, "cleanup-before-deferral");
            try
            {
                protocolLock.awaitFailedTryLock();
                releaseProtocol.countDown();
                awaitBlockedOrFinished(protocolThread);
                assertFalse(protocolLock.isLocked());
                assertFalse(closeFuture.isDone());
                verify(inputBuffer, never()).dispose();
            }
            finally
            {
                protocolLock.continueDeferral();
                cleanup.get(5L, TimeUnit.SECONDS);
            }
        }
        finally
        {
            releaseProtocol.countDown();
            protocolWork.get(5L, TimeUnit.SECONDS);
        }

        if (shutdown)
        {
            assertTrue(closeFuture.isDone(), "Shutdown cleanup must finish without another scheduler scan");
        }
        scheduleDueConnections();
        _threadPool.runAll();
        assertTrue(closeFuture.isDone(), "Cleanup lost the protocol unlock notification");
        verify(inputBuffer).dispose();
        verify(_connectionScheduler).unregisterConnection(connection);
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void blockedCloseCleanupDoesNotDelayAnotherCloseDeadline() throws Exception
    {
        final CountDownLatch cleanupStarted = new CountDownLatch(1);
        final CountDownLatch releaseCleanup = new CountDownLatch(1);
        final WebSocketConnection closedConnection = createConnection(new MutableTicker(37_000));
        _connectionScheduler.registerConnection(closedConnection);
        closedConnection.webSocketClosed(() ->
        {
            cleanupStarted.countDown();
            await(releaseCleanup, "Timed out waiting to release protocol cleanup");
        });
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());

        final FutureTask<Void> cleanup = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(cleanup, "blocked-close-cleanup");
        try
        {
            await(cleanupStarted, "Timed out waiting for protocol cleanup");
            final Session session = mock(Session.class);
            final WebSocketConnection expiringConnection = createConnection(new MutableTicker(37_000), session);
            _connectionScheduler.registerConnection(expiringConnection);
            expiringConnection.close();
            _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

            final FutureTask<Long> scan = new FutureTask<>(this::scheduleDueConnections);
            startDaemonThread(scan, "cleanup-deadline-scan");
            assertEquals(1_000L, scan.get(5L, TimeUnit.SECONDS).longValue());
            verify(session).disconnect();
            assertEquals(0, _threadPool.getQueueSize());
        }
        finally
        {
            releaseCleanup.countDown();
            cleanup.get(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    void blockedBufferDisposalDoesNotDelayAnotherCloseDeadline() throws Exception
    {
        final CountDownLatch disposalStarted = new CountDownLatch(1);
        final CountDownLatch releaseDisposal = new CountDownLatch(1);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        doAnswer(invocation ->
        {
            disposalStarted.countDown();
            await(releaseDisposal, "Timed out waiting to release buffer disposal");
            return null;
        }).when(duplicate).dispose();
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(1);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        assertEquals(1_000L, scheduleDueConnections());

        final FutureTask<Void> cleanup = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(cleanup, "blocked-buffer-disposal");
        try
        {
            await(disposalStarted, "Timed out waiting for buffer disposal");
            final Session session = mock(Session.class);
            final WebSocketConnection expiringConnection = createConnection(new MutableTicker(37_000), session);
            _connectionScheduler.registerConnection(expiringConnection);
            expiringConnection.close();
            _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));

            final FutureTask<Long> scan = new FutureTask<>(this::scheduleDueConnections);
            startDaemonThread(scan, "disposal-deadline-scan");
            assertEquals(1_000L, scan.get(5L, TimeUnit.SECONDS).longValue());
            verify(session).disconnect();
            assertEquals(0, _threadPool.getQueueSize());
        }
        finally
        {
            releaseDisposal.countDown();
            cleanup.get(5L, TimeUnit.SECONDS);
        }
        verify(duplicate).dispose();
    }

    @Test
    void rejectedCleanupDoesNotPreventDisconnectAndRetriesWithBackoff()
    {
        final Session session = mock(Session.class);
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
        final Runnable closeTask = mock(Runnable.class);
        doAnswer(invocation -> connection.webSocketClosed(closeTask)).when(session).disconnect();
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(1);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);
        _connectionScheduler.registerConnection(connection);
        connection.close();
        _nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        _threadPool.rejectNext();

        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
        assertTrue(connection.isClosed());
        for (int i = 0; i < SUBMISSION_COUNT; i++)
        {
            assertEquals(1_000L, scheduleDueConnections());
        }
        assertEquals(0, _threadPool.getQueueSize());
        verify(duplicate, never()).dispose();
        verify(closeTask, never()).run();

        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(999L));
        assertEquals(1L, scheduleDueConnections());
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(1L));
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        for (int i = 0; i < SUBMISSION_COUNT; i++)
        {
            scheduleDueConnections();
        }
        assertEquals(1, _threadPool.getQueueSize());
        _threadPool.runAll();
        verify(duplicate).dispose();
        verify(closeTask).run();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void shutdownCompletesQueuedCleanupOnce()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final Runnable closeTask = mock(Runnable.class);
        _connectionScheduler.registerConnection(connection);
        connection.webSocketClosed(closeTask);
        scheduleDueConnections();
        assertEquals(1, _threadPool.getQueueSize());

        _connectionScheduler.shutdown();

        verify(closeTask).run();
        _threadPool.runAll();
        verify(closeTask).run();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    @Test
    void shutdownDoesNotRepeatRunningCleanup() throws Exception
    {
        final CountDownLatch cleanupStarted = new CountDownLatch(1);
        final CountDownLatch releaseCleanup = new CountDownLatch(1);
        final AtomicInteger cleanupCount = new AtomicInteger();
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        _connectionScheduler.registerConnection(connection);
        connection.webSocketClosed(() ->
        {
            cleanupCount.incrementAndGet();
            cleanupStarted.countDown();
            await(releaseCleanup, "Timed out waiting to complete cleanup during shutdown");
        });
        scheduleDueConnections();
        final FutureTask<Void> cleanup = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(cleanup, "cleanup-during-shutdown");
        try
        {
            await(cleanupStarted, "Timed out waiting for cleanup before shutdown");
            final FutureTask<Void> shutdown = new FutureTask<>(() ->
            {
                _connectionScheduler.shutdown();
                return null;
            });
            startDaemonThread(shutdown, "shutdown-during-cleanup");
            shutdown.get(5L, TimeUnit.SECONDS);
            assertFalse(cleanup.isDone());
            assertEquals(1, cleanupCount.get());
        }
        finally
        {
            releaseCleanup.countDown();
            cleanup.get(5L, TimeUnit.SECONDS);
        }
        _threadPool.runAll();
        assertEquals(1, cleanupCount.get());
    }

    @Test
    void shutdownDefersCleanupUntilProtocolUnlock()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final Runnable closeTask = mock(Runnable.class);
        _connectionScheduler.registerConnection(connection);
        connection.lockProtocol();
        try
        {
            connection.webSocketClosed(closeTask);
            _connectionScheduler.shutdown();
            verify(closeTask, never()).run();
        }
        finally
        {
            connection.unlockProtocol();
        }

        verify(closeTask).run();
        assertEquals(0, _threadPool.getQueueSize());
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    @Test
    void closeAfterSchedulerShutdownCompletesCleanup()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final Runnable closeTask = mock(Runnable.class);
        _connectionScheduler.registerConnection(connection);
        _connectionScheduler.shutdown();

        connection.webSocketClosed(closeTask);

        verify(closeTask).run();
        assertEquals(0, _threadPool.getQueueSize());
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
    }

    @Test
    void closeCleanupFailureDoesNotRetainConnectionOrProtocolLock() throws Exception
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000));
        final Runnable closeTask = mock(Runnable.class);
        doThrow(new IllegalStateException("Cleanup failed for test")).when(closeTask).run();
        _connectionScheduler.registerConnection(connection);
        connection.webSocketClosed(closeTask);
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());

        assertThrows(IllegalStateException.class, _threadPool::runNext);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(0, _threadPool.getQueueSize());
        final FutureTask<Void> protocolWork = new FutureTask<>(() ->
        {
            connection.lockProtocol();
            connection.unlockProtocol();
            return null;
        });
        startDaemonThread(protocolWork, "protocol-after-cleanup-failure");
        protocolWork.get(5L, TimeUnit.SECONDS);
        verify(closeTask).run();
    }

    private static void awaitBlockedOrFinished(final Thread thread)
    {
        final long startTime = System.nanoTime();
        while (thread.getState() != Thread.State.BLOCKED && thread.isAlive())
        {
            if (System.nanoTime() - startTime >= TimeUnit.SECONDS.toNanos(5L))
            {
                throw new AssertionError("Protocol unlock neither completed nor waited for deferral");
            }
            Thread.yield();
        }
    }

    private static final class PausedTryLock extends ReentrantLock
    {
        @Serial
        private static final long serialVersionUID = 1L;

        private final CountDownLatch _tryLockFailed = new CountDownLatch(1);
        private final CountDownLatch _continueDeferral = new CountDownLatch(1);

        @Override
        public boolean tryLock()
        {
            final boolean acquired = super.tryLock();
            if (!acquired)
            {
                _tryLockFailed.countDown();
                await(_continueDeferral, "Timed out waiting to publish cleanup deferral");
            }
            return acquired;
        }

        private void awaitFailedTryLock()
        {
            await(_tryLockFailed, "Timed out waiting for cleanup to try the protocol lock");
        }

        private void continueDeferral()
        {
            _continueDeferral.countDown();
        }
    }
}
