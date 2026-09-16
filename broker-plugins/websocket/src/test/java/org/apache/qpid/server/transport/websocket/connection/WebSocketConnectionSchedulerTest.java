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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.network.Ticker;

class WebSocketConnectionSchedulerTest extends WebSocketTestBase
{
    @Test
    void completionBackoffCannotBeBypassedBetweenEligibilityCheckAndClaim() throws Exception
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final BlockingConnection connection = createBlockingConnection(ticker);

        assertTrue(tryScheduleTick(connection));
        _threadPool.runNext();

        final CountDownLatch tickStarted = new CountDownLatch(1);
        final CountDownLatch allowTickCompletion = new CountDownLatch(1);
        ticker.setOnTick(() ->
        {
            tickStarted.countDown();
            await(allowTickCompletion, "Timed out waiting to complete the tick job");
        });

        assertTrue(tryScheduleTick(connection));
        final FutureTask<Void> tickTask = new FutureTask<>(() ->
        {
            _threadPool.runNext();
            return null;
        });
        startDaemonThread(tickTask, "tick");

        final FutureTask<Boolean> schedulingTask = new FutureTask<>(() -> tryScheduleTick(connection));
        try
        {
            await(tickStarted, "Timed out waiting for the tick job to start");
            connection.blockNextRetryDelayRead();
            startDaemonThread(schedulingTask, "schedule");
            connection.awaitRetryDelayRead();

            allowTickCompletion.countDown();
            tickTask.get(10L, TimeUnit.SECONDS);
            connection.continueRetryDelayRead();
        }
        finally
        {
            allowTickCompletion.countDown();
            connection.continueRetryDelayRead();
        }

        assertFalse(schedulingTask.get(10L, TimeUnit.SECONDS));
        assertEquals(0, _threadPool.getQueueSize());

        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void tickSubmissionsAreCoalescedWhileQueued()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(-1));

        int accepted = 0;
        for (int i = 0; i < SUBMISSION_COUNT; i++)
        {
            if (tryScheduleTick(connection))
            {
                accepted++;
            }
        }

        assertEquals(1, accepted);
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void tickRemainsCoalescedUntilJobCompletes()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        ticker.setOnTick(() -> assertFalse(tryScheduleTick(connection)));

        assertTrue(tryScheduleTick(connection));
        _threadPool.runNext();

        assertEquals(1, ticker.getTickCount());
        assertEquals(0, _threadPool.getQueueSize());

        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
        _threadPool.runNext();
        assertEquals(2, ticker.getTickCount());
    }

    @Test
    void tickRefreshesNextDeadlineAfterProcessing()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        ticker.setOnTick(() -> ticker.setTimeToNextTick(FAR_FUTURE_TICK_DELAY_MILLIS));

        assertTrue(tryScheduleTick(connection));
        _threadPool.runNext();

        assertEquals(1, ticker.getTickCount());
        assertEquals(1, ticker.getTimeToNextTickCallCount());
    }

    @Test
    void submissionFailureDoesNotLeaveTickOutstanding()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(-1));
        _threadPool.rejectNext();

        assertThrows(RejectedExecutionException.class, () -> tryScheduleTick(connection));

        assertFalse(tryScheduleTick(connection));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void submissionErrorDoesNotLeaveTickOutstanding()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(-1));
        _threadPool.failNextWithError();

        assertThrows(AssertionError.class, () -> tryScheduleTick(connection));

        assertFalse(tryScheduleTick(connection));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void tickFailureDoesNotLeaveTickOutstanding()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        ticker.setOnTick(() ->
        {
            throw new IllegalStateException("Ticker failure");
        });

        assertTrue(tryScheduleTick(connection));
        assertThrows(IllegalStateException.class, _threadPool::runNext);

        ticker.setOnTick(() ->
        {
        });
        assertFalse(tryScheduleTick(connection));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void everyDueConnectionIsScheduled()
    {
        final WebSocketConnection first = createConnection(new MutableTicker(-1));
        final WebSocketConnection second = createConnection(new MutableTicker(0));
        final WebSocketConnection future = createConnection(new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS));
        _connectionScheduler.registerConnection(first);
        _connectionScheduler.registerConnection(second);
        _connectionScheduler.registerConnection(future);

        final long timeToNextTick = scheduleDueConnections();

        assertEquals(FAR_FUTURE_TICK_DELAY_MILLIS, timeToNextTick);
        assertEquals(2, _threadPool.getQueueSize());
    }

    @Test
    void pendingDueConnectionIsNotResubmitted()
    {
        final WebSocketConnection connection = createConnection(new MutableTicker(-1));
        _connectionScheduler.registerConnection(connection);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());

        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void rejectedSubmissionDoesNotPreventOtherDueConnectionsFromBeingScheduled()
    {
        final WebSocketConnection first = createConnection(new MutableTicker(-1));
        final WebSocketConnection second = createConnection(new MutableTicker(-1));
        _connectionScheduler.registerConnection(first);
        _connectionScheduler.registerConnection(second);
        _threadPool.rejectNext();

        final long timeToNextTick = scheduleDueConnections();

        assertEquals(100L, timeToNextTick);
        assertEquals(1, _threadPool.getQueueSize());
        assertEquals(100L, scheduleDueConnections());
        assertEquals(1, _threadPool.getQueueSize());
        assertFalse(tryScheduleTick(first));
        _currentTime.addAndGet(100L);
        final boolean firstScheduled = tryScheduleTick(first);
        final boolean secondScheduled = tryScheduleTick(second);
        assertTrue(firstScheduled ^ secondScheduled);
        assertEquals(2, _threadPool.getQueueSize());
    }

    @Test
    void overdueTickRetryBackoffCannotBeBypassedByRepeatedScans()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        _connectionScheduler.registerConnection(connection);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runNext();

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runNext();

        for (int i = 0; i < 10; i++)
        {
            assertEquals(100L, scheduleDueConnections());
        }
        assertEquals(0, _threadPool.getQueueSize());

        final long[] retryDelays = {100L, 200L, 400L, 800L, 1_000L, 1_000L};
        for (final long retryDelay : retryDelays)
        {
            assertEquals(retryDelay, scheduleDueConnections());
            _currentTime.addAndGet(retryDelay);
            assertEquals(Long.MAX_VALUE, scheduleDueConnections());
            _threadPool.runNext();
        }

        assertEquals(1_000L, scheduleDueConnections());
        assertEquals(8, ticker.getTickCount());
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void futureTickerDeadlineClearsRetryBackoff()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        _connectionScheduler.registerConnection(connection);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runNext();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runNext();
        assertEquals(100L, scheduleDueConnections());

        ticker.setTimeToNextTick(FAR_FUTURE_TICK_DELAY_MILLIS);
        assertEquals(FAR_FUTURE_TICK_DELAY_MILLIS, scheduleDueConnections());

        ticker.setTimeToNextTick(-1);
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    void closingConnectionIsNotTicked()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        _connectionScheduler.registerConnection(connection);
        connection.close();

        assertEquals(1_000L, scheduleDueConnections());
        assertEquals(0, ticker.getTickCount());
        assertEquals(0, ticker.getTimeToNextTickCallCount());
        assertEquals(1, _threadPool.getQueueSize());
        _threadPool.runNext();
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void queuedTickBecomesNoOpWhenCloseStarts()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketConnection connection = createConnection(ticker);
        _connectionScheduler.registerConnection(connection);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(1, _threadPool.getQueueSize());
        connection.close();
        _threadPool.runNext();

        assertEquals(0, ticker.getTickCount());
        assertEquals(1, _threadPool.getQueueSize());
        _threadPool.runNext();
        assertEquals(0, _threadPool.getQueueSize());
    }

    private BlockingConnection createBlockingConnection(final Ticker ticker)
    {
        final AggregateTicker aggregateTicker = new AggregateTicker();
        aggregateTicker.addTicker(ticker);

        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);

        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return new BlockingConnection(_connectionScheduler, mock(Session.class), address, address,
                protocolEngine, _threadPool);
    }

    private static final class BlockingConnection extends WebSocketConnection
    {
        private final AtomicBoolean _blockNextRetryDelayRead = new AtomicBoolean();
        private final CountDownLatch _retryDelayRead = new CountDownLatch(1);
        private final CountDownLatch _continueRetryDelayRead = new CountDownLatch(1);

        private BlockingConnection(final WebSocketConnectionScheduler scheduler,
                                   final Session connection,
                                   final InetSocketAddress localAddress,
                                   final InetSocketAddress remoteAddress,
                                   final MultiVersionProtocolEngine protocolEngine,
                                   final ThreadPool threadPool)
        {
            super(connection, localAddress, remoteAddress, protocolEngine, threadPool, scheduler, SETTINGS);
        }

        @Override
        long getTickRetryDelay(final long currentTime)
        {
            final long retryDelay = super.getTickRetryDelay(currentTime);
            if (_blockNextRetryDelayRead.compareAndSet(true, false))
            {
                _retryDelayRead.countDown();
                await(_continueRetryDelayRead, "Timed out waiting to continue the retry-delay read");
            }
            return retryDelay;
        }

        private void blockNextRetryDelayRead()
        {
            _blockNextRetryDelayRead.set(true);
        }

        private void awaitRetryDelayRead()
        {
            await(_retryDelayRead, "Timed out waiting for the retry-delay read");
        }

        private void continueRetryDelayRead()
        {
            _continueRetryDelayRead.countDown();
        }
    }
}
