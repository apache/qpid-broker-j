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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongPredicate;

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.TransactionTimeoutTicker;
import org.apache.qpid.server.transport.network.Ticker;

class WebSocketDeadlineUpdateTest extends WebSocketTestBase
{
    private static final long AWAIT_TIMEOUT_SECONDS = 10L;
    private static final int ITERATIONS_COUNT = 16;

    private final AtomicReference<Throwable> _schedulerFailure = new AtomicReference<>();
    private ObservedScheduler _observedScheduler;

    @BeforeEach
    public void beforeEach()
    {
        _observedScheduler = new ObservedScheduler();
        _connectionScheduler = _observedScheduler;
    }

    @AfterEach
    void afterEach() throws Exception
    {
        if (_connectionScheduler != null)
        {
            _connectionScheduler.shutdown();
            _connectionScheduler.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(_connectionScheduler.isAlive(), "Scheduler did not terminate after shutdown");
        }
        assertNull(_schedulerFailure.get(), "Scheduler failed on its background thread");
    }

    @Test
    void testRegistrationWakesIdleScheduler() throws Exception
    {
        startScheduler();
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 0L);

        final MutableTicker ticker = new MutableTicker(0);
        ticker.setOnTick(() -> ticker.setTimeToNextTick(FAR_FUTURE_TICK_DELAY_MILLIS));
        _connectionScheduler.registerConnection(createConnection(ticker));

        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
        _threadPool.runNext();
        assertEquals(1, ticker.getTickCount());
    }

    @Test
    void testProtocolWorkWakesWaitingScheduler() throws Exception
    {
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final MultiVersionProtocolEngine engine = createEngine(ticker);
        final WebSocketConnection connection = createConnection(engine);
        _connectionScheduler.registerConnection(connection);
        startScheduler();
        awaitScan(ticker, 0);

        for (int i = 0; i < ITERATIONS_COUNT; i++)
        {
            final int previous = ticker.getTimeToNextTickCallCount();
            runProtocolWork(engine, connection, () ->
            {
                // Completing ordinary protocol work must wake a sleeping scheduler.
            });
            awaitScan(ticker, previous);
        }
        assertEquals(0, _threadPool.getQueueSize());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEarlierDeadlinePublishedDuringScanIsNotLost(final boolean duringInitialScan) throws Exception
    {
        final AtomicInteger deadlineReads = new AtomicInteger();
        final AtomicInteger delay = new AtomicInteger(FAR_FUTURE_TICK_DELAY_MILLIS);
        final CountDownLatch calculating = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final int blockedRead = duringInitialScan ? 1 : 2;
        final Ticker ticker = new Ticker()
        {
            @Override
            public int getTimeToNextTick(final long currentTime)
            {
                final int result = delay.get();
                if (deadlineReads.incrementAndGet() == blockedRead)
                {
                    calculating.countDown();
                    await(release, "Deadline calculation was not released");
                }
                return result;
            }

            @Override
            public int tick(final long currentTime)
            {
                delay.set(FAR_FUTURE_TICK_DELAY_MILLIS);
                return FAR_FUTURE_TICK_DELAY_MILLIS;
            }
        };
        final MultiVersionProtocolEngine engine = createEngine(ticker);
        final WebSocketConnection connection = createConnection(engine);
        _connectionScheduler.registerConnection(connection);
        startScheduler();
        try
        {
            if (!duringInitialScan)
            {
                _observedScheduler.awaitWait(timeoutMillis -> deadlineReads.get() >= 1);
                runProtocolWork(engine, connection, () ->
                {
                    // Trigger another scan without changing its initial deadline.
                });
            }
            await(calculating, "Scheduler did not start the expected deadline calculation");
            runProtocolWork(engine, connection, () -> delay.set(0));
            release.countDown();

            _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
            _threadPool.runNext();
            assertEquals(FAR_FUTURE_TICK_DELAY_MILLIS, delay.get(), "The newly due ticker was not executed");
        }
        finally
        {
            release.countDown();
        }
    }

    @Test
    void testTickCompletionSchedulesNextDeadlineWithoutAnotherWakeup() throws Exception
    {
        final AtomicLong deadline = new AtomicLong(INITIAL_TIME);
        final AtomicInteger ticks = new AtomicInteger();
        final Ticker ticker = new Ticker()
        {
            @Override
            public int getTimeToNextTick(final long currentTime)
            {
                return (int) (deadline.get() - currentTime);
            }

            @Override
            public int tick(final long currentTime)
            {
                ticks.incrementAndGet();
                deadline.set(currentTime + 100L);
                return 100;
            }
        };
        _connectionScheduler.registerConnection(createConnection(ticker));
        startScheduler();
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 0L && _threadPool.getQueueSize() == 1);

        _threadPool.runNext();
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 100L);
        advanceTime(100L);

        // only tick completion supplied the deadline, advancing the clocks sends no notification
        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
        _threadPool.runNext();
        assertEquals(2, ticks.get());
    }

    @Test
    void testExistingTransactionTickerCanIntroduceAnEarlierDeadline() throws Exception
    {
        final AtomicLong transactionStarted = new AtomicLong();
        final AtomicLong expired = new AtomicLong();
        final AtomicInteger deadlineReads = new AtomicInteger();
        final Ticker ticker = new TransactionTimeoutTicker(100L, 100L, transactionStarted::get, expired::set)
        {
            @Override
            public int getTimeToNextTick(final long currentTime)
            {
                final int result = super.getTimeToNextTick(currentTime);
                deadlineReads.incrementAndGet();
                return result;
            }
        };
        final MultiVersionProtocolEngine engine = createEngine(ticker);
        final WebSocketConnection connection = createConnection(engine);
        _connectionScheduler.registerConnection(connection);
        startScheduler();
        _observedScheduler.awaitWait(timeoutMillis -> deadlineReads.get() > 0);

        final int previous = deadlineReads.get();
        runProtocolWork(engine, connection, () -> transactionStarted.set(INITIAL_TIME));
        _observedScheduler.awaitWait(timeoutMillis -> deadlineReads.get() > previous);
        assertEquals(0, _threadPool.getQueueSize());
        advanceTime(100L);

        // no further notification: protocol completion must already have shortened the scheduler's wait
        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
        _threadPool.runNext();
        assertEquals(100L, expired.get());
    }

    @Test
    void testOutstandingTickIsNotResubmittedDuringRepeatedWakeups() throws Exception
    {
        final MutableTicker ticker = new MutableTicker(0);
        _connectionScheduler.registerConnection(createConnection(ticker));
        startScheduler();
        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);

        for (int i = 0; i < ITERATIONS_COUNT; i++)
        {
            final int previous = ticker.getTimeToNextTickCallCount();
            _connectionScheduler.wakeup();
            awaitScan(ticker, previous);
            assertEquals(1, _threadPool.getQueueSize(), "A wakeup duplicated the outstanding tick job");
        }
    }

    @Test
    void testOverdueTickBackoffSurvivesRepeatedWakeups() throws Exception
    {
        final MutableTicker ticker = new MutableTicker(0);
        _connectionScheduler.registerConnection(createConnection(ticker));
        startScheduler();
        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
        _threadPool.runNext();
        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
        _threadPool.runNext();
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 100L && _threadPool.getQueueSize() == 0);

        for (int i = 0; i < ITERATIONS_COUNT; i++)
        {
            final int previous = ticker.getTimeToNextTickCallCount();
            _connectionScheduler.wakeup();
            awaitScan(ticker, previous);
            assertEquals(0, _threadPool.getQueueSize(), "A wakeup bypassed the overdue tick retry backoff");
        }
        advanceTime(100L);
        _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
        assertEquals(2, ticker.getTickCount());
    }

    @Test
    void testContinuousWakeupsCannotPostponeMonotonicCloseDeadline() throws Exception
    {
        final AtomicBoolean repeat = new AtomicBoolean();
        final AtomicInteger refreshes = new AtomicInteger();
        final CountDownLatch disconnected = new CountDownLatch(1);
        final CountDownLatch trafficStarted = new CountDownLatch(100);
        final Ticker ticker = createTrafficTicker(repeat, refreshes, trafficStarted, 10L, FAR_FUTURE_TICK_DELAY_MILLIS);
        _connectionScheduler.registerConnection(createConnection(ticker));
        final WebSocketConnection closing = createClosingConnection(repeat, disconnected);
        _connectionScheduler.registerConnection(closing);
        closing.close();
        startScheduler();
        try
        {
            _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis > 0L);
            _currentTime.addAndGet(-TimeUnit.DAYS.toMillis(1L));
            repeat.set(true);
            _connectionScheduler.wakeup();
            await(trafficStarted, "The scheduler did not process repeated wakeups");
            await(disconnected, "Continuous wakeups postponed the close deadline");
            assertTrue(refreshes.get() >= 100 && refreshes.get() <= 101,
                       "The monotonic close deadline was extended during repeated wakeups");
        }
        finally
        {
            repeat.set(false);
        }
    }

    @Test
    void testContinuouslyDueConnectionCannotStarveCloseNotification() throws Exception
    {
        final AtomicBoolean repeat = new AtomicBoolean();
        final AtomicInteger refreshes = new AtomicInteger();
        final CountDownLatch disconnected = new CountDownLatch(1);
        final CountDownLatch trafficStarted = new CountDownLatch(100);
        final Ticker ticker = createTrafficTicker(repeat, refreshes, trafficStarted, 2L, 0);
        _connectionScheduler.registerConnection(createConnection(ticker));
        final WebSocketConnection closing = createClosingConnection(repeat, disconnected);
        _connectionScheduler.registerConnection(closing);
        startScheduler();
        try
        {
            _observedScheduler.awaitWait(timeoutMillis -> _threadPool.getQueueSize() == 1);
            repeat.set(true);
            _connectionScheduler.wakeup();
            await(trafficStarted, "The scheduler did not process the continuously due connection");
            closing.close();
            await(disconnected, "Continuous due-connection scans starved the close notification or deadline");
        }
        finally
        {
            repeat.set(false);
        }
    }

    @Test
    void testStalledWriteDoesNotExtendCloseDeadline() throws Exception
    {
        final Session session = mock(Session.class);
        final WebSocketConnection connection = createConnection(new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS), session);
        final CountDownLatch disconnected = new CountDownLatch(1);
        final Runnable closeTask = mock(Runnable.class);
        doAnswer(invocation ->
        {
            connection.webSocketClosed(closeTask);
            disconnected.countDown();
            return null;
        }).when(session).disconnect();
        startScheduler();
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 0L);

        send(connection, new byte[] {1});
        connection.doWrite();
        _threadPool.runNext();
        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(any(ByteBuffer.class), callbackCaptor.capture());

        // leave the submitted write unfinished and queue more output behind it
        final QpidByteBuffer buffer = mock(QpidByteBuffer.class);
        final QpidByteBuffer duplicate = mock(QpidByteBuffer.class);
        when(buffer.remaining()).thenReturn(1);
        when(buffer.limit()).thenReturn(1);
        when(buffer.duplicate()).thenReturn(duplicate);
        connection.send(buffer);
        connection.doWrite();
        assertEquals(0, _threadPool.getQueueSize(), "An unfinished write must hold back subsequent output");

        connection.close();
        _connectionScheduler.registerConnection(connection);
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 1_000L);
        verify(session, never()).disconnect();
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        verify(duplicate, never()).dispose();

        // scheduler must wake for the deadline without a callback, incoming traffic or another notification
        advanceTime(1_000L);
        await(disconnected, "An unfinished write prevented the scheduler from enforcing the close deadline");
        _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 0L && _threadPool.getQueueSize() == 1);
        assertTrue(connection.isClosed(), "The expired connection did not finish transport closure");
        _threadPool.runNext();
        verify(duplicate).dispose();
        verify(closeTask).run();

        // completion arriving after forced closure must not restart output or repeat cleanup
        callbackCaptor.getValue().succeed();
        _threadPool.runAll();
        verify(session).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));
        verify(session).disconnect();
        verify(duplicate).dispose();
        verify(closeTask).run();
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void testConnectionUnregisteredDuringScanIsNotScannedAgain() throws Exception
    {
        final AtomicInteger deadlineReads = new AtomicInteger();
        final CountDownLatch calculating = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Ticker ticker = new Ticker()
        {
            @Override
            public int getTimeToNextTick(final long currentTime)
            {
                deadlineReads.incrementAndGet();
                calculating.countDown();
                await(release, "Deadline calculation was not released");
                return FAR_FUTURE_TICK_DELAY_MILLIS;
            }

            @Override
            public int tick(final long currentTime)
            {
                return FAR_FUTURE_TICK_DELAY_MILLIS;
            }
        };
        final WebSocketConnection connection = createConnection(ticker);
        _connectionScheduler.registerConnection(connection);
        startScheduler();
        try
        {
            await(calculating, "Scheduler did not start the deadline calculation");
            _connectionScheduler.unregisterConnection(connection);
            _connectionScheduler.wakeup();
            release.countDown();
            _observedScheduler.awaitWait(timeoutMillis -> timeoutMillis == 0L);
            assertEquals(1, deadlineReads.get(), "An unregistered connection was scanned again");
            assertEquals(0, _threadPool.getQueueSize());
        }
        finally
        {
            release.countDown();
        }
    }

    private MultiVersionProtocolEngine createEngine(final Ticker ticker)
    {
        final AggregateTicker aggregate = new AggregateTicker();
        aggregate.addTicker(ticker);
        final MultiVersionProtocolEngine engine = mock(MultiVersionProtocolEngine.class);
        when(engine.getAggregateTicker()).thenReturn(aggregate);
        when(engine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());
        return engine;
    }

    private void runProtocolWork(final MultiVersionProtocolEngine engine,
                                 final WebSocketConnection connection,
                                 final Runnable work)
    {
        when(engine.processPendingIterator()).thenAnswer(invocation -> List.of(work).iterator());
        connection.scheduleWork();
        _threadPool.runNext();
    }

    private Ticker createTrafficTicker(final AtomicBoolean repeat,
                                       final AtomicInteger refreshes,
                                       final CountDownLatch trafficStarted,
                                       final long elapsedMillis,
                                       final int timeToNextTick)
    {
        return new Ticker()
        {
            @Override
            public int getTimeToNextTick(final long currentTime)
            {
                if (repeat.get())
                {
                    refreshes.incrementAndGet();
                    _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(elapsedMillis));
                    _connectionScheduler.wakeup();
                    trafficStarted.countDown();
                }
                return timeToNextTick;
            }

            @Override
            public int tick(final long currentTime)
            {
                return timeToNextTick;
            }
        };
    }

    private WebSocketConnection createClosingConnection(final AtomicBoolean repeat,
                                                        final CountDownLatch disconnected)
    {
        final Session session = mock(Session.class);
        doAnswer(invocation ->
        {
            repeat.set(false);
            disconnected.countDown();
            return null;
        }).when(session).disconnect();
        return createConnection(new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS), session);
    }

    private void startScheduler()
    {
        _connectionScheduler.setUncaughtExceptionHandler((thread, failure) ->
        {
            _schedulerFailure.set(failure);
            _observedScheduler.signalWaiters();
        });
        _connectionScheduler.start();
    }

    private void advanceTime(final long elapsedMillis)
    {
        _currentTime.addAndGet(elapsedMillis);
        _nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(elapsedMillis));
    }

    private void awaitScan(final MutableTicker ticker, final int previous) throws Exception
    {
        _observedScheduler.awaitWait(timeoutMillis -> ticker.getTimeToNextTickCallCount() > previous);
    }

    private class ObservedScheduler extends WebSocketConnectionScheduler
    {
        private final Semaphore _waitStarted = new Semaphore(0);

        private boolean _waiting;
        private long _timeoutMillis;

        private ObservedScheduler()
        {
            super("WebSocket Deadline Update Test", _currentTime::get, _nanoTime::get,
                  TimeUnit.SECONDS.toNanos(1L));
        }

        @Override
        void awaitWakeup(final long timeoutMillis) throws InterruptedException
        {
            assertTrue(Thread.holdsLock(this), "The wait hook must run with the scheduler's monitor held");
            _timeoutMillis = timeoutMillis;
            _waiting = true;
            signalWaiters();
            try
            {
                super.awaitWakeup(timeoutMillis);
            }
            finally
            {
                _waiting = false;
            }
        }

        private void signalWaiters()
        {
            _waitStarted.release();
        }

        private void awaitWait(final LongPredicate condition) throws InterruptedException
        {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_TIMEOUT_SECONDS);
            while (true)
            {
                synchronized (this)
                {
                    assertNull(_schedulerFailure.get(), "Scheduler failed before reaching the expected wait");
                    assertTrue(isAlive(), "Scheduler stopped before reaching the expected wait");

                    if (_waiting && condition.test(_timeoutMillis))
                    {
                        return;
                    }

                    _waitStarted.drainPermits();
                }
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0L || !_waitStarted.tryAcquire(remaining, TimeUnit.NANOSECONDS))
                {
                    assertNull(_schedulerFailure.get(), "Scheduler failed before reaching the expected wait");
                    fail("Scheduler did not reach the expected wait within " + AWAIT_TIMEOUT_SECONDS + " seconds");
                }
            }
        }
    }
}
