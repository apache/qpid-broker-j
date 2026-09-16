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
package org.apache.qpid.server.transport.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.test.utils.UnitTestBase;

class WebSocketProviderTest extends UnitTestBase
{
    private static final int SUBMISSION_COUNT = 100_000;
    private static final long INITIAL_TIME = 1_000_000L;

    private TestThreadPool _threadPool;
    private WebSocketProvider _provider;
    private AtomicLong _currentTime;

    @BeforeEach
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setUp()
    {
        _threadPool = new TestThreadPool();
        _currentTime = new AtomicLong(INITIAL_TIME);

        final Broker<?> broker = mock(Broker.class);
        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getParent()).thenReturn((Broker) broker);

        _provider = new WebSocketProvider(Transport.WS, null, port, Set.of(Protocol.AMQP_1_0),
                                          Protocol.AMQP_1_0, _currentTime::get);
    }

    @Test
    public void testTickSubmissionsAreCoalescedWhileQueued()
    {
        final WebSocketProvider.ConnectionWrapper connection = createConnection(new MutableTicker(-1));

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
    public void testTickRemainsCoalescedUntilJobCompletes()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketProvider.ConnectionWrapper connection = createConnection(ticker);
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
    public void testCompletionBackoffCannotBeBypassedBetweenEligibilityCheckAndClaim() throws Exception
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final BlockingConnectionWrapper connection = createBlockingConnection(ticker);

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
    public void testTickRefreshesNextDeadlineAfterProcessing()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketProvider.ConnectionWrapper connection = createConnection(ticker);
        ticker.setOnTick(() -> ticker.setTimeToNextTick(37_000));

        assertTrue(tryScheduleTick(connection));
        _threadPool.runNext();

        assertEquals(1, ticker.getTickCount());
        assertEquals(1, ticker.getTimeToNextTickCallCount());
    }

    @Test
    public void testSubmissionFailureDoesNotLeaveTickOutstanding()
    {
        final WebSocketProvider.ConnectionWrapper connection = createConnection(new MutableTicker(-1));
        _threadPool.rejectNext();

        assertThrows(RejectedExecutionException.class, () -> tryScheduleTick(connection));

        assertFalse(tryScheduleTick(connection));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    public void testSubmissionErrorDoesNotLeaveTickOutstanding()
    {
        final WebSocketProvider.ConnectionWrapper connection = createConnection(new MutableTicker(-1));
        _threadPool.failNextWithError();

        assertThrows(AssertionError.class, () -> tryScheduleTick(connection));

        assertFalse(tryScheduleTick(connection));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    public void testTickFailureDoesNotLeaveTickOutstanding()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketProvider.ConnectionWrapper connection = createConnection(ticker);
        ticker.setOnTick(() ->
        {
            throw new IllegalStateException("Ticker failure");
        });

        assertTrue(tryScheduleTick(connection));
        assertThrows(IllegalStateException.class, _threadPool::runNext);

        ticker.setOnTick(() -> { });
        assertFalse(tryScheduleTick(connection));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(connection));
        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    public void testEveryDueConnectionIsScheduled()
    {
        final WebSocketProvider.ConnectionWrapper first = createConnection(new MutableTicker(-1));
        final WebSocketProvider.ConnectionWrapper second = createConnection(new MutableTicker(0));
        final WebSocketProvider.ConnectionWrapper future = createConnection(new MutableTicker(37_000));
        _provider.registerConnection(first);
        _provider.registerConnection(second);
        _provider.registerConnection(future);

        final long timeToNextTick = scheduleDueConnections();

        assertEquals(37_000, timeToNextTick);
        assertEquals(2, _threadPool.getQueueSize());
    }

    @Test
    public void testPendingDueConnectionIsNotResubmitted()
    {
        final WebSocketProvider.ConnectionWrapper connection = createConnection(new MutableTicker(-1));
        _provider.registerConnection(connection);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());

        assertEquals(1, _threadPool.getQueueSize());
    }

    @Test
    public void testRejectedSubmissionDoesNotPreventOtherDueConnectionsFromBeingScheduled()
    {
        final WebSocketProvider.ConnectionWrapper first = createConnection(new MutableTicker(-1));
        final WebSocketProvider.ConnectionWrapper second = createConnection(new MutableTicker(-1));
        _provider.registerConnection(first);
        _provider.registerConnection(second);
        _threadPool.rejectNext();

        final long timeToNextTick = scheduleDueConnections();

        assertEquals(100L, timeToNextTick);
        assertEquals(1, _threadPool.getQueueSize());
        assertEquals(100L, scheduleDueConnections());
        assertEquals(1, _threadPool.getQueueSize());
        assertFalse(tryScheduleTick(first));
        _currentTime.addAndGet(100L);
        assertTrue(tryScheduleTick(first));
        assertEquals(2, _threadPool.getQueueSize());
    }

    @Test
    public void testOverdueTickRetryBackoffCannotBeBypassedByRepeatedScans()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketProvider.ConnectionWrapper connection = createConnection(ticker);
        _provider.registerConnection(connection);

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
    public void testFutureTickerDeadlineClearsRetryBackoff()
    {
        final MutableTicker ticker = new MutableTicker(-1);
        final WebSocketProvider.ConnectionWrapper connection = createConnection(ticker);
        _provider.registerConnection(connection);

        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runNext();
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        _threadPool.runNext();
        assertEquals(100L, scheduleDueConnections());

        ticker.setTimeToNextTick(37_000);
        assertEquals(37_000L, scheduleDueConnections());

        ticker.setTimeToNextTick(-1);
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        assertEquals(1, _threadPool.getQueueSize());
    }

    private boolean tryScheduleTick(final WebSocketProvider.ConnectionWrapper connection)
    {
        return connection.tryScheduleTick(_currentTime.get());
    }

    private long scheduleDueConnections()
    {
        return _provider.scheduleDueConnections(_currentTime.get());
    }

    private WebSocketProvider.ConnectionWrapper createConnection(final Ticker ticker)
    {
        final AggregateTicker aggregateTicker = new AggregateTicker();
        aggregateTicker.addTicker(ticker);

        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);

        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return _provider.new ConnectionWrapper(mock(Session.class), address, address, protocolEngine, _threadPool);
    }

    private BlockingConnectionWrapper createBlockingConnection(final Ticker ticker)
    {
        final AggregateTicker aggregateTicker = new AggregateTicker();
        aggregateTicker.addTicker(ticker);

        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);

        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return new BlockingConnectionWrapper(_provider, mock(Session.class), address, address, protocolEngine,
                                             _threadPool);
    }

    private void startDaemonThread(final FutureTask<?> task, final String name)
    {
        final Thread thread = new Thread(task, getTestName() + "-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    private static void await(final CountDownLatch latch, final String timeoutMessage)
    {
        try
        {
            if (!latch.await(10L, TimeUnit.SECONDS))
            {
                throw new AssertionError(timeoutMessage);
            }
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class BlockingConnectionWrapper extends WebSocketProvider.ConnectionWrapper
    {
        private final AtomicBoolean _blockNextRetryDelayRead = new AtomicBoolean();
        private final CountDownLatch _retryDelayRead = new CountDownLatch(1);
        private final CountDownLatch _continueRetryDelayRead = new CountDownLatch(1);

        private BlockingConnectionWrapper(final WebSocketProvider provider,
                                          final Session connection,
                                          final InetSocketAddress localAddress,
                                          final InetSocketAddress remoteAddress,
                                          final MultiVersionProtocolEngine protocolEngine,
                                          final ThreadPool threadPool)
        {
            provider.super(connection, localAddress, remoteAddress, protocolEngine, threadPool);
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

    private static final class MutableTicker implements Ticker
    {
        private final AtomicInteger _tickCount = new AtomicInteger();
        private final AtomicInteger _timeToNextTickCallCount = new AtomicInteger();
        private volatile int _timeToNextTick;
        private Runnable _onTick = () -> { };

        private MutableTicker(final int timeToNextTick)
        {
            _timeToNextTick = timeToNextTick;
        }

        @Override
        public int getTimeToNextTick(final long currentTime)
        {
            _timeToNextTickCallCount.incrementAndGet();
            return _timeToNextTick;
        }

        @Override
        public int tick(final long currentTime)
        {
            _tickCount.incrementAndGet();
            _onTick.run();
            return _timeToNextTick;
        }

        private void setOnTick(final Runnable onTick)
        {
            _onTick = onTick;
        }

        private void setTimeToNextTick(final int timeToNextTick)
        {
            _timeToNextTick = timeToNextTick;
        }

        private int getTickCount()
        {
            return _tickCount.get();
        }

        private int getTimeToNextTickCallCount()
        {
            return _timeToNextTickCallCount.get();
        }
    }

    private static final class TestThreadPool implements ThreadPool
    {
        private final Queue<Runnable> _jobs = new ArrayDeque<>();
        private boolean _rejectNext;
        private boolean _failNextWithError;

        @Override
        public void execute(final Runnable job)
        {
            if (_rejectNext)
            {
                _rejectNext = false;
                throw new RejectedExecutionException("Rejected for test");
            }
            if (_failNextWithError)
            {
                _failNextWithError = false;
                throw new AssertionError("Failed for test");
            }
            _jobs.add(job);
        }

        @Override
        public void join()
        {
        }

        @Override
        public int getThreads()
        {
            return 0;
        }

        @Override
        public int getIdleThreads()
        {
            return 0;
        }

        @Override
        public boolean isLowOnThreads()
        {
            return false;
        }

        private void rejectNext()
        {
            _rejectNext = true;
        }

        private void failNextWithError()
        {
            _failNextWithError = true;
        }

        private int getQueueSize()
        {
            return _jobs.size();
        }

        private void runNext()
        {
            _jobs.remove().run();
        }
    }
}
