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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.test.utils.UnitTestBase;

abstract class WebSocketTestBase extends UnitTestBase
{
    protected static final int SUBMISSION_COUNT = 100_000;
    protected static final int WRITE_BATCH_SIZE = 256 * 1024;
    protected static final long INITIAL_TIME = 1_000_000L;
    protected static final WebSocketSettings SETTINGS = new WebSocketSettings(WRITE_BATCH_SIZE);
    // longer than the test's wait, so timer expiry cannot hide a missed wakeup
    protected static final int FAR_FUTURE_TICK_DELAY_MILLIS = 37_000;

    protected TestThreadPool _threadPool;
    protected WebSocketConnectionScheduler _connectionScheduler;
    protected AtomicLong _currentTime;
    protected AtomicLong _nanoTime;

    @BeforeEach
    public void init()
    {
        _threadPool = new TestThreadPool();
        _currentTime = new AtomicLong(INITIAL_TIME);
        _nanoTime = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(INITIAL_TIME));
        _connectionScheduler = new WebSocketConnectionScheduler("WebSocket Idle Checker Test", _currentTime::get,
                _nanoTime::get, TimeUnit.SECONDS.toNanos(1L));
    }

    protected boolean tryScheduleTick(final WebSocketConnection connection)
    {
        return connection.tryScheduleTick(_currentTime.get());
    }

    protected long scheduleDueConnections()
    {
        return _connectionScheduler.scheduleDueConnections(_currentTime.get());
    }

    protected WebSocketConnection createConnection(final Ticker ticker)
    {
        return createConnection(ticker, mock(Session.class));
    }

    protected WebSocketConnection createConnection(final Ticker ticker, final Session session)
    {
        final AggregateTicker aggregateTicker = new AggregateTicker();
        aggregateTicker.addTicker(ticker);

        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        when(protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);

        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return new WebSocketConnection(session, address, address, protocolEngine, _threadPool, _connectionScheduler,
                SETTINGS);
    }

    protected void send(final WebSocketConnection connection, final byte[] data)
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(data))
        {
            connection.send(buffer);
        }
    }

    protected WebSocketConnection createConnection(final MultiVersionProtocolEngine protocolEngine)
    {
        return createConnection(protocolEngine, mock(Session.class));
    }

    protected WebSocketConnection createConnection(final MultiVersionProtocolEngine protocolEngine,
                                                   final Session session)
    {
        final InetSocketAddress address = new InetSocketAddress("localhost", 0);
        return new WebSocketConnection(session, address, address, protocolEngine, _threadPool, _connectionScheduler,
                SETTINGS);
    }

    protected Thread startDaemonThread(final FutureTask<?> task, final String name)
    {
        final Thread thread = new Thread(task, getTestName() + "-" + name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    protected static void await(final CountDownLatch latch, final String timeoutMessage)
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

    protected static final class MutableTicker implements Ticker
    {
        private final AtomicInteger _tickCount = new AtomicInteger();
        private final AtomicInteger _timeToNextTickCallCount = new AtomicInteger();
        private volatile int _timeToNextTick;
        private Runnable _onTick = () ->
        {
        };

        MutableTicker(final int timeToNextTick)
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

        void setOnTick(final Runnable onTick)
        {
            _onTick = onTick;
        }

        void setTimeToNextTick(final int timeToNextTick)
        {
            _timeToNextTick = timeToNextTick;
        }

        int getTickCount()
        {
            return _tickCount.get();
        }

        int getTimeToNextTickCallCount()
        {
            return _timeToNextTickCallCount.get();
        }
    }

    protected static final class TestThreadPool implements ThreadPool
    {
        private final Queue<Runnable> _jobs = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean _rejectNext = new AtomicBoolean();
        private final AtomicBoolean _failNextWithError = new AtomicBoolean();

        @Override
        public void execute(final Runnable job)
        {
            if (_rejectNext.compareAndSet(true, false))
            {
                throw new RejectedExecutionException("Rejected for test");
            }
            if (_failNextWithError.compareAndSet(true, false))
            {
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

        void rejectNext()
        {
            _rejectNext.set(true);
        }

        void failNextWithError()
        {
            _failNextWithError.set(true);
        }

        int getQueueSize()
        {
            return _jobs.size();
        }

        void runNext()
        {
            _jobs.remove().run();
        }

        void runAll()
        {
            while (!_jobs.isEmpty())
            {
                runNext();
            }
        }
    }
}
