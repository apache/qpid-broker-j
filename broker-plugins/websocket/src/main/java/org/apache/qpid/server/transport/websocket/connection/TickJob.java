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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.util.thread.ThreadPool;

import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.network.Ticker;

final class TickJob implements Runnable
{
    private static final int IMMEDIATE_OVERDUE_TICK_RETRY_LIMIT = 1;
    private static final long INITIAL_TICK_RETRY_BACKOFF_MILLIS = 100L;
    private static final long MAXIMUM_TICK_RETRY_BACKOFF_MILLIS = 1_000L;

    private final WebSocketConnection _connection;
    private final MultiVersionProtocolEngine _protocolEngine;
    private final ThreadPool _threadPool;
    private final WebSocketConnectionScheduler _scheduler;
    private final AtomicBoolean _tickOutstanding = new AtomicBoolean();
    private final AtomicInteger _consecutiveTickRetries = new AtomicInteger();

    private volatile long _tickNotBeforeTime;

    TickJob(final WebSocketConnection connection,
            final MultiVersionProtocolEngine protocolEngine,
            final ThreadPool threadPool,
            final WebSocketConnectionScheduler scheduler)
    {
        _connection = connection;
        _protocolEngine = protocolEngine;
        _threadPool = threadPool;
        _scheduler = scheduler;
    }

    @Override
    public void run()
    {
        long nextTickTime = 0L;
        boolean tickSucceeded = false;
        try
        {
            nextTickTime = processTick();
            tickSucceeded = true;
        }
        finally
        {
            final long currentTime = _scheduler.getCurrentTime();
            final long tickRetryTime = tickSucceeded
                    ? processTickResult(nextTickTime, currentTime)
                    : deferTick(currentTime, false);
            completeTick(tickRetryTime);
        }
    }

    boolean trySchedule(final long currentTime)
    {
        if (!_connection.isOpen() || _connection.getTickRetryDelay(currentTime) > 0L)
        {
            return false;
        }

        if (_tickOutstanding.compareAndSet(false, true))
        {
            boolean submissionAttempted = false;
            boolean submitted = false;
            try
            {
                if (!_connection.isOpen() || _connection.getTickRetryDelay(_scheduler.getCurrentTime()) > 0L)
                {
                    return false;
                }

                submissionAttempted = true;
                _threadPool.execute(this);
                submitted = true;
                return true;
            }
            finally
            {
                if (!submitted)
                {
                    if (submissionAttempted)
                    {
                        final long tickRetryTime = deferTick(_scheduler.getCurrentTime(), false);
                        completeTick(tickRetryTime);
                    }
                    else
                    {
                        _tickOutstanding.set(false);
                    }
                }
            }
        }
        return false;
    }

    long getRetryDelay(final long currentTime)
    {
        final long tickNotBeforeTime = _tickNotBeforeTime;
        return tickNotBeforeTime > currentTime ? tickNotBeforeTime - currentTime : 0L;
    }

    void noLongerOverdue()
    {
        if (_tickNotBeforeTime != 0L)
        {
            resetTickRetry();
        }
    }

    private void completeTick(final long nextTickTime)
    {
        _tickOutstanding.set(false);
        _scheduler.tickCompleted(nextTickTime);
    }

    private long processTick()
    {
        long nextTickTime = Long.MAX_VALUE;
        _connection.lockProtocol();
        try
        {
            if (_connection.isOpen())
            {
                final Ticker ticker = _protocolEngine.getAggregateTicker();
                ticker.tick(_scheduler.getCurrentTime());

                if (_connection.isOpen())
                {
                    final long currentTime = _scheduler.getCurrentTime();
                    final long timeToNextTick = ticker.getTimeToNextTick(currentTime);
                    nextTickTime = timeToNextTick <= 0L ? currentTime : currentTime + timeToNextTick;
                }
            }
        }
        finally
        {
            _connection.unlockProtocol();
            _connection.doWrite();
        }
        return nextTickTime;
    }

    private long processTickResult(final long nextTickTime, final long currentTime)
    {
        if (nextTickTime > currentTime)
        {
            resetTickRetry();
            return nextTickTime;
        }

        return deferTick(currentTime, true);
    }

    private long deferTick(final long currentTime, final boolean immediateRetryAllowed)
    {
        final int retryCount = _consecutiveTickRetries.incrementAndGet();
        final int backoffRetryCount = immediateRetryAllowed
                ? retryCount - IMMEDIATE_OVERDUE_TICK_RETRY_LIMIT
                : retryCount;
        final long retryDelay = backoffRetryCount <= 0 ? 0L : calculateTickRetryBackoff(backoffRetryCount);
        final long retryTime = currentTime > Long.MAX_VALUE - retryDelay
                ? Long.MAX_VALUE
                : currentTime + retryDelay;
        _tickNotBeforeTime = retryTime;
        return retryTime;
    }

    private long calculateTickRetryBackoff(final int retryCount)
    {
        long retryDelay = INITIAL_TICK_RETRY_BACKOFF_MILLIS;
        for (int i = 1; i < retryCount && retryDelay < MAXIMUM_TICK_RETRY_BACKOFF_MILLIS; i++)
        {
            retryDelay = Math.min(retryDelay * 2L, MAXIMUM_TICK_RETRY_BACKOFF_MILLIS);
        }
        return retryDelay;
    }

    private void resetTickRetry()
    {
        _consecutiveTickRetries.set(0);
        _tickNotBeforeTime = 0L;
    }
}
