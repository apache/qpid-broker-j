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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketConnectionScheduler extends Thread
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnectionScheduler.class);

    private final Set<WebSocketConnection> _activeConnections = ConcurrentHashMap.newKeySet();
    private final LongSupplier _currentTimeSupplier;
    private final LongSupplier _nanoTimeSupplier;
    private final long _closeTimeoutNanos;
    private final AtomicBoolean _closed = new AtomicBoolean();

    private boolean _tickSubmissionFailureReported;
    private long _wakeupSequence;
    private long _tickCompletionTime = Long.MAX_VALUE;

    public WebSocketConnectionScheduler(final String threadName,
                                        final LongSupplier currentTimeSupplier,
                                        final LongSupplier nanoTimeSupplier,
                                        final long closeTimeoutNanos)
    {
        _currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier, "Current time supplier must not be null");
        _nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier, "Nano time supplier must not be null");
        _closeTimeoutNanos = closeTimeoutNanos;
        setName(threadName);
    }

    public void shutdown()
    {
        _closed.set(true);
        wakeup();
        for (final WebSocketConnection connection : _activeConnections)
        {
            try
            {
                connection.cleanupAfterShutdown();
            }
            catch (final RuntimeException e)
            {
                LOGGER.warn("Failed to clean up WebSocket connection during transport shutdown", e);
            }
        }
    }

    boolean isShutdown()
    {
        return _closed.get();
    }

    void registerConnection(final WebSocketConnection connection)
    {
        _activeConnections.add(connection);
        wakeup();
    }

    void unregisterConnection(final WebSocketConnection connection)
    {
        _activeConnections.remove(connection);
    }

    long scheduleDueConnections(final long currentTime)
    {
        long timeToNextTick = Long.MAX_VALUE;
        final long currentNanoTime = getNanoTime();
        RejectedExecutionException submissionFailure = null;
        boolean dueConnectionFound = false;
        boolean tickSubmissionSucceeded = false;
        for (final WebSocketConnection connection : _activeConnections)
        {
            if (connection.isClosed() || connection.isClosing())
            {
                timeToNextTick = Math.min(timeToNextTick, processClosingConnection(connection, currentNanoTime));
                continue;
            }

            final long timeToTick = connection.getTimeToNextTick(currentTime);
            if (timeToTick > 0L)
            {
                connection.tickNoLongerOverdue();
                timeToNextTick = Math.min(timeToNextTick, timeToTick);
                continue;
            }

            dueConnectionFound = true;
            final long retryDelay = connection.getTickRetryDelay(currentTime);
            if (retryDelay > 0L)
            {
                timeToNextTick = Math.min(timeToNextTick, retryDelay);
                continue;
            }

            try
            {
                final boolean tickScheduled = connection.tryScheduleTick(currentTime);
                tickSubmissionSucceeded |= tickScheduled;
                if (!tickScheduled)
                {
                    final long updatedRetryDelay = connection.getTickRetryDelay(currentTime);
                    if (updatedRetryDelay > 0L)
                    {
                        timeToNextTick = Math.min(timeToNextTick, updatedRetryDelay);
                    }
                }
            }
            catch (final RejectedExecutionException e)
            {
                if (submissionFailure == null)
                {
                    submissionFailure = e;
                }
                timeToNextTick = Math.min(timeToNextTick, connection.getTickRetryDelay(currentTime));
            }
        }
        reportTickSubmissionFailure(submissionFailure, tickSubmissionSucceeded, dueConnectionFound);
        return timeToNextTick;
    }

    private long processClosingConnection(final WebSocketConnection connection, final long currentNanoTime)
    {
        long closeDelay = Long.MAX_VALUE;
        if (!connection.isClosed())
        {
            closeDelay = connection.processClose(currentNanoTime);
            connection.doWrite();
            if (connection.isForceClosing())
            {
                closeDelay = connection.processClose(currentNanoTime);
            }
        }

        final long cleanupDelay = connection.scheduleCleanup(currentNanoTime);
        final long nextDelay = Math.min(closeDelay, cleanupDelay);
        return nextDelay == Long.MAX_VALUE ? Long.MAX_VALUE : toMillisCeiling(nextDelay);
    }

    private void reportTickSubmissionFailure(final RejectedExecutionException submissionFailure,
                                             final boolean tickSubmissionSucceeded,
                                             final boolean dueConnectionFound)
    {
        if (submissionFailure == null)
        {
            if (tickSubmissionSucceeded || !dueConnectionFound)
            {
                _tickSubmissionFailureReported = false;
            }
        }
        else if (!_closed.get() && !_tickSubmissionFailureReported)
        {
            _tickSubmissionFailureReported = true;
            LOGGER.warn("Failed to schedule WebSocket connection idle timeout processing; " +
                    "repeated failures will not be reported until scheduling recovers", submissionFailure);
        }
    }

    long getCurrentTime()
    {
        return _currentTimeSupplier.getAsLong();
    }

    long getNanoTime()
    {
        return _nanoTimeSupplier.getAsLong();
    }

    long getCloseTimeoutNanos()
    {
        return _closeTimeoutNanos;
    }

    @Override
    public void run()
    {
        while (!_closed.get())
        {
            final long wakeupSequence = beginScan();
            final long scanStartTimeNanos = getNanoTime();
            final long timeToNextTick = scheduleDueConnections(getCurrentTime());
            try
            {
                awaitNextTick(wakeupSequence, timeToNextTick, scanStartTimeNanos);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private synchronized long beginScan()
    {
        _tickCompletionTime = Long.MAX_VALUE;
        return _wakeupSequence;
    }

    private synchronized void awaitNextTick(final long wakeupSequence,
                                            final long timeToNextTick,
                                            final long scanStartTimeNanos)
            throws InterruptedException
    {
        final long scanDelayNanos = timeToNextTick == Long.MAX_VALUE ?
                Long.MAX_VALUE :
                TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeToNextTick));
        while (!_closed.get() && wakeupSequence == _wakeupSequence)
        {
            final long currentNanoTime = getNanoTime();
            final long scanDelayRemaining = scanDelayNanos == Long.MAX_VALUE ?
                    Long.MAX_VALUE :
                    getRemainingTimeNanos(currentNanoTime, scanStartTimeNanos, scanDelayNanos);
            final long currentTime = getCurrentTime();
            final long tickCompletionDelay = _tickCompletionTime == Long.MAX_VALUE ?
                    Long.MAX_VALUE :
                    TimeUnit.MILLISECONDS.toNanos(_tickCompletionTime > currentTime ? _tickCompletionTime - currentTime : 0L);
            final long waitTime = Math.min(scanDelayRemaining, tickCompletionDelay);
            if (waitTime == Long.MAX_VALUE)
            {
                awaitWakeup(0L);
            }
            else
            {
                if (waitTime <= 0L)
                {
                    break;
                }
                awaitWakeup(toMillisCeiling(waitTime));
            }
        }
    }

    /**
     * Called with the scheduler's monitor held; a zero timeout waits indefinitely.
     * Package visibility allows tests to observe the real wait without replacing the scheduling loop.
     */
    void awaitWakeup(final long timeoutMillis) throws InterruptedException
    {
        wait(timeoutMillis);
    }

    synchronized void tickCompleted(final long nextTickTime)
    {
        if (nextTickTime < _tickCompletionTime)
        {
            _tickCompletionTime = nextTickTime;
            notifyAll();
        }
    }

    synchronized void wakeup()
    {
        _wakeupSequence++;
        notifyAll();
    }

    static long getRemainingTimeNanos(final long currentTimeNanos,
                                      final long startTimeNanos,
                                      final long timeoutNanos)
    {
        final long elapsed = currentTimeNanos - startTimeNanos;
        if (elapsed < 0L)
        {
            return timeoutNanos;
        }
        return elapsed >= timeoutNanos ? 0L : timeoutNanos - elapsed;
    }

    private static long toMillisCeiling(final long nanoseconds)
    {
        if (nanoseconds <= 0L)
        {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds - 1L) + 1L;
    }
}
