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

import java.util.Deque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.thread.ThreadPool;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

final class CleanupJob implements Runnable
{
    private static final long CLEANUP_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private final WebSocketConnection _connection;
    private final ThreadPool _threadPool;
    private final WebSocketConnectionScheduler _scheduler;
    private final Object _transportLock;

    private Deque<QpidByteBuffer> _discardedBuffers;
    private Runnable _closeTask;
    private boolean _cleanupJobOutstanding;
    private boolean _cleanupJobRunning;
    private boolean _cleanupSubmissionFailed;
    private long _cleanupSubmissionTimeNanos;
    private volatile boolean _cleanupPending;
    private boolean _cleanupDeferred;

    CleanupJob(final WebSocketConnection connection,
               final ThreadPool threadPool,
               final WebSocketConnectionScheduler scheduler,
               final Object transportLock)
    {
        _connection = connection;
        _threadPool = threadPool;
        _scheduler = scheduler;
        _transportLock = transportLock;
    }

    void discardBuffers(final Deque<QpidByteBuffer> buffers)
    {
        synchronized (_transportLock)
        {
            if (!buffers.isEmpty())
            {
                _discardedBuffers = buffers;
                _cleanupPending = true;
            }
        }
    }

    void setCloseTask(final Runnable closeTask)
    {
        synchronized (_transportLock)
        {
            _closeTask = closeTask;
            _cleanupPending = true;
        }
    }

    long schedule(final long currentNanoTime)
    {
        if (!_cleanupPending)
        {
            return Long.MAX_VALUE;
        }

        synchronized (_transportLock)
        {
            if (!_cleanupPending || _cleanupJobOutstanding || (_discardedBuffers == null && _cleanupDeferred))
            {
                return Long.MAX_VALUE;
            }
            if (_cleanupSubmissionFailed)
            {
                final long remaining = WebSocketConnectionScheduler
                        .getRemainingTimeNanos(currentNanoTime, _cleanupSubmissionTimeNanos, CLEANUP_RETRY_DELAY_NANOS);
                if (remaining > 0L)
                {
                    return remaining;
                }
            }
            _cleanupJobOutstanding = true;
        }

        boolean submitted = false;
        try
        {
            _threadPool.execute(this);
            submitted = true;
            synchronized (_transportLock)
            {
                _cleanupSubmissionFailed = false;
            }
            return Long.MAX_VALUE;
        }
        catch (final RejectedExecutionException ignore)
        {
            return CLEANUP_RETRY_DELAY_NANOS;
        }
        finally
        {
            if (!submitted)
            {
                synchronized (_transportLock)
                {
                    _cleanupJobOutstanding = false;
                    _cleanupSubmissionFailed = true;
                    _cleanupSubmissionTimeNanos = _scheduler.getNanoTime();
                }
            }
        }
    }

    void protocolUnlocked()
    {
        if (!_cleanupPending)
        {
            return;
        }
        final boolean cleanupDeferred;
        synchronized (_transportLock)
        {
            cleanupDeferred = _cleanupDeferred;
            _cleanupDeferred = false;
        }
        if (!cleanupDeferred)
        {
            return;
        }
        if (_scheduler.isShutdown())
        {
            run();
        }
        else
        {
            _scheduler.wakeup();
        }
    }

    @Override
    public void run()
    {
        final Deque<QpidByteBuffer> discardedBuffers;
        synchronized (_transportLock)
        {
            if (!_cleanupPending || _cleanupJobRunning)
            {
                return;
            }
            _cleanupJobRunning = true;
            discardedBuffers = _discardedBuffers;
            _discardedBuffers = null;
        }
        try
        {
            disposeBuffers(discardedBuffers);

            runCloseTaskIfReady();
        }
        finally
        {
            cleanupCompleted();
        }
    }

    private void disposeBuffers(final Deque<QpidByteBuffer> discardedBuffers)
    {
        if (discardedBuffers != null)
        {
            QpidByteBuffer buffer;
            while ((buffer = discardedBuffers.poll()) != null)
            {
                buffer.dispose();
            }
        }
    }

    private void runCloseTaskIfReady()
    {
        final Runnable closeTask;
        synchronized (_transportLock)
        {
            if (_closeTask != null && _connection.tryLockProtocol())
            {
                closeTask = _closeTask;
                _closeTask = null;
            }
            else
            {
                closeTask = null;
                _cleanupDeferred = _closeTask != null;
            }
        }
        if (closeTask != null)
        {
            try
            {
                closeTask.run();
            }
            finally
            {
                _connection.unlockProtocol();
            }
        }
    }

    private void cleanupCompleted()
    {
        final boolean closedAndCleaned;
        final boolean retryAfterShutdown;
        synchronized (_transportLock)
        {
            _cleanupJobOutstanding = false;
            _cleanupJobRunning = false;
            _cleanupPending = _discardedBuffers != null || _closeTask != null;
            closedAndCleaned = _connection.isClosed() && !_cleanupPending;
            retryAfterShutdown = _scheduler.isShutdown() && _cleanupPending && !_cleanupDeferred;
        }
        if (closedAndCleaned)
        {
            _scheduler.unregisterConnection(_connection);
        }
        if (retryAfterShutdown)
        {
            run();
        }
        else
        {
            _scheduler.wakeup();
        }
    }
}
