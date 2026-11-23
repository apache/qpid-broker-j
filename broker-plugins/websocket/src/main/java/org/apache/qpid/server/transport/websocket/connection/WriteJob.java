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

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

final class WriteJob implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteJob.class);

    private final WebSocketConnection _connection;
    private final Session _session;
    private final ThreadPool _threadPool;
    private final WebSocketConnectionScheduler _scheduler;
    private final CleanupJob _cleanupJob;
    private final WebSocketWriteQueue _queue;
    private final WriteCallback _writeCallback;
    private final CloseCallback _closeCallback;

    private boolean _discardPendingWrites;
    private boolean _writePending;
    private boolean _writeJobOutstanding;
    private boolean _finalWriteAttemptPending;

    WriteJob(final WebSocketConnection connection,
             final Session session,
             final ThreadPool threadPool,
             final WebSocketConnectionScheduler scheduler,
             final WebSocketWriteQueue queue,
             final CleanupJob cleanupJob)
    {
        _connection = connection;
        _session = session;
        _threadPool = threadPool;
        _scheduler = scheduler;
        _cleanupJob = cleanupJob;
        _queue = queue;
        _writeCallback = new WriteCallback(connection, this, scheduler);
        _closeCallback = new CloseCallback(connection, scheduler);
    }

    void send(final QpidByteBuffer msg)
    {
        synchronized (_queue)
        {
            final int remaining = msg.remaining();
            if (_connection.isOpen() && remaining > 0)
            {
                _queue.append(msg, remaining);
            }
            msg.position(msg.limit());
        }
    }

    void flush()
    {
        synchronized (_queue)
        {
            if (_connection.isOpen())
            {
                _queue.markFlushBoundary();
            }
        }
    }

    void schedule()
    {
        boolean scheduleWriteJob = false;
        synchronized (_queue)
        {
            if (!_writePending && !_writeJobOutstanding && (_queue.hasBytes() || _connection.isCloseRequested()))
            {
                _writeJobOutstanding = true;
                scheduleWriteJob = true;
            }
        }
        if (scheduleWriteJob)
        {
            scheduleWriteJob();
        }
    }

    void allowFinalWriteAttempt()
    {
        synchronized (_queue)
        {
            _finalWriteAttemptPending = !_writePending;
        }
    }

    boolean isFinalWriteAttemptPending()
    {
        synchronized (_queue)
        {
            return _finalWriteAttemptPending;
        }
    }

    @Override
    public void run()
    {
        try
        {
            runWritePump();
        }
        catch (final RuntimeException e)
        {
            _writeCallback.fail(e);
        }
        catch (final Error e)
        {
            _writeCallback.fail(e);
            throw e;
        }
        finally
        {
            writeJobCompleted();
        }
    }

    boolean commitWriteBatch()
    {
        synchronized (_queue)
        {
            if (_connection.requestForceCloseIfDeadlineExpired() || _discardPendingWrites ||
                    _connection.isForceClosing() || _connection.isClosed())
            {
                return false;
            }
            _writePending = true;
            return true;
        }
    }

    boolean completeWrite()
    {
        synchronized (_queue)
        {
            final boolean writePending = _writePending;
            _writePending = false;
            return writePending;
        }
    }

    boolean failWrite(final Throwable failure, final boolean submissionFailed)
    {
        synchronized (_queue)
        {
            if (submissionFailed)
            {
                _writeJobOutstanding = false;
            }
            _writePending = false;
            return _connection.requestForceCloseAfterWriteFailure(failure);
        }
    }

    void closed()
    {
        synchronized (_queue)
        {
            _writePending = false;
            discardPendingWrites(false);
        }
    }

    void discardPendingWrites(final boolean preserveFinalWriteAttempt)
    {
        synchronized (_queue)
        {
            _discardPendingWrites = true;
            if (!preserveFinalWriteAttempt)
            {
                _finalWriteAttemptPending = false;
            }
            final Deque<QpidByteBuffer> buffers = _queue.detachForCleanup();
            if (buffers != null)
            {
                _cleanupJob.discardBuffers(buffers);
            }
        }
    }

    private void scheduleWriteJob()
    {
        try
        {
            _threadPool.execute(this);
        }
        catch (final RuntimeException e)
        {
            _writeCallback.submissionFailed(e);
        }
        catch (final Error e)
        {
            _writeCallback.submissionFailed(e);
            throw e;
        }
    }

    private void writeJobCompleted()
    {
        synchronized (_queue)
        {
            _writeJobOutstanding = false;
        }
        schedule();
    }

    private void runWritePump()
    {
        boolean finalWritePass = false;
        try
        {
            while (true)
            {
                if (_connection.requestForceCloseIfDeadlineExpired())
                {
                    _scheduler.wakeup();
                    return;
                }

                final ByteBuffer data;
                final int closeStatusCode;
                WebSocketWriteQueue.WriteBatch batch = null;
                try
                {
                    synchronized (_queue)
                    {
                        if (_writePending || _connection.isForceClosing() || _connection.isClosed())
                        {
                            return;
                        }

                        if (_queue.hasBytes())
                        {
                            batch = _queue.getWriteBatch();
                            _queue.reserveBatch(batch);
                            closeStatusCode = -1;
                        }
                        else if (_connection.isCloseRequested())
                        {
                            closeStatusCode = _connection.getCloseStatusCode();
                        }
                        else
                        {
                            return;
                        }
                    }
                    data = batch == null ? null : batch.copy();
                }
                finally
                {
                    if (batch != null)
                    {
                        batch.release();
                    }
                }

                if (data == null)
                {
                    finalWritePass = isFinalWriteAttemptPending();
                    submitClose(closeStatusCode);
                    return;
                }
                if (!_connection.commitWriteBatch())
                {
                    wakeupIfForceClosing();
                    return;
                }

                finalWritePass = _connection.isCloseRequested() && isFinalWriteAttemptPending();
                if (!submitBinary(data, finalWritePass))
                {
                    return;
                }
            }
        }
        finally
        {
            if (finalWritePass)
            {
                finishFinalWriteAttempt();
            }
        }
    }

    private boolean submitBinary(final ByteBuffer data, final boolean finalWritePass)
    {
        final AtomicBoolean completedInline = finalWritePass ? new AtomicBoolean() : null;
        final Callback callback = finalWritePass ? finalWriteCallback(completedInline) : _writeCallback;
        final int size = data.remaining();
        _session.sendBinary(data, callback);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Written {} byte(s)", size);
        }
        return completedInline != null && completedInline.get();
    }

    private Callback finalWriteCallback(final AtomicBoolean completedInline)
    {
        final Thread submitter = Thread.currentThread();
        return Callback.from(() ->
        {
            if (Thread.currentThread() == submitter)
            {
                completedInline.set(true);
            }
            _writeCallback.succeed();
        }, _writeCallback::fail);
    }

    private void submitClose(final int closeStatusCode)
    {
        if (!_connection.commitWebSocketClose())
        {
            wakeupIfForceClosing();
            return;
        }

        try
        {
            _session.close(closeStatusCode, null, _closeCallback);
        }
        catch (final RuntimeException e)
        {
            _closeCallback.fail(e);
        }
        catch (final Error e)
        {
            _closeCallback.fail(e);
            throw e;
        }
    }

    private void finishFinalWriteAttempt()
    {
        final boolean wakeup;
        synchronized (_queue)
        {
            wakeup = _finalWriteAttemptPending;
            _finalWriteAttemptPending = false;
        }
        if (wakeup)
        {
            _scheduler.wakeup();
        }
    }

    private void wakeupIfForceClosing()
    {
        if (_connection.isForceClosing())
        {
            _scheduler.wakeup();
        }
    }
}
