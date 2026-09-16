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

import org.eclipse.jetty.util.thread.ThreadPool;
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

    boolean failWrite(final boolean submissionFailed)
    {
        synchronized (_queue)
        {
            if (submissionFailed)
            {
                _writeJobOutstanding = false;
            }
            _writePending = false;
            return _connection.requestForceClose();
        }
    }

    void closed()
    {
        synchronized (_queue)
        {
            _writePending = false;
            discardPendingWrites();
        }
    }

    void discardPendingWrites()
    {
        synchronized (_queue)
        {
            _discardPendingWrites = true;
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
        if (_connection.requestForceCloseIfDeadlineExpired())
        {
            _scheduler.wakeup();
            return;
        }

        final ByteBuffer data;
        final int closeStatusCode;
        synchronized (_queue)
        {
            if (_writePending || _connection.isForceClosing() || _connection.isClosed())
            {
                return;
            }

            if (_queue.hasBytes())
            {
                data = _queue.createBatch();
                closeStatusCode = -1;
            }
            else if (_connection.isCloseRequested())
            {
                data = null;
                closeStatusCode = _connection.getCloseStatusCode();
            }
            else
            {
                return;
            }
        }

        if (data != null)
        {
            submitBinary(data);
        }
        else
        {
            submitClose(closeStatusCode);
        }
    }

    private void submitBinary(final ByteBuffer data)
    {
        if (!_connection.commitWriteBatch())
        {
            wakeupIfForceClosing();
            return;
        }

        try
        {
            final int size = data.remaining();
            _session.sendBinary(data, _writeCallback);
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Written {} byte(s)", size);
            }
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

    private void wakeupIfForceClosing()
    {
        if (_connection.isForceClosing())
        {
            _scheduler.wakeup();
        }
    }
}
