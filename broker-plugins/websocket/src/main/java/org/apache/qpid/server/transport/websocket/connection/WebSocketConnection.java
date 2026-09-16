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

import java.net.SocketAddress;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.SchedulingDelayNotificationListener;
import org.apache.qpid.server.transport.ServerNetworkConnection;

class WebSocketConnection implements ServerNetworkConnection, ByteBufferSender
{
    private enum WebSocketConnectionState
    {
        OPEN,
        CLOSE_REQUESTED,
        CLOSE_SENT,
        FORCE_CLOSING,
        CLOSED
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketConnection.class);
    private static final long FORCE_CLOSE_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private final Session _connection;
    private final SocketAddress _localAddress;
    private final SocketAddress _remoteAddress;
    private final ReentrantLock _protocolLock = new ReentrantLock();
    private final WebSocketWriteQueue _writeQueue;
    private final MultiVersionProtocolEngine _protocolEngine;
    private final WebSocketConnectionScheduler _scheduler;
    private final ProtocolWorkJob _workJob;
    private final TickJob _tickJob;
    private final WriteJob _writeJob;
    private final CleanupJob _cleanupJob;

    private Certificate _certificate;
    private long _maxWriteIdleMillis;
    private long _maxReadIdleMillis;
    private long _closeStartTimeNanos;
    private long _lastForceCloseAttemptTimeNanos;
    private int _closeStatusCode = StatusCode.NORMAL;
    private boolean _forceCloseAttempted;
    private boolean _forceCloseFailureReported;
    private volatile WebSocketConnectionState _webSocketConnectionState = WebSocketConnectionState.OPEN;

    WebSocketConnection(final Session connection,
                        final SocketAddress localAddress,
                        final SocketAddress remoteAddress,
                        final MultiVersionProtocolEngine protocolEngine,
                        final ThreadPool threadPool,
                        final WebSocketConnectionScheduler scheduler,
                        final WebSocketSettings settings)
    {
        Objects.requireNonNull(settings, "WebSocket settings must not be null");
        _connection = connection;
        _localAddress = localAddress;
        _remoteAddress = remoteAddress;
        _protocolEngine = protocolEngine;
        _scheduler = scheduler;
        _writeQueue = new WebSocketWriteQueue(settings);
        _workJob = new ProtocolWorkJob(this, protocolEngine, threadPool, scheduler);
        _cleanupJob = new CleanupJob(this, threadPool, scheduler, _writeQueue);
        _writeJob = new WriteJob(this, connection, threadPool, scheduler, _writeQueue, _cleanupJob);
        _tickJob = new TickJob(this, protocolEngine, threadPool, scheduler);
    }

    long getTimeToNextTick(final long currentTime)
    {
        return _protocolEngine.getAggregateTicker().getTimeToNextTick(currentTime);
    }

    long getTickRetryDelay(final long currentTime)
    {
        return _tickJob.getRetryDelay(currentTime);
    }

    void tickNoLongerOverdue()
    {
        _tickJob.noLongerOverdue();
    }

    @Override
    public ByteBufferSender getSender()
    {
        return this;
    }

    @Override
    public void start()
    {
    }

    @Override
    public boolean isDirectBufferPreferred()
    {
        return false;
    }

    @Override
    public void send(final QpidByteBuffer msg)
    {
        _writeJob.send(msg);
    }

    @Override
    public void flush()
    {
        _writeJob.flush();
    }

    @Override
    public void close()
    {
        close(StatusCode.NORMAL, true);
    }

    void close(final int statusCode, final boolean drainPendingWrites)
    {
        boolean wakeup = false;
        synchronized (_writeQueue)
        {
            if (isOpen())
            {
                _closeStatusCode = statusCode;
                _closeStartTimeNanos = _scheduler.getNanoTime();
                _webSocketConnectionState = WebSocketConnectionState.CLOSE_REQUESTED;
                if (!drainPendingWrites)
                {
                    _writeJob.discardPendingWrites();
                }
                wakeup = true;
            }
        }
        if (wakeup)
        {
            _scheduler.wakeup();
        }
    }

    @Override
    public SocketAddress getRemoteAddress()
    {
        return _remoteAddress;
    }

    @Override
    public SocketAddress getLocalAddress()
    {
        return _localAddress;
    }

    @Override
    public void setMaxWriteIdleMillis(final long millis)
    {
        _maxWriteIdleMillis = millis;
    }

    @Override
    public void setMaxReadIdleMillis(final long millis)
    {
        _maxReadIdleMillis = millis;
    }

    @Override
    public Principal getPeerPrincipal()
    {
        return _certificate instanceof X509Certificate x509Certificate
                ? x509Certificate.getSubjectX500Principal()
                : null;
    }

    @Override
    public Certificate getPeerCertificate()
    {
        return _certificate;
    }

    @Override
    public long getMaxReadIdleMillis()
    {
        return _maxReadIdleMillis;
    }

    @Override
    public long getMaxWriteIdleMillis()
    {
        return _maxWriteIdleMillis;
    }

    @Override
    public void addSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
    {
    }

    @Override
    public void removeSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
    {
    }

    @Override
    public String getTransportInfo()
    {
        return _connection.getProtocolVersion();
    }

    @Override
    public long getScheduledTime()
    {
        return 0;
    }

    @Override
    public String getSelectedHost()
    {
        return null;
    }

    void setPeerCertificate(final Certificate certificate)
    {
        _certificate = certificate;
    }

    public void doWrite()
    {
        _writeJob.schedule();
    }

    boolean commitWriteBatch()
    {
        return _writeJob.commitWriteBatch();
    }

    boolean commitWebSocketClose()
    {
        synchronized (_writeQueue)
        {
            if (requestForceCloseIfDeadlineExpired() ||
                    _webSocketConnectionState != WebSocketConnectionState.CLOSE_REQUESTED)
            {
                return false;
            }
            _webSocketConnectionState = WebSocketConnectionState.CLOSE_SENT;
            return true;
        }
    }

    boolean requestForceCloseIfDeadlineExpired()
    {
        synchronized (_writeQueue)
        {
            if ((_webSocketConnectionState == WebSocketConnectionState.CLOSE_REQUESTED ||
                    _webSocketConnectionState == WebSocketConnectionState.CLOSE_SENT) &&
                    getRemainingCloseTime(_scheduler.getNanoTime()) == 0L)
            {
                return requestForceClose();
            }
            return false;
        }
    }

    boolean requestForceClose()
    {
        synchronized (_writeQueue)
        {
            if (_webSocketConnectionState == WebSocketConnectionState.CLOSED ||
                    _webSocketConnectionState == WebSocketConnectionState.FORCE_CLOSING)
            {
                return false;
            }

            enterForceClosing();
            return true;
        }
    }

    void scheduleWork()
    {
        _workJob.schedule();
    }

    public void doWork()
    {
        _workJob.doWork();
    }

    void processPendingWork()
    {
        final Iterator<Runnable> iterator = _protocolEngine.processPendingIterator();
        while (iterator.hasNext())
        {
            iterator.next().run();
        }
    }

    void lockProtocol()
    {
        _protocolLock.lock();
    }

    boolean tryLockProtocol()
    {
        return !_protocolLock.isHeldByCurrentThread() && _protocolLock.tryLock();
    }

    void unlockProtocol()
    {
        _protocolLock.unlock();
        if (!_protocolLock.isHeldByCurrentThread())
        {
            _cleanupJob.protocolUnlocked();
        }
    }

    boolean isOpen()
    {
        return _webSocketConnectionState == WebSocketConnectionState.OPEN;
    }

    boolean isClosing()
    {
        final WebSocketConnectionState state = _webSocketConnectionState;
        return state != WebSocketConnectionState.OPEN && state != WebSocketConnectionState.CLOSED;
    }

    boolean isCloseRequested()
    {
        return _webSocketConnectionState == WebSocketConnectionState.CLOSE_REQUESTED;
    }

    int getCloseStatusCode()
    {
        return _closeStatusCode;
    }

    boolean isClosed()
    {
        return _webSocketConnectionState == WebSocketConnectionState.CLOSED;
    }

    boolean isForceClosing()
    {
        return _webSocketConnectionState == WebSocketConnectionState.FORCE_CLOSING;
    }

    boolean webSocketClosed(final Runnable closeTask)
    {
        Objects.requireNonNull(closeTask, "Close task must not be null");
        synchronized (_writeQueue)
        {
            if (isClosed())
            {
                return false;
            }

            _webSocketConnectionState = WebSocketConnectionState.CLOSED;
            _writeJob.closed();
            _cleanupJob.setCloseTask(closeTask);
        }
        if (_scheduler.isShutdown())
        {
            _cleanupJob.run();
        }
        else
        {
            _scheduler.wakeup();
        }
        return true;
    }

    long scheduleCleanup(final long currentNanoTime)
    {
        return _cleanupJob.schedule(currentNanoTime);
    }

    void cleanupAfterShutdown()
    {
        _cleanupJob.run();
    }

    long processClose(final long currentNanoTime)
    {
        synchronized (_writeQueue)
        {
            final long delay = getForceCloseDelay(currentNanoTime);
            if (delay != 0L)
            {
                return delay;
            }
            _forceCloseAttempted = true;
            _lastForceCloseAttemptTimeNanos = currentNanoTime;
        }

        try
        {
            _connection.disconnect();
        }
        catch (final RuntimeException e)
        {
            reportForceCloseFailure(e);
        }

        synchronized (_writeQueue)
        {
            return isClosed() ? Long.MAX_VALUE : FORCE_CLOSE_RETRY_DELAY_NANOS;
        }
    }

    private long getForceCloseDelay(final long currentNanoTime)
    {
        if (isOpen() || isClosed())
        {
            return Long.MAX_VALUE;
        }
        if (_webSocketConnectionState == WebSocketConnectionState.CLOSE_REQUESTED ||
                _webSocketConnectionState == WebSocketConnectionState.CLOSE_SENT)
        {
            final long remaining = getRemainingCloseTime(currentNanoTime);
            if (remaining > 0L)
            {
                return remaining;
            }
            enterForceClosing();
        }
        return _forceCloseAttempted ? WebSocketConnectionScheduler.getRemainingTimeNanos(
                currentNanoTime, _lastForceCloseAttemptTimeNanos, FORCE_CLOSE_RETRY_DELAY_NANOS) : 0L;
    }

    private long getRemainingCloseTime(final long currentNanoTime)
    {
        return WebSocketConnectionScheduler
                .getRemainingTimeNanos(currentNanoTime, _closeStartTimeNanos, _scheduler.getCloseTimeoutNanos());
    }

    private void enterForceClosing()
    {
        _webSocketConnectionState = WebSocketConnectionState.FORCE_CLOSING;
        _forceCloseAttempted = false;
        _writeJob.discardPendingWrites();
    }

    private void reportForceCloseFailure(final RuntimeException failure)
    {
        boolean report = false;
        synchronized (_writeQueue)
        {
            if (!isClosed() && !_forceCloseFailureReported)
            {
                _forceCloseFailureReported = true;
                report = true;
            }
        }
        if (report)
        {
            LOGGER.warn("Failed to force closure of WebSocket connection {}; " +
                    "further failures for this connection will be suppressed", _remoteAddress, failure);
        }
    }

    boolean tryScheduleTick(final long currentTime)
    {
        return _tickJob.trySchedule(currentTime);
    }
}
