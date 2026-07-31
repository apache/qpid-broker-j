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

package org.apache.qpid.server.protocol.v1_0;

import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

import org.apache.qpid.server.transport.network.Ticker;

final class EchoFlowCoalescer
{
    interface Scheduler
    {
        Cancellable schedule(final int delayMillis, final Runnable task);
    }

    interface Cancellable
    {
        void cancel();
    }

    private final long _coalesceIntervalNs;
    private final LongSupplier _clock;
    private final Scheduler _scheduler;
    private final Executor _ioThreadExecutor;
    private final Runnable _sendAction;

    private long _nextAllowedSendTimeNs;
    private boolean _pending;
    private Cancellable _scheduledWakeUp;

    EchoFlowCoalescer(final long coalesceIntervalMs,
                      final LongSupplier clock,
                      final Scheduler scheduler,
                      final Executor ioThreadExecutor,
                      final Runnable sendAction)
    {
        _coalesceIntervalNs = Math.max(0L, coalesceIntervalMs) * 1_000_000L;
        _clock = clock;
        _scheduler = scheduler;
        _ioThreadExecutor = ioThreadExecutor;
        _sendAction = sendAction;
        _nextAllowedSendTimeNs = clock == null ? 0L : clock.getAsLong();
    }

    static Scheduler newTickerScheduler(final Session_1_0 session)
    {
        return (delayMillis, task) ->
        {
            final OneShotTicker ticker = new OneShotTicker(session, delayMillis, task);
            session.addTicker(ticker);
            return ticker;
        };
    }

    void requestEcho()
    {
        final boolean sendNow;
        synchronized (this)
        {
            if (_coalesceIntervalNs == 0L)
            {
                sendNow = true;
            }
            else
            {
                final long now = _clock.getAsLong();
                if (!_pending && isDue(now, _nextAllowedSendTimeNs))
                {
                    sendNow = true;
                }
                else
                {
                    _pending = true;
                    ensureWakeUpScheduled(now);
                    sendNow = false;
                }
            }
        }

        if (sendNow)
        {
            _sendAction.run();
        }
    }

    synchronized void markFlowSent()
    {
        _pending = false;
        cancelWakeUp();

        if (_coalesceIntervalNs == 0L)
        {
            _nextAllowedSendTimeNs = 0L;
        }
        else
        {
            _nextAllowedSendTimeNs = _clock.getAsLong() + _coalesceIntervalNs;
        }
    }

    synchronized void cancel()
    {
        _pending = false;
        cancelWakeUp();
    }

    private void onScheduledWakeUp()
    {
        _ioThreadExecutor.execute(this::processScheduledWakeUpOnIoThread);
    }

    private void processScheduledWakeUpOnIoThread()
    {
        final boolean sendNow;
        synchronized (this)
        {
            _scheduledWakeUp = null;
            if (!_pending)
            {
                return;
            }

            if (_coalesceIntervalNs == 0L)
            {
                _pending = false;
                sendNow = true;
            }
            else
            {
                final long now = _clock.getAsLong();
                if (isDue(now, _nextAllowedSendTimeNs))
                {
                    _pending = false;
                    sendNow = true;
                }
                else
                {
                    ensureWakeUpScheduled(now);
                    sendNow = false;
                }
            }
        }

        if (sendNow)
        {
            _sendAction.run();
        }
    }

    private void ensureWakeUpScheduled(final long now)
    {
        if (_scheduledWakeUp == null)
        {
            final long remainingNs = Math.max(1L, _nextAllowedSendTimeNs - now);
            _scheduledWakeUp = _scheduler.schedule(toDelayMillis(remainingNs), this::onScheduledWakeUp);
        }
    }

    private void cancelWakeUp()
    {
        if (_scheduledWakeUp != null)
        {
            _scheduledWakeUp.cancel();
            _scheduledWakeUp = null;
        }
    }

    private boolean isDue(final long now, final long deadline)
    {
        return now - deadline >= 0L;
    }

    private int toDelayMillis(final long remainingNs)
    {
        final long delayMillis = (remainingNs + 999_999L) / 1_000_000L;
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, delayMillis));
    }

    private static final class OneShotTicker implements Ticker, Cancellable
    {
        private final Session_1_0 _session;
        private final long _deadlineTime;
        private final Runnable _task;

        private volatile boolean _cancelled;

        private OneShotTicker(final Session_1_0 session, final int delayMillis, final Runnable task)
        {
            _session = session;
            _deadlineTime = System.currentTimeMillis() + Math.max(0, delayMillis);
            _task = task;
        }

        @Override
        public int getTimeToNextTick(final long currentTime)
        {
            if (_cancelled)
            {
                return Integer.MAX_VALUE;
            }

            final long remaining = _deadlineTime - currentTime;
            return remaining <= 0L ? 0 : (int) Math.min(Integer.MAX_VALUE, remaining);
        }

        @Override
        public int tick(final long currentTime)
        {
            final int nextTick = getTimeToNextTick(currentTime);
            if (nextTick <= 0)
            {
                cancel();
                _task.run();
            }
            return nextTick;
        }

        @Override
        public void cancel()
        {
            if (!_cancelled)
            {
                _cancelled = true;
                _session.removeTicker(this);
            }
        }
    }
}
