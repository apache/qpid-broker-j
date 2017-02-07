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

package org.apache.qpid.server.transport;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Supplier;

import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.transport.network.Ticker;

public class TransactionTimeoutTicker implements Ticker, SchedulingDelayNotificationListener
{
    private final long _timeoutValue;
    private final Action<Long> _notification;
    private final Supplier<Long> _timeSupplier;
    private final long _notificationRepeatPeriod;

    private AtomicLong _accumulatedSchedulingDelay = new AtomicLong();
    /** The time the ticker will next procedure the notification */
    private volatile long _nextNotificationTime = 0;
    /** Last transaction time stamp seen by this ticker.  */
    private volatile long _lastTransactionTimeStamp = 0;

    public TransactionTimeoutTicker(long timeoutValue,
                                    long notificationRepeatPeriod,
                                    Supplier<Long> timeStampSupplier,
                                    Action<Long> notification)
    {
        _timeoutValue = timeoutValue;
        _notification = notification;
        _lastTransactionTimeStamp = timeStampSupplier.get();
        _timeSupplier = timeStampSupplier;
        _notificationRepeatPeriod = notificationRepeatPeriod;
    }

    @Override
    public int getTimeToNextTick(final long currentTime)
    {
        final long transactionTimeStamp = _timeSupplier.get();
        int tick = calculateTimeToNextTick(currentTime, transactionTimeStamp);
        if (tick <= 0 && _nextNotificationTime > currentTime)
        {
            tick = (int) (_nextNotificationTime - currentTime);
        }
        return tick;
    }

    @Override
    public int tick(final long currentTime)
    {
        final long transactionTimeStamp = _timeSupplier.get();
        int tick = calculateTimeToNextTick(currentTime, transactionTimeStamp);
        if (tick <= 0)
        {
            if (currentTime >= _nextNotificationTime)
            {
                final long idleTime = currentTime - transactionTimeStamp;
                _nextNotificationTime = currentTime + _notificationRepeatPeriod;
                _notification.performAction(idleTime);
            }
            else
            {
                tick = (int) (_nextNotificationTime - currentTime);
            }
        }
        return tick;
    }

    private int calculateTimeToNextTick(final long currentTime, final long transactionTimeStamp)
    {
        if (transactionTimeStamp != _lastTransactionTimeStamp)
        {
            // Transactions's time stamp has changed, reset the next notification time
            _lastTransactionTimeStamp = transactionTimeStamp;
            _nextNotificationTime = 0;
            _accumulatedSchedulingDelay.set(0);
        }
        if (transactionTimeStamp > 0)
        {
            return (int) ((transactionTimeStamp + _timeoutValue + _accumulatedSchedulingDelay.get()) - currentTime);
        }
        else
        {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public void notifySchedulingDelay(final long schedulingDelay)
    {
        if (schedulingDelay > 0)
        {
            long accumulatedSchedulingDelay;
            do
            {
                accumulatedSchedulingDelay = _accumulatedSchedulingDelay.get();
            }
            while (!_accumulatedSchedulingDelay.compareAndSet(accumulatedSchedulingDelay, accumulatedSchedulingDelay + schedulingDelay));
        }
    }
}
