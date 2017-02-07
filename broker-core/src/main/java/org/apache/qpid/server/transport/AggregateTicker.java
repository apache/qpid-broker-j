/*
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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.server.transport.network.Ticker;

public class AggregateTicker implements Ticker, SchedulingDelayNotificationListener
{

    private final CopyOnWriteArrayList<Ticker> _tickers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean _modified = new AtomicBoolean();

    @Override
    public int getTimeToNextTick(final long currentTime)
    {
        int nextTick = Integer.MAX_VALUE;
        // QPID-7447: prevent unnecessary allocation of empty iterator
        if (!_tickers.isEmpty())
        {
            for (Ticker ticker : _tickers)
            {
                nextTick = Math.min(ticker.getTimeToNextTick(currentTime), nextTick);
            }
        }
        return nextTick;
    }

    @Override
    public int tick(final long currentTime)
    {
        int nextTick = Integer.MAX_VALUE;
        // QPID-7447: prevent unnecessary allocation of empty iterator
        if (!_tickers.isEmpty())
        {
            for(Ticker ticker : _tickers)
            {
                nextTick = Math.min(ticker.tick(currentTime), nextTick);
            }
        }
        return nextTick;
    }

    public void addTicker(Ticker ticker)
    {
        _tickers.add(ticker);
        _modified.set(true);
    }

    public void removeTicker(Ticker ticker)
    {
        _tickers.remove(ticker);
        _modified.set(true);
    }

    public boolean getModified()
    {
        return _modified.get();
    }

    public void resetModified()
    {
        _modified.set(false);
    }

    @Override
    public void notifySchedulingDelay(final long schedulingDelay)
    {
        // QPID-7447: prevent unnecessary allocation of empty iterator
        if (!_tickers.isEmpty())
        {
            for (Ticker ticker : _tickers)
            {
                if (ticker instanceof SchedulingDelayNotificationListener)
                {
                    ((SchedulingDelayNotificationListener) ticker).notifySchedulingDelay(schedulingDelay);
                }
            }
        }
    }
}
