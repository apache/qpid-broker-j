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
package org.apache.qpid.server.protocol;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.network.Ticker;

public class ConnectionClosingTicker implements Ticker
{
    private final long _startTimeNanos;
    private final long _timeoutNanos;
    private final LongSupplier _nanoTime;
    private final ServerNetworkConnection _network;

    public ConnectionClosingTicker(final long timeoutMillis, final ServerNetworkConnection network)
    {
        this(timeoutMillis, network, System::nanoTime);
    }

    ConnectionClosingTicker(final long timeoutMillis,
                            final ServerNetworkConnection network,
                            final LongSupplier nanoTime)
    {
        _nanoTime = nanoTime;
        _startTimeNanos = nanoTime.getAsLong();
        _timeoutNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, timeoutMillis));
        _network = network;
    }

    @Override
    public int getTimeToNextTick(final long currentTime)
    {
        final long elapsed = _nanoTime.getAsLong() - _startTimeNanos;
        if (elapsed >= _timeoutNanos)
        {
            return 0;
        }
        final long remaining = _timeoutNanos - elapsed;
        final long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
        return (int) Math.min(Integer.MAX_VALUE, millis + (remaining % 1_000_000L == 0 ? 0 : 1));
    }

    @Override
    public int tick(final long currentTime)
    {
        final int nextTick = getTimeToNextTick(currentTime);
        if(nextTick <= 0)
        {
            _network.abort();
        }
        return nextTick;
    }
}
