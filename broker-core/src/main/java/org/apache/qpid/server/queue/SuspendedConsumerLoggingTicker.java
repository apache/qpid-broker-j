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
package org.apache.qpid.server.queue;

import org.apache.qpid.server.transport.network.Ticker;

abstract public class SuspendedConsumerLoggingTicker implements Ticker
{
    private volatile long _nextTick;
    private volatile long _startTime;
    private final long _repeatPeriod;

    public SuspendedConsumerLoggingTicker(final long repeatPeriod)
    {
        _repeatPeriod = repeatPeriod;
    }

    public void setStartTime(final long currentTime)
    {
        _startTime = currentTime;
        _nextTick = currentTime + _repeatPeriod;
    }

    @Override
    public int getTimeToNextTick(final long currentTime)
    {
        return (int) (_nextTick - currentTime);
    }

    @Override
    public int tick(final long currentTime)
    {
        int nextTick = getTimeToNextTick(currentTime);
        if(nextTick <= 0)
        {
            log(currentTime - _startTime);
            _nextTick = _nextTick + _repeatPeriod;
            nextTick = getTimeToNextTick(currentTime);
        }
        return nextTick;
    }

    abstract protected void log(long period);
}
