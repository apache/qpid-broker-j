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

import org.apache.qpid.transport.network.Ticker;
import org.apache.qpid.transport.network.TransportActivity;

public class ServerIdleWriteTimeoutTicker implements Ticker
{
    private final TransportActivity _transport;
    private final int _defaultTimeout;
    private final ServerNetworkConnection _connection;

    public ServerIdleWriteTimeoutTicker(ServerNetworkConnection connection, TransportActivity transport,
                                        int defaultTimeout)
    {
        _connection = connection;
        _transport = transport;
        _defaultTimeout = defaultTimeout;
    }

    @Override
    public int getTimeToNextTick(long currentTime)
    {
        long maxWriteIdle = _connection.getMaxWriteIdleMillis();
        if (maxWriteIdle > 0)
        {
            long writeTime = _transport.getLastWriteTime() + maxWriteIdle;
            return (int) (writeTime - currentTime);
        }

        return _defaultTimeout;
    }

    @Override
    public int tick(long currentTime)
    {
        int timeToNextTick = getTimeToNextTick(currentTime);
        if (_connection.getMaxWriteIdleMillis() > 0 && timeToNextTick <= 0)
        {
            _transport.writerIdle();
        }

        return timeToNextTick;
    }
}
