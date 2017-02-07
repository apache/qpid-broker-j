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

import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.server.transport.network.TransportActivity;

public class ServerIdleReadTimeoutTicker implements Ticker
{
    private final TransportActivity _transport;
    private final int _readDelay;
    private final ServerNetworkConnection _connection;

    public ServerIdleReadTimeoutTicker(ServerNetworkConnection connection, TransportActivity transport, int readDelay)
    {
        if (readDelay <= 0)
        {
            throw new IllegalArgumentException("Read delay should be positive");
        }

        _connection = connection;
        _transport = transport;
        _readDelay = readDelay;
    }

    @Override
    public int getTimeToNextTick(long currentTime)
    {
        long nextTime = _transport.getLastReadTime() + (long) _readDelay;
        return (int) (nextTime - (_connection.getScheduledTime() > 0 ?  _connection.getScheduledTime() : currentTime) );
    }

    @Override
    public int tick(long currentTime)
    {
        int timeToNextTick = getTimeToNextTick(currentTime);;
        if (timeToNextTick <= 0)
        {
            _transport.readerIdle();
        }

        return timeToNextTick;
    }
}
