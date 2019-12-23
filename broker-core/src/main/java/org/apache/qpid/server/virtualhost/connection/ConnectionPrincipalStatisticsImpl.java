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
 */

package org.apache.qpid.server.virtualhost.connection;

import java.util.Collections;
import java.util.List;

import org.apache.qpid.server.virtualhost.ConnectionPrincipalStatistics;

class ConnectionPrincipalStatisticsImpl implements ConnectionPrincipalStatistics
{
    private final int _connectionCount;
    private final List<Long> _latestConnectionCreatedTimes;

    ConnectionPrincipalStatisticsImpl(final int connectionCount, final List<Long> latestConnectionCreatedTimes)
    {
        _connectionCount = connectionCount;
        _latestConnectionCreatedTimes = Collections.unmodifiableList(latestConnectionCreatedTimes);
    }

    @Override
    public int getConnectionCount()
    {
        return _connectionCount;
    }

    @Override
    public int getConnectionFrequency()
    {
        return _latestConnectionCreatedTimes.size();
    }

    List<Long> getLatestConnectionCreatedTimes()
    {
        return _latestConnectionCreatedTimes;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final ConnectionPrincipalStatisticsImpl that = (ConnectionPrincipalStatisticsImpl) o;

        if (_connectionCount != that._connectionCount)
        {
            return false;
        }
        return _latestConnectionCreatedTimes.equals(that._latestConnectionCreatedTimes);
    }

    @Override
    public int hashCode()
    {
        int result = _connectionCount;
        result = 31 * result + _latestConnectionCreatedTimes.hashCode();
        return result;
    }
}
