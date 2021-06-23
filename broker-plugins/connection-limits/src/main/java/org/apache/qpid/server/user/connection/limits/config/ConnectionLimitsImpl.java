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
package org.apache.qpid.server.user.connection.limits.config;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class ConnectionLimitsImpl implements ConnectionLimits
{
    private final Integer _connectionCount;

    private final Map<Duration, Integer> _connectionFrequency;

    ConnectionLimitsImpl(final ConnectionLimits first, final ConnectionLimits second)
    {
        super();
        _connectionCount = min(first.getCountLimit(), second.getCountLimit());
        _connectionFrequency = new HashMap<>(first.getFrequencyLimits());
        second.getFrequencyLimits().forEach(
                (duration, limit) -> _connectionFrequency.merge(duration, limit, ConnectionLimitsImpl::min));
        if (_connectionCount == null && _connectionFrequency.isEmpty())
        {
            throw new IllegalArgumentException("Unexpected empty limits");
        }
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public Integer getCountLimit()
    {
        return _connectionCount;
    }

    @Override
    public Map<Duration, Integer> getFrequencyLimits()
    {
        return Collections.unmodifiableMap(_connectionFrequency);
    }

    @Override
    public boolean isUserBlocked()
    {
        return false;
    }

    static Integer min(final Integer first, final Integer second)
    {
        if (first == null)
        {
            return second;
        }
        if (second == null)
        {
            return first;
        }
        return Math.min(first, second);
    }
}
