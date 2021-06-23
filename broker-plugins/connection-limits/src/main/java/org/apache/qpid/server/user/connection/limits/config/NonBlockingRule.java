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
import java.util.Optional;

import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;

final class NonBlockingRule extends AbstractRule
{
    private final Integer _connectionCount;

    private final Integer _connectionFrequency;

    private Duration _frequencyPeriod;

    NonBlockingRule(ConnectionLimitRule rule)
    {
        this(rule.getPort(), rule.getIdentity(), rule.getCountLimit(), rule.getFrequencyLimit(),
                Optional.ofNullable(rule.getFrequencyPeriod()).map(Duration::ofMillis).orElse(null));
    }

    NonBlockingRule(String port, String identity, Integer connectionCount,
                    Integer connectionFrequency, Duration frequencyPeriod)
    {
        super(port, identity);
        if (connectionCount == null && connectionFrequency == null)
        {
            throw new IllegalArgumentException("Empty connection limit rule");
        }
        this._connectionCount = connectionCount;
        this._connectionFrequency = connectionFrequency;
        this._frequencyPeriod = frequencyPeriod;
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public boolean isUserBlocked()
    {
        return false;
    }

    @Override
    public Integer getCountLimit()
    {
        return _connectionCount;
    }

    @Override
    public Integer getFrequencyLimit()
    {
        return _connectionFrequency;
    }

    @Override
    public Duration getFrequencyPeriod()
    {
        return _frequencyPeriod;
    }

    @Override
    public void updateWithDefaultFrequencyPeriod(Duration period)
    {
        if (_frequencyPeriod == null)
        {
            _frequencyPeriod = period;
        }
    }
}
