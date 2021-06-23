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
import java.util.Map;

import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;

final class BlockingRule extends AbstractRule
{
    BlockingRule(ConnectionLimitRule rule)
    {
        this(rule.getPort(), rule.getIdentity());
    }

    BlockingRule(String port, String identity)
    {
        super(port, identity);
    }

    @Override
    public boolean isUserBlocked()
    {
        return true;
    }

    @Override
    public Integer getCountLimit()
    {
        return 0;
    }

    @Override
    public Integer getFrequencyLimit()
    {
        return 0;
    }

    @Override
    public Duration getFrequencyPeriod()
    {
        return Duration.ofMinutes(1L);
    }

    @Override
    public Map<Duration, Integer> getFrequencyLimits()
    {
        return Collections.emptyMap();
    }

    @Override
    public void updateWithDefaultFrequencyPeriod(Duration period)
    {
        // Do nothing
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }
}
