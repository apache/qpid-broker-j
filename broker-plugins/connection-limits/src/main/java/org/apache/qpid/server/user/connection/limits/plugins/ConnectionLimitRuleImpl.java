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
package org.apache.qpid.server.user.connection.limits.plugins;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.apache.qpid.server.user.connection.limits.config.Rule;

class ConnectionLimitRuleImpl implements ConnectionLimitRule
{
    private final Rule _rule;

    ConnectionLimitRuleImpl(Rule rule)
    {
        _rule = Objects.requireNonNull(rule);
    }

    @Override
    public String getPort()
    {
        return _rule.getPort();
    }

    @Override
    public String getIdentity()
    {
        return _rule.getIdentity();
    }

    @Override
    public Boolean getBlocked()
    {
        return _rule.isUserBlocked();
    }

    @Override
    public Integer getCountLimit()
    {
        if (!_rule.isUserBlocked())
        {
            return _rule.getCountLimit();
        }
        return null;
    }

    @Override
    public Integer getFrequencyLimit()
    {
        if (!_rule.isUserBlocked())
        {
            return _rule.getFrequencyLimit();
        }
        return null;
    }

    @Override
    public Long getFrequencyPeriod()
    {
        if (!_rule.isUserBlocked() && _rule.getFrequencyLimit() != null)
        {
            return Optional.ofNullable(_rule.getFrequencyPeriod()).map(Duration::toMillis).orElse(null);
        }
        return null;
    }
}
