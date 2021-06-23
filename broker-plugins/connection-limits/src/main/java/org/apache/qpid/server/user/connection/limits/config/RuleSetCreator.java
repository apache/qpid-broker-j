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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.qpid.server.security.limit.ConnectionLimitProvider;

public final class RuleSetCreator extends ArrayList<Rule>
{
    private static final long serialVersionUID = -7;

    private Long _defaultFrequencyPeriod = null;

    private boolean _logAllMessages = false;

    public RuleSetCreator()
    {
        super(new ArrayList<>());
    }

    public void setDefaultFrequencyPeriod(long defaultFrequencyPeriod)
    {
        _defaultFrequencyPeriod = defaultFrequencyPeriod;
    }

    public Long getDefaultFrequencyPeriod()
    {
        return _defaultFrequencyPeriod;
    }

    public boolean isDefaultFrequencyPeriodSet()
    {
        return _defaultFrequencyPeriod != null;
    }

    public void setLogAllMessages(boolean logAllMessages)
    {
        this._logAllMessages = logAllMessages;
    }

    public boolean isLogAllMessages()
    {
        return _logAllMessages;
    }

    public RuleSet getLimiter(String name)
    {
        final long period = Optional.ofNullable(_defaultFrequencyPeriod)
                .orElse(ConnectionLimitProvider.DEFAULT_CONNECTION_FREQUENCY_PERIOD);
        if (period > 0)
        {
            final Duration defaultFrequencyPeriod = Duration.ofMillis(period);
            updateRulesWithDefaultFrequencyPeriod(defaultFrequencyPeriod);
            return RuleSet.newBuilder(name, defaultFrequencyPeriod)
                    .logAllMessages(_logAllMessages).addRules(this).build();
        }
        return RuleSet.newBuilder(name, null)
                .logAllMessages(_logAllMessages).addRules(this).build();
    }

    public RuleSetCreator updateRulesWithDefaultFrequencyPeriod()
    {
        if (_defaultFrequencyPeriod != null && _defaultFrequencyPeriod > 0)
        {
            updateRulesWithDefaultFrequencyPeriod(Duration.ofMillis(_defaultFrequencyPeriod));
        }
        return this;
    }

    @Override
    public boolean add(Rule rule)
    {
        return addImpl(rule);
    }

    @Override
    public boolean addAll(Collection<? extends Rule> c)
    {
        boolean changed = false;
        if (c != null)
        {
            for (final Rule rule : c)
            {
                changed = add(rule) || changed;
            }
        }
        return changed;
    }

    private boolean addImpl(Rule rule)
    {
        if (rule != null && !rule.isEmpty())
        {
            return super.add(rule);
        }
        return false;
    }

    private void updateRulesWithDefaultFrequencyPeriod(final Duration defaultFrequencyPeriod)
    {
        forEach(r -> r.updateWithDefaultFrequencyPeriod(defaultFrequencyPeriod));
    }
}
