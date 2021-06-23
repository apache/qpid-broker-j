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

public interface Rule extends ConnectionLimits
{
    String getPort();

    String getIdentity();

    @Override
    Integer getCountLimit();

    Integer getFrequencyLimit();

    Duration getFrequencyPeriod();

    @Override
    default Map<Duration, Integer> getFrequencyLimits()
    {
        if (getFrequencyLimit() != null)
        {
            return Collections.singletonMap(getFrequencyPeriod(), getFrequencyLimit());
        }
        else
        {
            return Collections.emptyMap();
        }
    }

    void updateWithDefaultFrequencyPeriod(Duration period);

    static Rule newInstance(ConnectionLimitRule rule)
    {
        if (Boolean.TRUE.equals(rule.getBlocked()))
        {
            return new BlockingRule(rule);
        }
        else
        {
            return new NonBlockingRule(rule);
        }
    }

    static Rule newInstance(String identity, RulePredicates predicates)
    {
        if (predicates.isUserBlocked())
        {
            return newBlockingRule(predicates.getPort(), identity);
        }
        else
        {
            return newNonBlockingRule(
                    predicates.getPort(),
                    identity,
                    predicates.getConnectionCountLimit(),
                    predicates.getConnectionFrequencyLimit(),
                    predicates.getConnectionFrequencyPeriod());
        }
    }

    static NonBlockingRule newNonBlockingRule(
            String port, String identity, Integer count, Integer frequency, Duration frequencyPeriod)
    {
        return new NonBlockingRule(port, identity, count, frequency, frequencyPeriod);
    }

    static BlockingRule newBlockingRule(String port, String identity)
    {
        return new BlockingRule(port, identity);
    }
}
