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
package org.apache.qpid.server.security.access.config;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RuleCollector
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleCollector.class);

    private static final Integer INCREMENT = 10;

    private final NavigableMap<Integer, Rule> _rules = new TreeMap<>();
    private final Map<RuleKey, Integer> _ruleSet = new HashMap<>();

    private Result _defaultResult = Result.DENIED;

    RuleCollector()
    {
        super();
    }

    public boolean isValidNumber(Integer number)
    {
        return !_rules.containsKey(number);
    }

    public void addRule(Integer position, Rule rule)
    {
        // set rule number if needed
        if (position == null)
        {
            if (_rules.isEmpty())
            {
                position = 0;
            }
            else
            {
                position = _rules.lastKey() + INCREMENT;
            }
        }

        final RuleKey key = new RuleKey(rule);
        if (_ruleSet.containsKey(key))
        {
            LOGGER.warn("Duplicate rule for the '{}'", key);
            final Integer previousPosition = _ruleSet.get(key);
            if (previousPosition > position)
            {
                _ruleSet.remove(key);
                _rules.remove(previousPosition);
            }
            else
            {
                return;
            }
        }

        // save rule
        _rules.put(position, rule);
        _ruleSet.put(key, position);
    }

    void setDefaultResult(final Result defaultResult)
    {
        _defaultResult = defaultResult;
    }

    RuleSet createRuleSet(EventLoggerProvider eventLoggerProvider)
    {
        return RuleSet.newInstance(eventLoggerProvider, _rules.values(), _defaultResult);
    }

    private static final class RuleKey
    {
        private final String _identity;

        private final LegacyOperation _operation;

        private final ObjectType _object;

        private final AclRulePredicates _predicates;

        RuleKey(Rule rule)
        {
            _identity = rule.getIdentity();
            _operation = rule.getOperation();
            _object = rule.getObjectType();
            _predicates = rule.getPredicates();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(_identity, _operation, _object, _predicates);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final RuleKey ruleKey = (RuleKey) o;

            return Objects.equals(_identity, ruleKey._identity) &&
                    Objects.equals(_operation, ruleKey._operation) &&
                    Objects.equals(_object, ruleKey._object) &&
                    Objects.equals(_predicates, ruleKey._predicates);
        }

        @Override
        public String toString()
        {
            return "RuleKey[identity=" + _identity +
                    ", operation=" + _operation +
                    ", object=" + _object +
                    ", predicates=" + _predicates + "]";
        }
    }
}
