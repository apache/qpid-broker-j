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

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;

final class RuleSetCreator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleSetCreator.class);
    private static final Integer INCREMENT = 10;

    private final NavigableMap<Integer, Rule> _rules = new TreeMap<>();
    private final Set<RuleKey> _ruleSet = new HashSet<>();

    private Result _defaultResult = Result.DENIED;

    RuleSetCreator()
    {
        super();
    }

    boolean isValidNumber(Integer number)
    {
        return !_rules.containsKey(number);
    }

    void addRule(Integer number, String identity, RuleOutcome ruleOutcome, LegacyOperation operation)
    {
        addRule(number, identity, ruleOutcome, new AclAction(operation));
    }

    void addRule(Integer number,
                 String identity,
                 RuleOutcome ruleOutcome,
                 LegacyOperation operation,
                 ObjectType object,
                 ObjectProperties properties)
    {
        addRule(number, identity, ruleOutcome, new AclAction(operation, object, properties));
    }

    void addRule(Integer number,
                 String identity,
                 RuleOutcome ruleOutcome,
                 LegacyOperation operation,
                 ObjectType object,
                 AclRulePredicates predicates)
    {
        addRule(number, identity, ruleOutcome, new AclAction(operation, object, predicates));
    }

    private void addRule(Integer number, String identity, RuleOutcome ruleOutcome, AclAction action)
    {
        if (!action.isAllowed())
        {
            throw new IllegalArgumentException("Action is not allowed: " + action);
        }
        final RuleKey key = new RuleKey(identity, action);
        if (_ruleSet.contains(key))
        {
            LOGGER.warn("Duplicate rule for the {}", key);
            return;
        }
        // set rule number if needed
        final Rule rule = new Rule(identity, action, ruleOutcome);
        if (number == null)
        {
            if (_rules.isEmpty())
            {
                number = 0;
            }
            else
            {
                number = _rules.lastKey() + INCREMENT;
            }
        }
        // save rule
        _rules.put(number, rule);
        _ruleSet.add(new RuleKey(rule));
    }

    void setDefaultResult(final Result defaultResult)
    {
        _defaultResult = defaultResult;
    }

    RuleSet createRuleSet(EventLoggerProvider eventLoggerProvider)
    {
        return new RuleSet(eventLoggerProvider, _rules.values(), _defaultResult);
    }

    private static final class RuleKey
    {
        private final String _identity;
        private final AclAction _action;

        RuleKey(String identity, AclAction action)
        {
            super();
            _identity = identity;
            _action = action;
        }

        RuleKey(Rule rule)
        {
            this(rule.getIdentity(), rule.getAclAction());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(_identity, _action);
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
            return Objects.equals(_identity, ruleKey._identity) && Objects.equals(_action, ruleKey._action);
        }

        @Override
        public String toString()
        {
            return "RuleKey[identity=" + _identity + ", action=" + _action + "]";
        }
    }
}
