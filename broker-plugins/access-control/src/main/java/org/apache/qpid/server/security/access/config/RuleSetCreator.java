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

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;

final class RuleSetCreator
{
    private final SortedMap<Integer, Rule> _rules = new TreeMap<Integer, Rule>();
    private static final Integer INCREMENT = 10;
    private Result _defaultResult = Result.DENIED;

    RuleSetCreator()
    {
    }


    boolean isValidNumber(Integer number)
    {
        return !_rules.containsKey(number);
    }

    void addRule(Integer number, String identity, RuleOutcome ruleOutcome, LegacyOperation operation)
    {
        AclAction action = new AclAction(operation);
        addRule(number, identity, ruleOutcome, action);
    }

    void addRule(Integer number,
                 String identity,
                 RuleOutcome ruleOutcome,
                 LegacyOperation operation,
                 ObjectType object,
                 ObjectProperties properties)
    {
        AclAction action = new AclAction(operation, object, properties);
        addRule(number, identity, ruleOutcome, action);
    }

    void addRule(Integer number,
                 String identity,
                 RuleOutcome ruleOutcome,
                 LegacyOperation operation,
                 ObjectType object,
                 AclRulePredicates predicates)
    {
        AclAction aclAction = new AclAction(operation, object, predicates);
        addRule(number, identity, ruleOutcome, aclAction);
    }

    private boolean ruleExists(String identity, AclAction action)
    {
        for (Rule rule : _rules.values())
        {
            if (rule.getIdentity().equals(identity) && rule.getAclAction().equals(action))
            {
                return true;
            }
        }
        return false;
    }


    private void addRule(Integer number, String identity, RuleOutcome ruleOutcome, AclAction action)
    {

        if (!action.isAllowed())
        {
            throw new IllegalArgumentException("Action is not allowed: " + action);
        }
        if (ruleExists(identity, action))
        {
            return;
        }

        // set rule number if needed
        Rule rule = new Rule(identity, action, ruleOutcome);
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
    }

    void setDefaultResult(final Result defaultResult)
    {
        _defaultResult = defaultResult;
    }

    Result getDefaultResult()
    {
        return _defaultResult;
    }

    SortedMap<Integer, Rule> getRules()
    {
        return _rules;
    }

    RuleSet createRuleSet(EventLoggerProvider eventLoggerProvider)
    {
        return new RuleSet(eventLoggerProvider, _rules.values(), _defaultResult);
    }
}
