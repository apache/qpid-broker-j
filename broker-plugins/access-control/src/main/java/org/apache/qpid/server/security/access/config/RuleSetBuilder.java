/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.access.config;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.RuleInspector.RuleInspectorFactory;
import org.apache.qpid.server.security.access.config.RuleSet.Builder;
import org.apache.qpid.server.security.access.config.RuleSetImpl.CachedInspector;
import org.apache.qpid.server.security.access.config.RuleSetImpl.DefaultResultInspector;
import org.apache.qpid.server.security.access.config.RuleSetImpl.RuleBasedInspectorFactory;
import org.apache.qpid.server.security.access.config.RuleSetImpl.RuleBasedInspectorWithOwnerFilteringFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RuleSetBuilder extends AbstractList<Rule> implements Builder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleSetBuilder.class);

    private final Map<LegacyOperation, Map<ObjectType, List<Rule>>> _ruleMap =
            new EnumMap<>(LegacyOperation.class);

    private final List<Rule> _ruleList = new ArrayList<>();

    private final EventLoggerProvider _eventLogger;

    private DefaultResultInspector _defaultInspector = new DefaultResultInspector(Result.DENIED);

    RuleSetBuilder(EventLoggerProvider eventLogger)
    {
        super();
        _eventLogger = eventLogger;
    }

    @Override
    public Builder setDefaultResult(Result result)
    {
        _defaultInspector = new DefaultResultInspector(result);
        return this;
    }

    @Override
    public Builder addAllRules(Collection<? extends Rule> rules)
    {
        addAll(rules);
        return this;
    }

    @Override
    public RuleSet build()
    {
        return new RuleSetImpl(this);
    }

    @Override
    public Rule get(int index)
    {
        return _ruleList.get(index);
    }

    @Override
    public int size()
    {
        return _ruleList.size();
    }

    @Override
    public boolean add(Rule rule)
    {
        if (validate(rule))
        {
            addRuleToOperationMap(rule);
            return _ruleList.add(rule);
        }
        return false;
    }

    private boolean validate(Rule rule)
    {
        // Owner identity does not have any meaning in case of create operation.
        if (rule.getOperation() == LegacyOperation.CREATE && rule.isForOwner())
        {
            LOGGER.warn("Invalid rule with create operation for owner '{}'", rule);
            return false;
        }
        return true;
    }

    EventLoggerProvider getEventLogger()
    {
        return _eventLogger;
    }

    DefaultResultInspector getDefaultInspector()
    {
        return _defaultInspector;
    }

    Map<LegacyOperation, Map<ObjectType, RuleInspector>> buildCache()
    {
        final Map<LegacyOperation, Map<ObjectType, RuleInspector>> operationMap =
                new EnumMap<>(LegacyOperation.class);

        for (final LegacyOperation operation : LegacyOperation.values())
        {
            final Map<ObjectType, RuleInspector> objectMap = new EnumMap<>(ObjectType.class);
            for (final ObjectType object : ObjectType.values())
            {
                final List<Rule> rules = groupBy(operation, object);
                objectMap.put(object, convertToInspector(rules));
            }
            operationMap.put(operation, Collections.unmodifiableMap(objectMap));
        }
        return Collections.unmodifiableMap(operationMap);
    }

    private RuleInspector convertToInspector(List<? extends Rule> rules)
    {
        // Return fixed result if there are no rules at all for given operation and object type.
        if (rules.isEmpty())
        {
            return _defaultInspector;
        }
        // In case of any rule with 'owner' identity the special 'owner' logic is needed.
        if (rules.stream().anyMatch(Rule::isForOwner))
        {
            return newInspector(RuleBasedInspectorWithOwnerFilteringFactory.newInstance(rules, _eventLogger));
        }
        return newInspector(RuleBasedInspectorFactory.newInstance(rules, _eventLogger));
    }

    private RuleInspector newInspector(RuleInspectorFactory factory)
    {
        // The constant factory provides the same inspector for any input. Hence, caching is not needed.
        if (factory.isConstant())
        {
            return factory.newInspector(Collections.emptySet());
        }
        return new CachedInspector(factory).init();
    }

    private List<Rule> groupBy(LegacyOperation operation, ObjectType objectType)
    {
        return Collections.unmodifiableList(_ruleMap.getOrDefault(operation, Collections.emptyMap())
                .getOrDefault(objectType, Collections.emptyList()));
    }

    private void addRuleToOperationMap(Rule rule)
    {
        if (LegacyOperation.ALL == rule.getOperation())
        {
            for (final LegacyOperation operation : LegacyOperation.values())
            {
                addRuleToObjectTypeMap(_ruleMap.computeIfAbsent(operation,
                        opera -> new EnumMap<>(ObjectType.class)), rule);
            }
        }
        else
        {
            addRuleToObjectTypeMap(_ruleMap.computeIfAbsent(rule.getOperation(),
                    opera -> new EnumMap<>(ObjectType.class)), rule);
        }
    }

    private void addRuleToObjectTypeMap(Map<ObjectType, List<Rule>> objectTypeMap, Rule rule)
    {
        if (ObjectType.ALL == rule.getObjectType())
        {
            for (final ObjectType object : ObjectType.values())
            {
                objectTypeMap.computeIfAbsent(object, obj -> new ArrayList<>()).add(rule);
            }
        }
        else
        {
            objectTypeMap.computeIfAbsent(rule.getObjectType(), obj -> new ArrayList<>()).add(rule);
        }
    }
}
