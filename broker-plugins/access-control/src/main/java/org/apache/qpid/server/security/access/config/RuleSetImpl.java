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

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.security.Principal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Composite inspector that delegates the rule check to specialized inspector for given operation and object type.
 * Rule inspector is a 'smart' ordered list of rules.
 */
final class RuleSetImpl extends AbstractList<Rule> implements RuleSet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleSet.class);

    private static final String CHECKING_AGAINST_RULE = "Checking against rule: {}";
    private static final String CHECKING_ACTION_OPERATION_OBJECT_PROPERTIES =
            "Checking action: operation={}, object={}, properties={}";

    private final List<Rule> _rules;

    private final Map<LegacyOperation, Map<ObjectType, RuleInspector>> _cache;

    private final EventLoggerProvider _eventLogger;

    private final DefaultResultInspector _defaultInspector;

    RuleSetImpl(RuleSetBuilder builder)
    {
        super();
        _rules = new ArrayList<>(builder);
        _eventLogger = builder.getEventLogger();

        _defaultInspector = builder.getDefaultInspector();
        _cache = builder.buildCache();
    }

    @Override
    public Result getDefault()
    {
        return _defaultInspector.getDefaultResult();
    }

    @Override
    public Result check(Subject subject,
                        LegacyOperation operation,
                        ObjectType objectType,
                        ObjectProperties properties)
    {
        return _cache.get(operation)
                .get(objectType)
                .check(subject, operation, objectType, properties);
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger.getEventLogger();
    }

    @Override
    public Rule get(int index)
    {
        return _rules.get(index);
    }

    @Override
    public int size()
    {
        return _rules.size();
    }

    /**
     * Inspector for given operation and object type and so it contains only rules that match operation and object type.
     * The inspector caches the specialized inspectors for given principals of the subject in a map.
     */
    static final class CachedInspector implements RuleInspector
    {
        private final Map<Set<String>, RuleInspector> _cache = new ConcurrentHashMap<>();

        private final Set<String> _ruleIdentities;

        private final RuleInspectorFactory _factory;

        CachedInspector(RuleInspectorFactory factory)
        {
            super();
            _ruleIdentities = new HashSet<>(factory.allRuleIdentities());
            _factory = factory;
        }

        /**
         * Populates the cache with basic cases, the set of principals without any relevant principal or
         * the set with only one relevant principal. If the rules don't utilize groups than all possible cases will be
         * cached at the beginning.
         * @return Cached inspector with populated cache
         */
        public CachedInspector init()
        {
            final Set<String> empty = Collections.emptySet();
            _cache.put(empty, _factory.newInspector(empty));

            for (final String ruleIdentity : _ruleIdentities)
            {
                final Set<String> identities = Collections.singleton(ruleIdentity);
                _cache.put(identities, _factory.newInspector(identities));
            }
            return this;
        }

        @Override
        public Result check(Subject subject, LegacyOperation operation,
                            ObjectType objectType, ObjectProperties properties)
        {
            final Set<String> relevantPrincipals = collectPrincipalNames(subject);
            relevantPrincipals.retainAll(_ruleIdentities);

            return _cache.computeIfAbsent(relevantPrincipals, _factory::newInspector)
                    .check(subject, operation, objectType, properties);
        }

        private Set<String> collectPrincipalNames(Subject subject)
        {
            final Set<Principal> principals = subject.getPrincipals();
            final Set<String> principalNames = new HashSet<>(principals.size());
            for (final Principal principal : principals)
            {
                principalNames.add(principal.getName());
            }
            return principalNames;
        }
    }

    /**
     * If there is no rule for given operation and object type then the default result has to be returned.
     * The default result is DENIED.
     */
    static final class DefaultResultInspector implements RuleInspector
    {
        private final Result _defaultResult;

        DefaultResultInspector(Result defaultResult)
        {
            super();
            _defaultResult = Optional.ofNullable(defaultResult).orElse(Result.DENIED);
        }

        @Override
        public Result check(Subject subject, LegacyOperation operation,
                            ObjectType objectType, ObjectProperties properties)
        {
            LOGGER.debug("No rules found, returning default result");
            return _defaultResult;
        }

        public Result getDefaultResult()
        {
            return _defaultResult;
        }
    }

    /**
     * Rule inspector is a 'smart' ordered list of rules for given operation and object type that match subject
     * principals. Hence, the rule list has been already filtered based on the subject principals and so it contains
     * only rules that match the operation, object type and subject principals.
     */
    private static final class RuleBasedInspector implements RuleInspector
    {
        private final EventLoggerProvider _logger;

        private final Rule[] _rules;

        RuleBasedInspector(Collection<? extends Rule> rules, EventLoggerProvider logger)
        {
            _logger = logger;
            _rules = rules.toArray(new Rule[0]);
        }

        @Override
        public Result check(Subject subject,
                            LegacyOperation operation,
                            ObjectType objectType,
                            ObjectProperties properties)
        {
            LOGGER.debug(CHECKING_ACTION_OPERATION_OBJECT_PROPERTIES, operation, objectType, properties);
            for (final Rule rule : _rules)
            {
                LOGGER.debug(CHECKING_AGAINST_RULE, rule);
                if (rule.predicatesMatch(operation, properties, subject))
                {
                    return rule.getOutcome().logResult(_logger, operation, objectType, properties);
                }
            }
            LOGGER.debug("Deferring result of ACL check");
            return Result.DEFER;
        }
    }

    /**
     * Rule inspector with 'owner' logic.
     */
    private static final class RuleBasedInspectorWithOwnerFiltering implements RuleInspector
    {
        // Iteration through array is faster than using a collection.
        private final Rule[] _rules;

        private final EventLoggerProvider _logger;

        RuleBasedInspectorWithOwnerFiltering(Collection<? extends Rule> rules, EventLoggerProvider logger)
        {
            super();
            _logger = logger;
            _rules = rules.toArray(new Rule[0]);
        }

        @Override
        public Result check(Subject subject,
                            LegacyOperation operation,
                            ObjectType objectType,
                            ObjectProperties properties)
        {
            LOGGER.debug(CHECKING_ACTION_OPERATION_OBJECT_PROPERTIES, operation, objectType, properties);

            final Object objectCreator = properties.get(Property.CREATED_BY);
            final Principal principal = AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subject);

            // Discard OWNER rules if the object wasn't created by the subject
            if (principal != null && principal.getName().equals(objectCreator))
            {
                for (final Rule rule : _rules)
                {
                    LOGGER.debug(CHECKING_AGAINST_RULE, rule);
                    if (rule.predicatesMatch(operation, properties, subject))
                    {
                        return rule.getOutcome().logResult(_logger, operation, objectType, properties);
                    }
                }
            }
            else
            {
                for (final Rule rule : _rules)
                {
                    LOGGER.debug(CHECKING_AGAINST_RULE, rule);
                    // Discarding owner rule
                    if (!rule.isForOwner() &&
                            rule.predicatesMatch(operation, properties, subject))
                    {
                        return rule.getOutcome().logResult(_logger, operation, objectType, properties);
                    }
                }
            }
            LOGGER.debug("Deferring result of ACL check");
            return Result.DEFER;
        }
    }

    private abstract static class AbstractInspectorFactory implements RuleInspectorFactory
    {
        private final EventLoggerProvider _logger;

        // Iteration through array is faster than using a collection.
        private final Rule[] _rules;

        private final Set<String> _allRuleIdentities;

        abstract boolean matchAnyIdentity(Rule rule);

        abstract RuleInspector newInspector(List<? extends Rule> list, EventLoggerProvider logger);

        AbstractInspectorFactory(List<? extends Rule> rules, EventLoggerProvider logger)
        {
            super();
            final List<? extends Rule> filterRules = filterSuppressedRules(rules);
            _rules = filterRules.toArray(new Rule[0]);
            _logger = Objects.requireNonNull(logger);
            _allRuleIdentities = collectRuleIdentities(filterRules);
        }

        @Override
        public Set<String> allRuleIdentities()
        {
            return Collections.unmodifiableSet(_allRuleIdentities);
        }

        @Override
        public RuleInspector newInspector(Set<String> principalNames)
        {
            final List<Rule> filteredRules = new ArrayList<>();
            for (final Rule rule : _rules)
            {
                if (matchAnyIdentity(rule) || principalNames.contains(rule.getIdentity()))
                {
                    filteredRules.add(rule);
                }
            }
            return newInspector(filteredRules, _logger);
        }

        private Set<String> collectRuleIdentities(Collection<? extends Rule> rules)
        {
            return rules.stream()
                    .filter(rule -> !rule.isForOwnerOrAll())
                    .map(Rule::getIdentity)
                    .collect(Collectors.toSet());
        }

        /**
         * The final rule always match for any principal (identity). Hence, any rule after the final rule is never
         * checked and the rules suppressed by the final rule can be ignored.
         *
         * @param rules the list of the rules
         * @return a filtered list of the rules. If there is any final rule then the final rule is the last one.
         */
        private List<? extends Rule> filterSuppressedRules(List<? extends Rule> rules)
        {
            final ListIterator<? extends Rule> iter = rules.listIterator();
            while (iter.hasNext())
            {
                final Rule rule = iter.next();
                if (isFinalRule(rule))
                {
                    return rules.subList(0, iter.nextIndex());
                }
            }
            return rules;
        }

        private boolean isFinalRule(Rule rule)
        {
            return rule.anyPropertiesMatch() && rule.isForAll();
        }
    }

    static final class RuleBasedInspectorFactory extends AbstractInspectorFactory
    {
        public static RuleInspectorFactory newInstance(List<? extends Rule> rules, EventLoggerProvider logger)
        {
            return new RuleBasedInspectorFactory(rules, logger);
        }

        RuleBasedInspectorFactory(List<? extends Rule> rules, EventLoggerProvider logger)
        {
            super(rules, logger);
        }

        @Override
        boolean matchAnyIdentity(Rule rule)
        {
            return rule.isForAll();
        }

        @Override
        RuleInspector newInspector(List<? extends Rule> filteredRules, EventLoggerProvider logger)
        {
            return new RuleBasedInspector(filteredRules, logger);
        }
    }

    static final class RuleBasedInspectorWithOwnerFilteringFactory extends AbstractInspectorFactory
    {
        public static RuleInspectorFactory newInstance(List<? extends Rule> rules, EventLoggerProvider logger)
        {
            return new RuleBasedInspectorWithOwnerFilteringFactory(rules, logger);
        }

        RuleBasedInspectorWithOwnerFilteringFactory(List<? extends Rule> rules, EventLoggerProvider logger)
        {
            super(rules, logger);
        }

        @Override
        boolean matchAnyIdentity(Rule rule)
        {
            return rule.isForOwnerOrAll();
        }

        @Override
        RuleInspector newInspector(List<? extends Rule> filteredRules, EventLoggerProvider logger)
        {
            return new RuleBasedInspectorWithOwnerFiltering(filteredRules, logger);
        }
    }
}
