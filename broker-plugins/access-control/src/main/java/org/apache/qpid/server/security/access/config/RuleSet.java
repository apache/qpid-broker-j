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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;

/**
 * Models the rule configuration for the access control plugin.
 */
public class RuleSet implements EventLoggerProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleSet.class);

    private final List<Rule> _rules;
    private final Map<Subject, Map<LegacyOperation, Map<ObjectType, List<Rule>>>> _cache =
                        Collections.synchronizedMap(new WeakHashMap<Subject, Map<LegacyOperation, Map<ObjectType, List<Rule>>>>());

    private final EventLoggerProvider _eventLogger;
    private Result _defaultResult = Result.DENIED;

    public RuleSet(final EventLoggerProvider eventLogger,
                   final Collection<Rule> rules,
                   final Result defaultResult)
    {
        _eventLogger = eventLogger;
        _rules = new ArrayList<>(rules);
        _defaultResult = defaultResult;
    }

    int getRuleCount()
    {
        return _rules.size();
    }

    /**
     * Filtered rules list based on a subject and operation.
     *
     * Allows only enabled rules with identity equal to all, the same, or a group with identity as a member,
     * and operation is either all or the same operation.
     */
    private List<Rule> getRules(final Subject subject, final LegacyOperation operation, final ObjectType objectType)
    {
        final Map<ObjectType, List<Rule>> objects = getObjectToRuleCache(subject, operation);

        // Lookup object type rules for the operation
        if (!objects.containsKey(objectType))
        {
            final Set<Principal> principals = subject.getPrincipals();
            boolean controlled = false;
            List<Rule> filtered = new LinkedList<>();
            for (Rule rule : _rules)
            {
                final Action ruleAction = rule.getAction();
                if ((ruleAction.getOperation() == LegacyOperation.ALL || ruleAction.getOperation() == operation)
                    && (ruleAction.getObjectType() == ObjectType.ALL || ruleAction.getObjectType() == objectType))
                {
                    controlled = true;

                    if (isRelevant(principals,rule))
                    {
                        filtered.add(rule);
                    }
                }
            }

            // Return null if there are no rules at all for this operation and object type
            if (filtered.isEmpty() && !controlled)
            {
                filtered = null;
            }

            // Save the rules we selected
            objects.put(objectType, filtered == null ? null : Collections.unmodifiableList(filtered));

            LOGGER.debug("Cached {} RulesList: {}", objectType, filtered);
        }

        // Return the cached rules
        List<Rule> rules = objects.get(objectType);

        LOGGER.debug("Returning RuleList: {}", rules);

        return rules;
    }

    /**
     * Check the authorisation granted to a particular identity for an operation on an object type with
     * specific properties.
     *
     * Looks up the entire ruleset, which may be cached, for the user and operation and goes through the rules
     * in order to find the first one that matches. Either defers if there are no rules, returns the result of
     * the first match found, or denies access if there are no matching rules. Normally, it would be expected
     * to have a default deny or allow rule at the end of an access configuration however.
     */
    public Result check(Subject subject,
                        LegacyOperation operation,
                        ObjectType objectType,
                        ObjectProperties properties)
    {
        ClientAction action = new ClientAction(operation, objectType, properties);

        LOGGER.debug("Checking action: {}", action);

        // get the list of rules relevant for this request
        List<Rule> rules = getRules(subject, operation, objectType);
        if (rules == null)
        {

            LOGGER.debug("No rules found, returning default result");

            return getDefault();
        }

        final boolean ownerRules = rules.stream()
                                        .anyMatch(rule -> rule.getIdentity().equalsIgnoreCase(Rule.OWNER));

        if (ownerRules)
        {
            rules = new LinkedList<>(rules);

            if (operation == LegacyOperation.CREATE)
            {
                rules.removeIf(rule -> rule.getIdentity().equalsIgnoreCase(Rule.OWNER));
            }
            else
            {
                // Discard OWNER rules if the object wasn't created by the subject
                final String objectCreator = properties.get(ObjectProperties.Property.CREATED_BY);
                final Principal principal =
                        AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subject);
                if (principal == null || !principal.getName().equalsIgnoreCase(objectCreator))
                {
                    rules.removeIf(rule -> rule.getIdentity().equalsIgnoreCase(Rule.OWNER));
                }
            }
        }

        // Iterate through a filtered set of rules dealing with this identity and operation
        for (Rule rule : rules)
        {
            LOGGER.debug("Checking against rule: {}", rule);

            if (action.matches(rule.getAclAction(), subject))
            {
                RuleOutcome ruleOutcome = rule.getRuleOutcome();
                LOGGER.debug("Action matches.  Result: {}", ruleOutcome);
                boolean allowed = ruleOutcome.isAllowed();
                if(ruleOutcome.isLogged())
                {
                    if(allowed)
                    {
                        getEventLogger().message(AccessControlMessages.ALLOWED(
                                action.getOperation().toString(),
                                action.getObjectType().toString(),
                                action.getProperties().toString()));
                    }
                    else
                    {
                        getEventLogger().message(AccessControlMessages.DENIED(
                                action.getOperation().toString(),
                                action.getObjectType().toString(),
                                action.getProperties().toString()));
                    }
                }


                return allowed ? Result.ALLOWED : Result.DENIED;
            }
        }
        LOGGER.debug("Deferring result of ACL check");
        // Defer to the next plugin of this type, if it exists
        return Result.DEFER;
    }

    /** Default deny. */
    public Result getDefault()
    {
        return _defaultResult;

    }

    /**
      * Returns all rules in the {@link RuleSet}.   Primarily intended to support unit-testing.
      * @return map of rules
      */
    public List<Rule> getAllRules()
     {
         return Collections.unmodifiableList(_rules);
     }

    private boolean isRelevant(final Set<Principal> principals, final Rule rule)
    {
        if (rule.getIdentity().equalsIgnoreCase(Rule.ALL) ||
            rule.getIdentity().equalsIgnoreCase(Rule.OWNER))
        {
            return true;
        }
        else
        {
            for (Iterator<Principal> iterator = principals.iterator(); iterator.hasNext();)
            {
                final Principal principal = iterator.next();

                if (rule.getIdentity().equalsIgnoreCase(principal.getName()))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private Map<ObjectType, List<Rule>> getObjectToRuleCache(final Subject subject, final LegacyOperation operation)
    {
        // Lookup identity in cache and create empty operation map if required
        Map<LegacyOperation, Map<ObjectType, List<Rule>>> operations = _cache.get(subject);
        if (operations == null)
        {
            operations = Collections.synchronizedMap(new EnumMap<LegacyOperation, Map<ObjectType, List<Rule>>>(LegacyOperation.class));
            _cache.put(subject, operations);
        }

        // Lookup operation and create empty object type map if required
        Map<ObjectType, List<Rule>> objects = operations.get(operation);
        if (objects == null)
        {
            objects = Collections.synchronizedMap(new EnumMap<ObjectType, List<Rule>>(ObjectType.class));
            operations.put(operation, objects);
        }
        return objects;
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger.getEventLogger();
    }
}
