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
 *
 *
 */
package org.apache.qpid.server.security.access.config;

import java.util.Map;
import java.util.Objects;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.access.plugins.AclRule;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;

public class Rule
{
    /**
     * String indicating all identified.
     */
    public static final String ALL = "all";

    /**
     * String indicating all identified.
     */
    public static final String OWNER = "owner";

    private final String _identity;
    private final LegacyOperation _operation;
    private final ObjectType _object;
    private final AclRulePredicates _predicates;
    private final RuleOutcome _ruleOutcome;
    private final RulePredicate _rulePredicate;

    public Rule(AclRule rule)
    {
        this(rule.getIdentity(), rule.getOperation(), rule.getObjectType(),
                new AclRulePredicatesBuilder(rule.getAttributes()).build(), rule.getOutcome());
    }

    Rule(String identity, LegacyOperation operation, ObjectType object,
         AclRulePredicates predicates, RuleOutcome ruleOutcome)
    {
        super();
        _identity = Objects.requireNonNull(identity);
        _operation = Objects.requireNonNull(operation);
        _object = Objects.requireNonNull(object);
        _ruleOutcome = Objects.requireNonNull(ruleOutcome);
        _predicates = Objects.requireNonNull(predicates);
        _rulePredicate = Objects.requireNonNull(predicates.asSinglePredicate());
    }

    public static boolean isAll(String identity)
    {
        return ALL.equalsIgnoreCase(identity);
    }

    public static boolean isOwner(String identity)
    {
        return OWNER.equalsIgnoreCase(identity);
    }

    public String getIdentity()
    {
        return _identity;
    }

    AclRulePredicates getPredicates()
    {
        return _predicates;
    }

    public RuleOutcome getOutcome()
    {
        return _ruleOutcome;
    }

    public boolean matches(LegacyOperation actionOperation, ObjectType actionObjectType,
                           ObjectProperties actionObjectProperties, Subject subject)
    {
        return operationsMatch(actionOperation) &&
                objectTypesMatch(actionObjectType) &&
                propertiesAttributesMatch(actionOperation, actionObjectProperties, subject);
    }

    private boolean propertiesAttributesMatch(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
    {
        return _rulePredicate.matches(operation, objectProperties, subject);
    }

    private boolean operationsMatch(LegacyOperation actionOperation)
    {
        return LegacyOperation.ALL == getOperation() || getOperation() == actionOperation;
    }

    private boolean objectTypesMatch(ObjectType actionObjectType)
    {
        return ObjectType.ALL == getObjectType() || getObjectType() == actionObjectType;
    }

    public LegacyOperation getOperation()
    {
        return _operation;
    }

    public ObjectType getObjectType()
    {
        return _object;
    }

    public Map<Property, String> getAttributes()
    {
        return _predicates.getParsedProperties();
    }

    public AclRule asAclRule()
    {
        return new AclRuleImpl(this);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o != null && o.getClass() == getClass())
        {
            final Rule rule = (Rule) o;
            return getIdentity().equalsIgnoreCase(rule.getIdentity())
                    && getOperation() == rule.getOperation()
                    && getObjectType() == rule.getObjectType()
                    && getPredicates().equals(rule.getPredicates())
                    && getOutcome() == rule.getOutcome();
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getIdentity(), getOperation(), getObjectType(), getPredicates(), getOutcome());
    }

    @Override
    public String toString()
    {
        return "Rule[" +
                "identity='" + _identity + '\'' +
                "action=Action[operation=" + _operation +
                ", object=" + _object +
                ", predicates=" + _predicates + "]" +
                ", permission=" + _ruleOutcome + ']';
    }

    private static class AclRuleImpl implements AclRule
    {
        private final Rule _rule;

        AclRuleImpl(Rule rule)
        {
            _rule = Objects.requireNonNull(rule);
        }

        @Override
        public String getIdentity()
        {
            return _rule.getIdentity();
        }

        @Override
        public ObjectType getObjectType()
        {
            return _rule.getObjectType();
        }

        @Override
        public LegacyOperation getOperation()
        {
            return _rule.getOperation();
        }

        @Override
        public Map<Property, String> getAttributes()
        {
            return _rule.getAttributes();
        }

        @Override
        public RuleOutcome getOutcome()
        {
            return _rule.getOutcome();
        }
    }

    public static final class Builder
    {
        private String _identity = ALL;
        private LegacyOperation _operation = LegacyOperation.ALL;
        private ObjectType _object = ObjectType.ALL;
        private RuleOutcome _outcome = RuleOutcome.DENY_LOG;

        private final AclRulePredicatesBuilder _aclRulePredicatesBuilder;

        public Builder()
        {
            super();
            _aclRulePredicatesBuilder = new AclRulePredicatesBuilder();
        }

        public Builder withIdentity(String identity)
        {
            _identity = Objects.requireNonNull(identity);
            return this;
        }

        public Builder withOperation(LegacyOperation operation)
        {
            _operation = Objects.requireNonNull(operation);
            return this;
        }

        public Builder withObject(ObjectType object)
        {
            _object = Objects.requireNonNull(object);
            return this;
        }

        public Builder withOutcome(RuleOutcome outcome)
        {
            _outcome = Objects.requireNonNull(outcome);
            return this;
        }

        public Builder withPredicate(String key, String value)
        {
            _aclRulePredicatesBuilder.parse(key, value);
            return this;
        }

        public Builder withPredicate(Property key, String value)
        {
            _aclRulePredicatesBuilder.put(key, value);
            return this;
        }

        public Builder withOwner()
        {
            _identity = OWNER;
            return this;
        }

        public Builder withPredicates(ObjectProperties properties)
        {
            for (final Map.Entry<Property, Object> entry : properties.getAll().entrySet())
            {
                _aclRulePredicatesBuilder.put(entry.getKey(), entry.getValue().toString());
            }
            for (final String name : properties.getAttributeNames())
            {
                _aclRulePredicatesBuilder.put(Property.ATTRIBUTES, name);
            }
            return this;
        }

        public Rule build()
        {
            validate();
            return new Rule(_identity, _operation, _object, _aclRulePredicatesBuilder.build(), _outcome);
        }

        public Rule build(FirewallRuleFactory firewallRuleFactory)
        {
            validate();
            return new Rule(_identity, _operation, _object, _aclRulePredicatesBuilder.build(firewallRuleFactory), _outcome);
        }

        private void validate()
        {
            if (!_object.isSupported(_operation))
            {
                throw new IllegalArgumentException(
                        String.format("Operation %s  is not allowed for %s", _operation, _object));
            }
        }
    }
}
