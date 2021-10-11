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
package org.apache.qpid.server.security.access.config.predicates;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.Property;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;

@FunctionalInterface
public interface RulePredicate
{
    boolean test(LegacyOperation operation, ObjectProperties objectProperties, Subject subject);

    default RulePredicate and(RulePredicate other)
    {
        if (Objects.requireNonNull(other) instanceof Any)
        {
            return this;
        }
        return (operation, objectProperties, subject) ->
                RulePredicate.this.test(operation, objectProperties, subject)
                        && other.test(operation, objectProperties, subject);
    }

    static RulePredicate any()
    {
        return Any.INSTANCE;
    }

    final class Any implements RulePredicate
    {
        public static final RulePredicate INSTANCE = new Any();

        private Any()
        {
            super();
        }

        @Override
        public boolean test(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
        {
            return true;
        }

        @Override
        public RulePredicate and(RulePredicate other)
        {
            return Objects.requireNonNull(other);
        }
    }

    static RulePredicate build(Map<Property, Set<?>> properties)
    {
        return build(new FirewallRuleFactory(), properties);
    }

    static RulePredicate build(FirewallRuleFactory firewallRuleFactory, Map<Property, Set<?>> properties)
    {
        RulePredicate predicate = RulePredicate.any();
        for (final Map.Entry<Property, Set<?>> entry : properties.entrySet())
        {
            predicate = predicate.and(build(firewallRuleFactory, entry.getKey(), entry.getValue()));
        }
        return predicate;
    }

    static RulePredicate build(FirewallRuleFactory firewallRuleFactory, Property property, Set<?> values)
    {
        RulePredicate predicate = RulePredicate.any();
        switch (property)
        {
            case FROM_HOSTNAME:
                if (!values.isEmpty())
                {
                    predicate = firewallRuleFactory.createForHostname(
                            values.stream().map(Object::toString).collect(Collectors.toSet()));
                }
                break;
            case FROM_NETWORK:
                if (!values.isEmpty())
                {
                    predicate = firewallRuleFactory.createForNetwork(
                            values.stream().map(Object::toString).collect(Collectors.toSet()));
                }
                break;
            case ATTRIBUTES:
                predicate = AttributeNames.newInstance(
                        values.stream().map(Object::toString).collect(Collectors.toSet()));
                break;
            case CONNECTION_LIMIT:
            case CONNECTION_FREQUENCY_LIMIT:
                break;
            default:
                for (final Object value : values)
                {
                    if (value instanceof String)
                    {
                        final String str = (String) value;
                        if (AclRulePredicatesBuilder.WILD_CARD.equals(str))
                        {
                            predicate = predicate.and(Some.newInstance(property));
                        }
                        else if (str.endsWith(AclRulePredicatesBuilder.WILD_CARD))
                        {
                            predicate = predicate.and(WildCard.newInstance(property,
                                    str.substring(0, str.length() - AclRulePredicatesBuilder.WILD_CARD_LENGTH)));
                        }
                        else
                        {
                            predicate = predicate.and(Equal.newInstance(property, str));
                        }
                    }
                    else
                    {
                        predicate = predicate.and(Equal.newInstance(property, value));
                    }
                }
        }
        return predicate;
    }
}
