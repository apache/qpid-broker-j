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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.qpid.server.security.access.config.AclRulePredicatesBuilder;
import org.apache.qpid.server.security.access.config.Property;
import org.apache.qpid.server.security.access.config.RulePredicate;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.access.util.PrefixTreeSet;

public final class RulePredicateBuilder
{
    private final FirewallRuleFactory _factory;

    public RulePredicateBuilder(FirewallRuleFactory factory)
    {
        super();
        _factory = Objects.requireNonNull(factory);
    }

    public RulePredicateBuilder()
    {
        super();
        _factory = new FirewallRuleFactory();
    }

    public RulePredicate build(Map<Property, ? extends Collection<?>> properties)
    {
        RulePredicate predicate = RulePredicate.any();
        for (final Map.Entry<Property, ? extends Collection<?>> entry : properties.entrySet())
        {
            predicate = predicate.and(buildPredicate(entry.getKey(), entry.getValue()));
        }
        return predicate;
    }

    private RulePredicate buildPredicate(Property property, Collection<?> values)
    {
        if (values == null || values.isEmpty())
        {
            return RulePredicate.any();
        }
        switch (property)
        {
            case FROM_HOSTNAME:
                return _factory.createForHostname(toSet(values));
            case FROM_NETWORK:
                return _factory.createForNetwork(toSet(values));
            case ATTRIBUTES:
                return AttributeNames.newInstance(toSet(values));
            default:
                return buildGenericPredicate(property, values);
        }
    }

    private Set<String> toSet(Collection<?> values)
    {
        return values.stream().map(Object::toString).collect(Collectors.toSet());
    }

    private RulePredicate buildGenericPredicate(Property property, Collection<?> values)
    {
        if (values instanceof PrefixTreeSet && values.size() > 2)
        {
            return MultiValue.newInstance(property, ((PrefixTreeSet) values).toPrefixTree());
        }
        RulePredicate predicate = RulePredicate.none();
        for (final Object value : values)
        {
            if (value instanceof String)
            {
                predicate = predicate.or(string(property, (String) value));
            }
            else
            {
                predicate = predicate.or(Equal.newInstance(property, value));
            }
        }
        return predicate;
    }

    private RulePredicate string(Property property, String value)
    {
        if (AclRulePredicatesBuilder.WILD_CARD.equals(value))
        {
            return AnyValue.newInstance(property);
        }
        if (value.endsWith(AclRulePredicatesBuilder.WILD_CARD))
        {
            return WildCard.newInstance(property,
                    value.substring(0, value.length() - AclRulePredicatesBuilder.WILD_CARD_LENGTH));
        }
        return Equal.newInstance(property, value);
    }
}
