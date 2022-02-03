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
package org.apache.qpid.server.security.access.config;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public final class AclRulePredicates extends AbstractMap<Property, Set<Object>>
        implements RulePredicate
{
    public static final String SEPARATOR = ",";

    private static final Set<Property> JOIN_PROPERTIES =
            EnumSet.of(Property.ATTRIBUTES, Property.FROM_HOSTNAME, Property.FROM_NETWORK);

    private final Map<Property, Set<Object>> _properties;

    private final RulePredicate _rulePredicate;

    AclRulePredicates()
    {
        super();
        _properties = Collections.emptyMap();
        _rulePredicate = RulePredicate.any();
    }

    AclRulePredicates(AclRulePredicatesBuilder builder)
    {
        super();
        _properties = Objects.requireNonNull(builder.newProperties());
        _rulePredicate = Objects.requireNonNull(builder.newRulePredicate());
    }

    AclRulePredicates(FirewallRuleFactory factory, AclRulePredicatesBuilder builder)
    {
        super();
        _properties = Objects.requireNonNull(builder.newProperties());
        _rulePredicate = Objects.requireNonNull(builder.newRulePredicate(factory));
    }

    public Map<Property, Object> getParsedProperties()
    {
        final Map<Property, Object> parsed = new EnumMap<>(Property.class);
        for (final Map.Entry<Property, Set<Object>> entry : _properties.entrySet())
        {
            final Set<Object> values = entry.getValue();
            if (values.size() == 1)
            {
                parsed.put(entry.getKey(), Iterables.getOnlyElement(values).toString());
            }
            else
            {
                parsed.put(entry.getKey(), collect(entry.getKey(), values));
            }
        }
        return parsed;
    }

    private Object collect(Property property, Set<Object> values)
    {
        if (JOIN_PROPERTIES.contains(property))
        {
            return values.stream()
                    .map(Object::toString)
                    .sorted()
                    .collect(Collectors.joining(SEPARATOR));
        }
        else
        {
            return values.stream()
                    .map(Object::toString)
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    @Override
    public boolean matches(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
    {
        return _rulePredicate.matches(operation, objectProperties, subject);
    }

    @Override
    public RulePredicate and(RulePredicate other)
    {
        return _rulePredicate.and(other);
    }

    public RulePredicate asSinglePredicate()
    {
        return _rulePredicate;
    }

    @Override
    public Set<Entry<Property, Set<Object>>> entrySet()
    {
        return Collections.unmodifiableSet(_properties.entrySet());
    }

    @Override
    public int size()
    {
        return _properties.size();
    }

    @Override
    public boolean isEmpty()
    {
        return _properties.isEmpty();
    }

    @Override
    public Set<Object> get(Object key)
    {
        return _properties.getOrDefault(key, Collections.emptySet());
    }

    @Override
    public boolean containsValue(Object value)
    {
        return _properties.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key)
    {
        return _properties.containsKey(key);
    }

    @Override
    public Set<Property> keySet()
    {
        return Collections.unmodifiableSet(_properties.keySet());
    }

    @Override
    public Collection<Set<Object>> values()
    {
        return Collections.unmodifiableCollection(_properties.values());
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("AclRulePredicates[");

        final Joiner joiner = Joiner.on(",");
        joiner.withKeyValueSeparator("=").appendTo(sb, getParsedProperties());
        sb.append("]");
        return sb.toString();
    }
}
