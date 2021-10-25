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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.Property;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public final class AclRulePredicates extends AbstractMap<Property, Set<Object>>
        implements RulePredicate
{
    static final String SEPARATOR = ",";

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
        _properties = newProperties(builder);
        _rulePredicate = RulePredicate.build(builder.getParsedProperties());
    }

    AclRulePredicates(FirewallRuleFactory factory, AclRulePredicatesBuilder builder)
    {
        super();
        _properties = newProperties(builder);
        _rulePredicate = RulePredicate.build(factory, builder.getParsedProperties());
    }

    private Map<Property, Set<Object>> newProperties(AclRulePredicatesBuilder builder)
    {
        final Map<Property, Set<Object>> properties = new EnumMap<>(Property.class);
        for (final Map.Entry<Property, Set<?>> entry : builder.getParsedProperties().entrySet())
        {
            final Set<?> values = entry.getValue();
            if (values != null && !values.isEmpty())
            {
                properties.put(entry.getKey(), ImmutableSet.builder().addAll(values).build());
            }
        }
        return properties;
    }

    public Map<Property, String> getParsedProperties()
    {
        final Map<Property, String> parsed = new EnumMap<>(Property.class);
        for (final Map.Entry<Property, Set<Object>> entry : _properties.entrySet())
        {
            final Set<Object> values = entry.getValue();
            if (values.size() == 1)
            {
                parsed.put(entry.getKey(), Iterables.getOnlyElement(values).toString());
            }
            else
            {
                parsed.put(entry.getKey(),
                        values.stream()
                                .map(Object::toString)
                                .sorted()
                                .collect(Collectors.joining(SEPARATOR)));
            }
        }
        return parsed;
    }

    @Override
    public boolean test(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
    {
        return _rulePredicate.test(operation, objectProperties, subject);
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
