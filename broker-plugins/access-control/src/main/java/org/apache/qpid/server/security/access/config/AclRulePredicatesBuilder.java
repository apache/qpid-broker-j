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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.qpid.server.security.access.config.predicates.RulePredicateBuilder;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.access.util.PrefixTreeSet;
import org.apache.qpid.server.security.access.util.WildCardSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the predicates on an ACL rule by combining predicates relating to the object being operated on
 * (e.g. name=foo) with dynamic rules.
 */
public final class AclRulePredicatesBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AclRulePredicatesBuilder.class);

    private static final AclRulePredicates EMPTY = new AclRulePredicates();

    public static final String WILD_CARD = "*";

    public static final int WILD_CARD_LENGTH = WILD_CARD.length();

    private final Map<Property, Set<?>> _parsedProperties = new EnumMap<>(Property.class);

    private final Set<String> _hostNames;

    private final Set<String> _networks;

    private final Set<String> _attributeNames;

    public AclRulePredicatesBuilder(Map<Property, ?> values)
    {
        this();
        if (values != null)
        {
            for (final Map.Entry<Property, ?> entry : values.entrySet())
            {
                addPropertyValues(entry.getKey(), entry.getValue());
            }
        }
    }

    public AclRulePredicatesBuilder()
    {
        super();
        _hostNames = new HashSet<>();
        _networks = new HashSet<>();
        _attributeNames = new HashSet<>();
        for (final Property property : Property.values())
        {
            if (property == Property.FROM_HOSTNAME)
            {
                _parsedProperties.put(property, _hostNames);
            }
            else if (property == Property.FROM_NETWORK)
            {
                _parsedProperties.put(property, _networks);
            }
            else if (property == Property.ATTRIBUTES)
            {
                _parsedProperties.put(property, _attributeNames);
            }
            else if (Property.isBooleanType(property))
            {
                _parsedProperties.put(property, new HashSet<>());
            }
            else
            {
                _parsedProperties.put(property, new PrefixTreeSet());
            }
        }
    }

    public AclRulePredicates build()
    {
        return _parsedProperties.isEmpty() ? EMPTY : new AclRulePredicates(this);
    }

    public AclRulePredicates build(FirewallRuleFactory firewallRuleFactory)
    {
        return _parsedProperties.isEmpty() ? EMPTY : new AclRulePredicates(firewallRuleFactory, this);
    }

    public AclRulePredicatesBuilder parse(String key, String value)
    {
        final Property property = Property.parse(key);
        if (addPropertyValue(property, value))
        {
            LOGGER.debug("Parsed '{}' with value '{}'", property, value);
        }
        return this;
    }

    public AclRulePredicatesBuilder parse(String key, Set<String> values)
    {
        final Property property = Property.parse(key);
        for (final String value : values)
        {
            if (addPropertyValue(property, value))
            {
                LOGGER.debug("Parsed {} with value {}", property, value);
            }
        }
        return this;
    }

    public AclRulePredicatesBuilder put(Property property, String value)
    {
        addPropertyValue(property, value);
        return this;
    }

    private void addPropertyValues(Property property, Object value)
    {
        if (value instanceof Collection)
        {
            for (final Object v : (Collection<?>) value)
            {
                addPropertyValues(property, v);
            }
        }
        else
        {
            addPropertyValue(property, Objects.toString(value, null));
        }
    }

    private boolean addPropertyValue(Property property, String value)
    {
        if (property == Property.FROM_HOSTNAME)
        {
            checkFirewallRule(property, value, Property.FROM_NETWORK);
            _hostNames.addAll(splitToSet(value));
        }
        else if (property == Property.FROM_NETWORK)
        {
            checkFirewallRule(property, value, Property.FROM_HOSTNAME);
            _networks.addAll(splitToSet(value));
        }
        else if (property == Property.ATTRIBUTES)
        {
            _attributeNames.addAll(splitToSet(value));
        }
        else if (property == Property.CONNECTION_LIMIT)
        {
            LOGGER.warn("The ACL Rule property 'connection_limit' was removed and it is not supported anymore");
            return false;
        }
        else if (property == Property.CONNECTION_FREQUENCY_LIMIT)
        {
            LOGGER.warn("The ACL Rule property 'connection_frequency_limit' was removed and it is not supported anymore");
            return false;
        }
        else
        {
            addPropertyValueImpl(property, value);
        }
        return true;
    }

    private void addPropertyValueImpl(Property property, String value)
    {
        if (value == null)
        {
            _parsedProperties.put(property, WildCardSet.newSet());
            return;
        }
        value = value.trim();
        if (value.isEmpty() || WILD_CARD.equals(value))
        {
            _parsedProperties.put(property, WildCardSet.newSet());
            return;
        }
        final Set<?> values = _parsedProperties.get(property);
        if (Property.isBooleanType(property))
        {
            ((Set<Object>) values).add(parseBoolean(value));
        }
        else
        {
            if (values instanceof PrefixTreeSet)
            {
                ((PrefixTreeSet) values).add(value);
            }
            else
            {
                ((Set<Object>) values).add(value);
            }
        }
    }

    private Boolean parseBoolean(String value)
    {
        if (value.endsWith(WILD_CARD))
        {
            final String prefix = value.substring(0, value.length() - WILD_CARD_LENGTH).toLowerCase(Locale.ENGLISH);
            if (Boolean.TRUE.toString().startsWith(prefix))
            {
                return Boolean.TRUE;
            }
            if (Boolean.FALSE.toString().startsWith(prefix))
            {
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("Unknown boolean value: " + value);
        }
        return Boolean.parseBoolean(value);
    }

    private Set<String> splitToSet(String value)
    {
        if (value == null)
        {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(value.split(AclRulePredicates.SEPARATOR)));
    }

    private void checkFirewallRule(Property property, String value, Property exclusiveProperty)
    {
        if (!_parsedProperties.get(exclusiveProperty).isEmpty())
        {
            throw new IllegalStateException(
                    String.format("Cannot parse '%s=%s' because property '%s' has already been defined",
                            property.toString().toLowerCase(Locale.ENGLISH),
                            value, exclusiveProperty));
        }
    }

    Map<Property, Set<Object>> newProperties()
    {
        final Map<Property, Set<Object>> properties = new EnumMap<>(Property.class);
        for (final Map.Entry<Property, Set<?>> entry : _parsedProperties.entrySet())
        {
            final Set<?> values = entry.getValue();
            if (!values.isEmpty())
            {
                properties.put(entry.getKey(), Collections.unmodifiableSet(values));
            }
        }
        return properties;
    }

    RulePredicate newRulePredicate()
    {
        return new RulePredicateBuilder().build(_parsedProperties);
    }

    RulePredicate newRulePredicate(FirewallRuleFactory factory)
    {
        return new RulePredicateBuilder(factory).build(_parsedProperties);
    }
}
