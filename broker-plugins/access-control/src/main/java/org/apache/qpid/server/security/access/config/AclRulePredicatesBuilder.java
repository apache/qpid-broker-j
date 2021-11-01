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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;

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
                addPropertyValue(entry.getKey(), Objects.toString(entry.getValue(), null));
            }
        }
    }

    public AclRulePredicatesBuilder()
    {
        super();
        for (final Property property : Property.values())
        {
            _parsedProperties.put(property, Collections.emptySet());
        }
        _hostNames = new HashSet<>();
        _parsedProperties.put(Property.FROM_HOSTNAME, _hostNames);
        _networks = new HashSet<>();
        _parsedProperties.put(Property.FROM_NETWORK, _networks);
        _attributeNames = new HashSet<>();
        _parsedProperties.put(Property.ATTRIBUTES, _attributeNames);
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

    public AclRulePredicatesBuilder put(Property property, String value)
    {
        addPropertyValue(property, value);
        return this;
    }

    private boolean addPropertyValue(final Property property, final String value)
    {
        if (property == Property.FROM_HOSTNAME)
        {
            checkFirewallRuleNotAlreadyDefined(property, value, Property.FROM_NETWORK);
            _hostNames.addAll(splitToSet(value));
        }
        else if (property == Property.FROM_NETWORK)
        {
            checkFirewallRuleNotAlreadyDefined(property, value, Property.FROM_HOSTNAME);
            _networks.addAll(splitToSet(value));
        }
        else if (property == Property.ATTRIBUTES)
        {
            _attributeNames.addAll(splitToSet(value));
        }
        else
        {
            _parsedProperties.put(property, Collections.singleton(sanitiseValue(property, value)));
        }
        return true;
    }

    private Object sanitiseValue(Property property, String value)
    {
        if (value == null)
        {
            return WILD_CARD;
        }
        value = value.trim();
        if (value.isEmpty() || WILD_CARD.equals(value))
        {
            return WILD_CARD;
        }
        if (Property.isBooleanType(property))
        {
            return parseBoolean(value);
        }
        return value;
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

    private HashSet<String> splitToSet(String value)
    {
        return new HashSet<>(Arrays.asList(value.split(AclRulePredicates.SEPARATOR)));
    }

    private void checkFirewallRuleNotAlreadyDefined(Property property, String value, Property exclusiveProperty)
    {
        checkPropertyAlreadyDefined(property);
        if (!_parsedProperties.get(exclusiveProperty).isEmpty())
        {
            throw new IllegalStateException(
                    String.format("Cannot parse '%s=%s' because property '%s' has already been defined",
                            property.toString().toLowerCase(Locale.ENGLISH),
                            value, exclusiveProperty));
        }
    }

    private void checkPropertyAlreadyDefined(Property property)
    {
        if (!_parsedProperties.get(property).isEmpty())
        {
            throw new IllegalStateException(String.format("Property '%s' has already been defined",
                    property.toString().toLowerCase(Locale.ENGLISH)));
        }
    }

    Map<Property, Set<?>> getParsedProperties()
    {
        return Collections.unmodifiableMap(_parsedProperties);
    }
}
