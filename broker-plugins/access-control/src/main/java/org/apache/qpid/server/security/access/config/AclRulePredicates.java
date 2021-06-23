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

import com.google.common.collect.Sets;
import org.apache.qpid.server.security.access.config.ObjectProperties.Property;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the predicates on an ACL rule by combining predicates relating to the object being operated on
 * (e.g. name=foo) with dynamic rules.
 */
public class AclRulePredicates
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AclRulePredicates.class);

    private static final String SEPARATOR = ",";

    private final ObjectProperties _properties = new ObjectProperties();
    private final Map<Property, String> _parsedProperties = new HashMap<>();
    private volatile FirewallRule _firewallRule = s -> true;
    private volatile FirewallRuleFactory _firewallRuleFactory = new FirewallRuleFactory();

    public AclRulePredicates()
    {
    }

    public AclRulePredicates(Map<Property, String> values)
    {
        if (values != null)
        {
            for (Map.Entry<Property, String> entry : values.entrySet())
            {
                addPropertyValue(entry.getKey(), entry.getValue());
            }
        }
    }

    public void parse(String key, String value)
    {
        ObjectProperties.Property property = ObjectProperties.Property.parse(key);

        if (addPropertyValue(property, value))
        {
            _parsedProperties.put(property, value);
            LOGGER.debug("Parsed {} with value {}", property, value);
        }
    }

    private boolean addPropertyValue(final Property property, final String value)
    {
        final FirewallRule firewallRule = _firewallRule;
        if (property == Property.FROM_HOSTNAME)
        {
            checkFirewallRuleNotAlreadyDefined(property, value, Property.FROM_NETWORK);
            _firewallRule = firewallRule.and(_firewallRuleFactory.createForHostname(value.split(SEPARATOR)));
        }
        else if (property == Property.FROM_NETWORK)
        {
            checkFirewallRuleNotAlreadyDefined(property, value, Property.FROM_HOSTNAME);
            _firewallRule = firewallRule.and(_firewallRuleFactory.createForNetwork(value.split(SEPARATOR)));
        }
        else if (property == Property.ATTRIBUTES)
        {
            _properties.setAttributeNames(Sets.newHashSet((value.split(SEPARATOR))));
        }
        else if (property == Property.CONNECTION_LIMIT)
        {
            LOGGER.warn("The ACL Rule property 'connection_limit' has been deprecated");
            return false;
        }
        else if (property == Property.CONNECTION_FREQUENCY_LIMIT)
        {
            LOGGER.warn("The ACL Rule property 'connection_frequency_limit' has been deprecated");
            return false;
        }
        else
        {
            _properties.put(property, value);
        }
        return true;
    }

    private void checkFirewallRuleNotAlreadyDefined(Property property, String value, Property... exclusiveProperty)
    {
        checkPropertyAlreadyDefined(property);
        for (Property p : exclusiveProperty)
        {
            if (_parsedProperties.containsKey(p))
            {
                throw new IllegalStateException(
                        String.format("Cannot parse '%s=%s' because property '%s' has already been defined",
                                      property.toString().toLowerCase(),
                                      value,
                                      p));
            }
        }
    }

    private void checkPropertyAlreadyDefined(Property property)
    {
        if (_parsedProperties.containsKey(property))
        {
            throw new IllegalStateException(String.format("Property '%s' has already been defined", property.toString().toLowerCase()));
        }
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final AclRulePredicates that = (AclRulePredicates) o;

        return _parsedProperties.equals(that._parsedProperties);
    }

    @Override
    public int hashCode()
    {
        return _parsedProperties.hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("AclRulePredicates[%s]",
                             _parsedProperties.entrySet()
                                              .stream()
                                              .map(e -> e.getKey() + "=" + e.getValue())
                                              .collect(Collectors.joining(" ")));
    }

    FirewallRule getFirewallRule()
    {
        return _firewallRule;
    }

    ObjectProperties getObjectProperties()
    {
        return _properties;
    }

    Map<Property, String> getParsedProperties()
    {
        return Collections.unmodifiableMap(_parsedProperties);
    }

    void setFirewallRuleFactory(FirewallRuleFactory firewallRuleFactory)
    {
        _firewallRuleFactory = firewallRuleFactory;
    }
}
