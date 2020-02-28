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

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An set of properties for an access control v2 rule {@link ObjectType}.
 *
 * The {@link #propertiesMatch(ObjectProperties)} method is intended to be used when determining precedence of rules, and
 * {@link #equals(Object)} and {@link #hashCode()} are intended for use in maps. This is due to the wildcard matching
 * described above.
 */
public class ObjectProperties
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectProperties.class);
    static final String WILD_CARD = "*";

    static final ObjectProperties EMPTY = new ObjectProperties();
    private Set<String> _attributeNames;

    public enum Property
    {
        ROUTING_KEY,
        NAME,
        QUEUE_NAME,
        OWNER,
        TYPE,
        ALTERNATE,
        DURABLE,
        EXCLUSIVE,
        TEMPORARY,
        AUTO_DELETE,
        COMPONENT,
        PACKAGE,
        CLASS,
        FROM_NETWORK,
        FROM_HOSTNAME,
        VIRTUALHOST_NAME,
        METHOD_NAME,
        ATTRIBUTES,
        CREATED_BY,
        CONNECTION_LIMIT,
        CONNECTION_FREQUENCY_LIMIT;

        private static final Map<String, Property> _canonicalNameToPropertyMap = new HashMap<String, ObjectProperties.Property>();

        static
        {
            for (Property property : values())
            {
                _canonicalNameToPropertyMap.put(property.getCanonicalName(), property);
            }
        }

        /**
         * Properties are parsed using their canonical name (see {@link #getCanonicalName(String)})
         * so that, for the sake of user-friendliness, the ACL file parses is insensitive to
         * case and underscores.
         */
        public static Property parse(String text)
        {
            String propertyName = getCanonicalName(text);
            Property property = _canonicalNameToPropertyMap.get(propertyName);

            if(property == null)
            {
                throw new IllegalArgumentException("Not a valid property: " + text
                        + " because " + propertyName
                        + " is not in " + _canonicalNameToPropertyMap.keySet());
            }
            else
            {
                return property;
            }
        }

        public String getCanonicalName()
        {
            return getCanonicalName(name());
        }

        private static String getCanonicalName(String name)
        {
            return name.replace("_","").toLowerCase();
        }
    }

    private final EnumMap<Property, String> _properties = new EnumMap<Property, String>(Property.class);

    public ObjectProperties()
    {
    }

    public ObjectProperties(Property property, String value)
    {
        _properties.put(property, value);
    }

    public ObjectProperties(ObjectProperties copy)
    {
        _properties.putAll(copy._properties);
    }

    public ObjectProperties(String name)
    {
        setName(name);
    }


    public ObjectProperties(String virtualHostName, String exchangeName, String routingKey)
    {
        super();

        setName(exchangeName);

        put(Property.ROUTING_KEY, routingKey);
        put(Property.VIRTUALHOST_NAME, virtualHostName);
    }

    public Boolean isSet(Property key)
    {
        return _properties.containsKey(key) && Boolean.valueOf(_properties.get(key));
    }

    public String get(Property key)
    {
        return _properties.get(key);
    }

    public String getName()
    {
        return _properties.get(Property.NAME);
    }

    public void setName(String name)
    {
        _properties.put(Property.NAME, name);
    }

    Set<String> getAttributeNames()
    {
        return _attributeNames;
    }

    void setAttributeNames(Set<String> attributeNames)
    {
        _attributeNames = attributeNames == null ? null : new HashSet<>(attributeNames);
    }

    public String put(Property key, String value)
    {
        return _properties.put(key, value == null ? "" : value.trim());
    }

    public void put(Property key, Boolean value)
    {
        if (value != null)
        {
            _properties.put(key, Boolean.toString(value));
        }
    }

    boolean propertiesMatch(ObjectProperties other)
    {
        if (other._properties.keySet().isEmpty())
        {
            return true;
        }

        if (!_properties.keySet().containsAll(other._properties.keySet()))
        {
            return false;
        }

        for (Map.Entry<Property,String> entry : other._properties.entrySet())
        {
            Property key = entry.getKey();
            String ruleValue = entry.getValue();

            String thisValue = _properties.get(key);

            if (!valueMatches(thisValue, ruleValue))
            {
                return false;
            }
        }

        return true;
    }

    boolean attributesMatch(final ObjectProperties other)
    {
        return !(other._attributeNames != null
                 && (_attributeNames == null || !other._attributeNames.containsAll(_attributeNames)));
    }

    private boolean valueMatches(String thisValue, String ruleValue)
    {
        return (ruleValue == null
                || ruleValue.equals("")
                || ruleValue.equals(thisValue))
                || ruleValue.equals(WILD_CARD)
                || (ruleValue.endsWith(WILD_CARD)
                        && thisValue != null
                        && thisValue.length() >= ruleValue.length() - 1
                        && thisValue.startsWith(ruleValue.substring(0, ruleValue.length() - 1)));
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

        final ObjectProperties that = (ObjectProperties) o;

        if (_attributeNames != null ? !_attributeNames.equals(that._attributeNames) : that._attributeNames != null)
        {
            return false;
        }
        return _properties != null ? _properties.equals(that._properties) : that._properties == null;
    }

    @Override
    public int hashCode()
    {
        int result = _attributeNames != null ? _attributeNames.hashCode() : 0;
        result = 31 * result + (_properties != null ? _properties.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Joiner joiner = Joiner.on(",");
        joiner.withKeyValueSeparator("=").appendTo(sb, _properties);
        if (_attributeNames != null && !_attributeNames.isEmpty())
        {
            if (!_properties.isEmpty())
            {
                sb.append(",");
            }
            sb.append("ATTRIBUTES=[");
            joiner.appendTo(sb, _attributeNames);
            sb.append("]");
        }
        return sb.toString();
    }

    public boolean isEmpty()
    {
        return _properties.isEmpty();
    }

    public Map<Property, String> asPropertyMap()
    {
        return Collections.unmodifiableMap(_properties);
    }
}
