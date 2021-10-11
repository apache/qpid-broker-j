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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

public final class ObjectProperties
{
    static final String EMPTY_STRING = "";

    private final Set<String> _attributeNames = new HashSet<>();

    private final EnumMap<Property, Object> _properties = new EnumMap<>(Property.class);

    private String _description = EMPTY_STRING;

    public ObjectProperties()
    {
        super();
    }

    public ObjectProperties(Property property, String value)
    {
        super();
        put(property, value);
    }

    public ObjectProperties(ObjectProperties copy)
    {
        super();
        if (copy != null)
        {
            _properties.putAll(copy._properties);
            _attributeNames.addAll(copy._attributeNames);
            _description = copy._description;
        }
    }

    public ObjectProperties(String name)
    {
        super();
        setName(name);
    }

    public ObjectProperties withDescription(String description)
    {
        _description = description == null ? EMPTY_STRING : description;
        return this;
    }

    public Object get(Property key)
    {
        return _properties.get(key);
    }

    public Map<Property, Object> getAll()
    {
        return new EnumMap<>(_properties);
    }

    public Object getName()
    {
        return _properties.get(Property.NAME);
    }

    public void setName(String name)
    {
        _properties.put(Property.NAME, name == null ? EMPTY_STRING : name);
    }

    public void setCreatedBy(Object user)
    {
        if (user != null)
        {
            put(Property.CREATED_BY, user.toString());
        }
    }

    public void setOwner(Object owner)
    {
        if (owner != null)
        {
            put(Property.OWNER, owner.toString());
        }
    }

    public Set<String> getAttributeNames()
    {
        return Collections.unmodifiableSet(_attributeNames);
    }

    public void addAttributeNames(Collection<String> attributeNames)
    {
        if (attributeNames != null && !attributeNames.isEmpty())
        {
            _attributeNames.addAll(attributeNames);
        }
    }

    public void addAttributeNames(String... attributeNames)
    {
        if (attributeNames != null && attributeNames.length > 0)
        {
            _attributeNames.addAll(Arrays.asList(attributeNames));
        }
    }

    public Object put(Property key, String value)
    {
        return _properties.put(key, value == null ? EMPTY_STRING : value.trim());
    }

    public Object put(Property key, boolean value)
    {
        return _properties.put(key, value);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o instanceof ObjectProperties)
        {
            final ObjectProperties that = (ObjectProperties) o;
            return _attributeNames.equals(that._attributeNames) &&
                    _properties.equals(that._properties);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return 31 * _attributeNames.hashCode() + _properties.hashCode();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("ObjectProperties[");
        final Joiner joiner = Joiner.on(",");
        if (!_properties.isEmpty())
        {
            sb.append("properties=[");
            joiner.withKeyValueSeparator("=").appendTo(sb, _properties);
            sb.append("]");
        }
        if (!_attributeNames.isEmpty())
        {
            if (sb.length() > 1)
            {
                sb.append(",");
            }
            sb.append(Property.ATTRIBUTES.name());
            sb.append("=[");
            joiner.appendTo(sb, _attributeNames);
            sb.append("]");
        }
        if (!_description.isEmpty())
        {
            if (sb.length() > 1)
            {
                sb.append(", ");
            }
            sb.append(_description);
        }
        sb.append("]");
        return sb.toString();
    }
}
