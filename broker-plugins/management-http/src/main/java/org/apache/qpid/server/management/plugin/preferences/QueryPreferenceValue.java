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

package org.apache.qpid.server.management.plugin.preferences;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.model.preferences.PreferenceValue;

public class QueryPreferenceValue implements PreferenceValue
{

    public static final String SCOPE_ATTRIBUTE = "scope";
    public static final String CATEGORY_ATTRIBUTE = "category";
    public static final String SELECT_ATTRIBUTE = "select";
    public static final String WHERE_ATTRIBUTE = "where";
    public static final String ORDER_BY_ATTRIBUTE = "orderBy";
    public static final String DEFAULT_SCOPE = "";
    public static final String DEFAULT_CATEGORY = "queue";

    private final String _scope;
    private final String _category;
    private final String _select;
    private final String _where;
    private final String _orderBy;
    private final Map<String, Object> _originalAttributeMap;

    public QueryPreferenceValue(final Map<String, Object> preferenceValueAttributes)
    {
        // TODO: how should we treat unrecognised attributes? ignore/pass-through, discard, error?
        try
        {
            _scope = getValue(preferenceValueAttributes, SCOPE_ATTRIBUTE, DEFAULT_SCOPE);
            _category = getValue(preferenceValueAttributes, CATEGORY_ATTRIBUTE, DEFAULT_CATEGORY);
            _select = getValue(preferenceValueAttributes, SELECT_ATTRIBUTE, null);
            _where = getValue(preferenceValueAttributes, WHERE_ATTRIBUTE, null);
            _orderBy = getValue(preferenceValueAttributes, ORDER_BY_ATTRIBUTE, null);
            _originalAttributeMap = Collections.unmodifiableMap(new LinkedHashMap<>(preferenceValueAttributes));
        }
        catch (ClassCastException e)
        {
            throw new IllegalArgumentException("Failed to create QueryPreference", e);
        }
    }

    @Override
    public Map<String, Object> getAttributes()
    {
        return _originalAttributeMap;
    }

    public String getScope()
    {
        return _scope;
    }

    public String getCategory()
    {
        return _category;
    }

    public String getSelect()
    {
        return _select;
    }

    public String getWhere()
    {
        return _where;
    }

    public String getOrderBy()
    {
        return _orderBy;
    }

    private <T> T getValue(Map<String, Object> attributes, String attributeName, T defaultValue)
    {
        if (attributes.containsKey(attributeName))
        {
            return (T) attributes.get(attributeName);
        }
        else
        {
            return defaultValue;
        }
    }
}
