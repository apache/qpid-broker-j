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

package org.apache.qpid.server.model.preferences;

import java.security.Principal;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.SecurityManager;

public class PreferenceImpl implements Preference
{
    private final String _name;
    private final String _description;
    private final Set<Principal> _visibilitySet;
    private final PreferenceValue _preferenceValue;
    private final UUID _id;
    private final Principal _owner;
    private final ConfiguredObject<?> _associatedObject;
    private final String _type;
    private final Date _lastUpdatedDate;

    public PreferenceImpl(final ConfiguredObject<?> associatedObject,
                          final UUID uuid,
                          final String name,
                          final String type,
                          final String description,
                          final Set<Principal> visibilitySet,
                          final PreferenceValue preferenceValue)
    {
        _lastUpdatedDate = new Date();
        _associatedObject = associatedObject;
        _id = uuid;
        _name = name;
        _type = type;
        _description = description;
        _owner = SecurityManager.getCurrentUser();
        _visibilitySet = (visibilitySet == null ? ImmutableSet.<Principal>of() : ImmutableSet.copyOf(visibilitySet));
        _preferenceValue = preferenceValue;
    }

    @Override
    public UUID getId()
    {
        return _id;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public String getType()
    {
        return _type;
    }

    @Override
    public String getDescription()
    {
        return _description;
    }

    @Override
    public Principal getOwner()
    {
        return _owner;
    }

    @Override
    public ConfiguredObject<?> getAssociatedObject()
    {
        return _associatedObject;
    }

    @Override
    public Set<Principal> getVisibilityList()
    {
        return _visibilitySet;
    }

    @Override
    public Date getLastUpdatedDate()
    {
        return new Date(_lastUpdatedDate.getTime());
    }

    @Override
    public PreferenceValue getValue()
    {
        return _preferenceValue;
    }

    @Override
    public Map<String, Object> getAttributes()
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(ID_ATTRIBUTE, _id);
        map.put(NAME_ATTRIBUTE, _name);
        map.put(TYPE_ATTRIBUTE, _type);
        map.put(DESCRIPTION_ATTRIBUTE, _description);
        map.put(OWNER_ATTRIBUTE, _owner == null ? "" : _owner.getName());
        map.put(ASSOCIATED_OBJECT_ATTRIBUTE, _associatedObject.getId());
        Set<String> visibilityList = new HashSet<>(_visibilitySet.size());
        for (Principal principal : _visibilitySet)
        {
            visibilityList.add(principal.getName());
        }
        map.put(VISIBILITY_LIST_ATTRIBUTE, visibilityList);
        map.put(LAST_UPDATED_DATE_ATTRIBUTE, new Date(_lastUpdatedDate.getTime()));
        map.put(VALUE_ATTRIBUTE, _preferenceValue.getAttributes());
        return map;
    }
}
