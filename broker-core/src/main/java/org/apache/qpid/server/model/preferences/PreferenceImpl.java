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

    public PreferenceImpl(final ConfiguredObject<?> associatedObject,
                          final UUID uuid,
                          final String name,
                          final String type,
                          final String description,
                          final Set<Principal> visibilitySet,
                          final PreferenceValue preferenceValue)
    {
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
    public PreferenceValue getValue()
    {
        return _preferenceValue;
    }

    @Override
    public Map<String, Object> getAttributes()
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", _id);
        map.put("name", _name);
        map.put("type", _type);
        map.put("description", _description);
        map.put("owner", _owner.getName());
        map.put("associatedObject", _associatedObject.getId());
        Set<String> visibilityList = new HashSet<>(_visibilitySet.size());
        for (Principal principal : _visibilitySet)
        {
            visibilityList.add(principal.getName());
        }
        map.put("visibilityList", visibilityList);
        //map.put("createdDate", foundPreference.get);
        //map.put("lastUpdatedDate", foundPreference.get);
        map.put("value", _preferenceValue.getAttributes());
        return map;
    }
}
