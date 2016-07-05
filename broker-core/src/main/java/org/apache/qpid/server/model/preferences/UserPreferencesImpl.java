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
 *
 */

package org.apache.qpid.server.model.preferences;

import java.security.AccessController;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStore;

public class UserPreferencesImpl implements UserPreferences
{
    private final ConfiguredObject<?> _associatedObject;
    private final Map<UUID, Preference> _preferences;
    private final Map<String, List<Preference>> _preferencesByName;
    private final PreferenceStore _preferenceStore;

    public UserPreferencesImpl(final ConfiguredObject<?> associatedObject,
                               final Map<UUID, Preference> userPreferences,
                               final Map<String, List<Preference>> userPreferencesByName,
                               final PreferenceStore preferenceStore)
    {
        _preferences = userPreferences;
        _associatedObject = associatedObject;
        _preferencesByName = userPreferencesByName;
        _preferenceStore = preferenceStore;
    }

    @Override
    public Preference createPreference(final UUID id,
                                       final String type,
                                       final String name,
                                       final String description,
                                       final Set<Principal> visibilitySet,
                                       final Map<String, Object> preferenceValueAttributes)
    {
        final Map<String, PreferenceValueFactoryService> preferenceValueFactories =
                new QpidServiceLoader().getInstancesByType(PreferenceValueFactoryService.class);

        UUID uuid = id;
        if (uuid == null)
        {
            uuid = UUID.randomUUID();
        }

        if (name == null || "".equals(name))
        {
            throw new IllegalArgumentException("Preference name is mandatory");
        }

        String implementationType = type;
        if (type != null && type.startsWith("X-"))
        {
            implementationType = "X-generic";
        }
        final PreferenceValueFactoryService preferenceValueFactory = preferenceValueFactories.get(implementationType);
        if (preferenceValueFactory == null)
        {
            throw new IllegalArgumentException(String.format("Cannot find preference type factory for type '%s'",
                                                             implementationType));
        }

        PreferenceValue preferenceValue = preferenceValueFactory.createInstance(preferenceValueAttributes);
        return new PreferenceImpl(_associatedObject, uuid, name, type, description, visibilitySet, preferenceValue);
    }

    @Override
    public void updateOrAppend(final Collection<Preference> preferences)
    {
        validateNewPreferencesForUpdate(preferences);

        Collection<PreferenceRecord> preferenceRecords = new HashSet<>();
        for (Preference preference : preferences)
        {
            preferenceRecords.add(PreferenceRecordImpl.fromPreference(preference));
        }
        _preferenceStore.updateOrCreate(preferenceRecords);

        for (Preference preference : preferences)
        {
            final Preference oldPreference = _preferences.get(preference.getId());
            if (oldPreference != null)
            {
                _preferencesByName.get(oldPreference.getName()).remove(oldPreference);
            }

            _preferences.put(preference.getId(), preference);

            if (!_preferencesByName.containsKey(preference.getName()))
            {
                _preferencesByName.put(preference.getName(), new ArrayList<Preference>());
            }
            _preferencesByName.get(preference.getName()).add(preference);
        }
    }

    @Override
    public Set<Preference> getPreferences()
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        Set<Preference> preferences = new HashSet<>();
        for (Preference preference : _preferences.values())
        {
            if (currentPrincipal.equals(preference.getOwner()))
            {
                preferences.add(preference);
            }
        }
        return preferences;
    }

    @Override
    public void replace(final Collection<Preference> preferences)
    {
        replaceByType(null, preferences);
    }

    @Override
    public void replaceByType(final String type, final Collection<Preference> preferences)
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        validateNewPreferencesForReplaceByType(type, preferences);

        Collection<UUID> preferenceRecordsToRemove = new HashSet<>();
        Collection<PreferenceRecord> preferenceRecordsToAdd = new HashSet<>();
        for (Preference preference : _preferences.values())
        {
            if (Objects.equals(preference.getOwner(), currentPrincipal)
                && (type == null || Objects.equals(preference.getType(), type)))
            {
                preferenceRecordsToRemove.add(preference.getId());
            }
        }

        for (Preference preference : preferences)
        {
            preferenceRecordsToAdd.add(PreferenceRecordImpl.fromPreference(preference));
        }
        _preferenceStore.replace(preferenceRecordsToRemove, preferenceRecordsToAdd);

        for (UUID id : preferenceRecordsToRemove)
        {
            Preference preference = _preferences.remove(id);
            _preferencesByName.get(preference.getName()).remove(preference);
        }

        for (Preference preference : preferences)
        {
            _preferences.put(preference.getId(), preference);

            String preferenceName = preference.getName();
            if (!_preferencesByName.containsKey(preferenceName))
            {
                _preferencesByName.put(preferenceName, new ArrayList<Preference>());
            }
            _preferencesByName.get(preferenceName).add(preference);
        }
    }

    @Override
    public void replaceByTypeAndName(final String type, final String name, final Preference newPreference)
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        validateNewPreferencesForReplaceByTypeAndName(type, name, newPreference);

        UUID existingPreferenceId = null;
        Iterator<Preference> preferenceIterator = null;
        List<Preference> preferencesWithSameName = _preferencesByName.get(name);
        if (preferencesWithSameName != null)
        {
            preferenceIterator = preferencesWithSameName.iterator();
            while (preferenceIterator.hasNext())
            {
                Preference preference = preferenceIterator.next();
                if (Objects.equals(preference.getOwner(), currentPrincipal)
                    && Objects.equals(preference.getType(), type))
                {
                    existingPreferenceId = preference.getId();
                    break;
                }
            }
        }

        _preferenceStore.replace(Collections.singleton(existingPreferenceId),
                                 newPreference == null
                                         ? Collections.<PreferenceRecord>emptyList()
                                         : Collections.singleton(PreferenceRecordImpl.fromPreference(newPreference)));


        if (existingPreferenceId != null)
        {
            _preferences.remove(existingPreferenceId);
            preferenceIterator.remove();
        }

        if (newPreference != null)
        {
            _preferences.put(newPreference.getId(), newPreference);

            if (!_preferencesByName.containsKey(name))
            {
                _preferencesByName.put(name, new ArrayList<Preference>());
            }
            _preferencesByName.get(name).add(newPreference);
        }
    }

    @Override
    public Set<Preference> getVisiblePreferences()
    {
        Set<Principal> currentPrincipals = getPrincipalsOrThrow();

        Set<Preference> visiblePreferences = new HashSet<>();
        for (Preference preference : _preferences.values())
        {
            if (currentPrincipals.contains(preference.getOwner()))
            {
                continue;
            }
            final Set<Principal> visibilityList = preference.getVisibilityList();
            if (visibilityList != null)
            {
                for (Principal principal : visibilityList)
                {
                    if (currentPrincipals.contains(principal))
                    {
                        visiblePreferences.add(preference);
                        break;
                    }
                }
            }
        }

        return visiblePreferences;
    }

    private void validateNewPreferencesForReplaceByType(final String type, final Collection<Preference> preferences)
    {
        if (type != null)
        {
            for (Preference preference : preferences)
            {
                if (!Objects.equals(preference.getType(), type))
                {
                    throw new IllegalArgumentException(String.format(
                            "Replacing preferences of type '%s' with preferences of different type '%s'",
                            type,
                            preference.getType()));
                }
            }
        }
        checkForValidPrincipal(preferences);
        checkForConflictWithinCollection(preferences);
    }

    private void validateNewPreferencesForReplaceByTypeAndName(final String type,
                                                               final String name,
                                                               final Preference newPreference)
    {
        if (newPreference == null)
        {
            return;
        }
        if (!Objects.equals(newPreference.getType(), type))
        {
            throw new IllegalArgumentException(String.format(
                    "Replacing preference of type '%s' with preference of different type '%s'",
                    type,
                    newPreference.getType()));
        }
        if (!Objects.equals(newPreference.getName(), name))
        {
            throw new IllegalArgumentException(String.format(
                    "Replacing preference with name '%s' with preference of different name '%s'",
                    name,
                    newPreference.getName()));
        }
        checkForValidPrincipal(Collections.singleton(newPreference));
    }

    private void validateNewPreferencesForUpdate(final Collection<Preference> preferences)
    {
        checkForValidPrincipal(preferences);
        checkForConflictWithExisting(preferences);
        checkForConflictWithinCollection(preferences);
    }

    private void checkForValidPrincipal(final Collection<Preference> preferences)
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        for (Preference preference : preferences)
        {
            // validate owner
            if (!currentPrincipal.equals(preference.getOwner()))
            {
                throw new SecurityException(String.format("Preference '%s' not owned by current user.",
                                                          preference.getId().toString()));
            }
        }
    }

    private void checkForConflictWithExisting(final Collection<Preference> preferences)
    {
        for (Preference preference : preferences)
        {
            // check for conflicts with existing preferences
            final Preference storedPreference = _preferences.get(preference.getId());
            if (storedPreference != null && !Objects.equals(storedPreference.getType(), preference.getType()))
            {
                throw new IllegalArgumentException("Cannot change type of preference");
            }
            List<Preference> preferencesWithSameName = _preferencesByName.get(preference.getName());
            if (preferencesWithSameName != null)
            {
                for (Preference preferenceWithSameName : preferencesWithSameName)
                {
                    if (Objects.equals(preferenceWithSameName.getOwner(), preference.getOwner())
                        && Objects.equals(preferenceWithSameName.getType(), preference.getType())
                        && !Objects.equals(preferenceWithSameName.getId(), preference.getId()))
                    {
                        throw new IllegalArgumentException(String.format("Preference '%s' of type '%s' already exists",
                                                                         preference.getName(),
                                                                         preference.getType()));
                    }
                }
            }
        }
    }

    private void checkForConflictWithinCollection(final Collection<Preference> preferences)
    {
        Map<UUID, Preference> checkedPreferences = new HashMap<>(preferences.size());
        Map<String, List<Preference>> checkedPreferencesByName = new HashMap<>(preferences.size());

        for (Preference preference : preferences)
        {
            // check for conflicts within the provided preferences set
            if (checkedPreferences.containsKey(preference.getId()))
            {
                throw new IllegalArgumentException(String.format("Duplicate Id '%s' in update set",
                                                                 preference.getId().toString()));
            }
            List<Preference> checkedPreferencesWithSameName = checkedPreferencesByName.get(preference.getName());
            if (checkedPreferencesWithSameName != null)
            {
                for (Preference preferenceWithSameName : checkedPreferencesWithSameName)
                {
                    if (Objects.equals(preferenceWithSameName.getType(), preference.getType())
                        && !Objects.equals(preferenceWithSameName.getId(), preference.getId()))
                    {
                        throw new IllegalArgumentException(String.format(
                                "Duplicate preference name '%s' of type '%s' in update set",
                                preference.getName(),
                                preference.getType()));
                    }
                }
            }
            else
            {
                checkedPreferencesByName.put(preference.getName(), new ArrayList<Preference>());
            }
            checkedPreferences.put(preference.getId(), preference);
            checkedPreferencesByName.get(preference.getName()).add(preference);
        }
    }

    private Principal getMainPrincipalOrThrow() throws SecurityException
    {
        Principal currentPrincipal = SecurityManager.getCurrentUser();
        if (currentPrincipal == null)
        {
            throw new SecurityException("Current thread does not have a user");
        }
        return currentPrincipal;
    }

    private Set<Principal> getPrincipalsOrThrow() throws SecurityException
    {
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        if (currentSubject == null)
        {
            throw new SecurityException("Current thread does not have a user");
        }
        final Set<Principal> currentPrincipals = currentSubject.getPrincipals();
        if (currentPrincipals == null || currentPrincipals.isEmpty())
        {
            throw new SecurityException("Current thread does not have a user");
        }
        return currentPrincipals;
    }
}
