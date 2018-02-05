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

import static org.apache.qpid.server.model.preferences.GenericPrincipal.principalsContain;
import static org.apache.qpid.server.model.preferences.GenericPrincipal.principalsEqual;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStore;

public class UserPreferencesImpl implements UserPreferences
{
    private static final Comparator<Preference> PREFERENCE_COMPARATOR = new Comparator<Preference>()
    {
        private final Ordering<Comparable> _ordering = Ordering.natural().nullsFirst();

        @Override
        public int compare(final Preference o1, final Preference o2)
        {
            int nameOrder = _ordering.compare(o1.getName(), o2.getName());
            if (nameOrder != 0)
            {
                return nameOrder;
            }
            else
            {
                int typeOrder = _ordering.compare(o1.getType(), o2.getType());
                if (typeOrder != 0)
                {
                    return typeOrder;
                }
                else
                {
                    return o1.getId().compareTo(o2.getId());
                }
            }
        }
    };

    private final Map<UUID, Preference> _preferences;
    private final Map<String, List<Preference>> _preferencesByName;
    private final PreferenceStore _preferenceStore;
    private final TaskExecutor _executor;
    private final ConfiguredObject<?> _associatedObject;

    public UserPreferencesImpl(final TaskExecutor executor,
                               final ConfiguredObject<?> associatedObject,
                               final PreferenceStore preferenceStore,
                               final Collection<Preference> preferences)
    {
        _preferences = new HashMap<>();
        _preferencesByName = new HashMap<>();
        _preferenceStore = preferenceStore;
        _executor = executor;
        _associatedObject = associatedObject;
        for (Preference preference : preferences)
        {
            addPreference(preference);
        }
    }

    @Override
    public ListenableFuture<Void> updateOrAppend(final Collection<Preference> preferences)
    {
        return _executor.submit(new PreferencesTask<Void>("updateOrAppend", preferences)
        {
            @Override
            public Void doOperation()
            {
                doUpdateOrAppend(preferences);
                return null;
            }
        });
    }

    private void doUpdateOrAppend(final Collection<Preference> preferences)
    {
        Collection<Preference> augmentedPreferences = augmentForUpdate(preferences);
        validateNewPreferencesForUpdate(augmentedPreferences);

        Collection<PreferenceRecord> preferenceRecords = new HashSet<>();
        for (Preference preference : augmentedPreferences)
        {
            preferenceRecords.add(PreferenceRecordImpl.fromPreference(preference));
        }
        _preferenceStore.updateOrCreate(preferenceRecords);

        for (Preference preference : augmentedPreferences)
        {
            final Preference oldPreference = _preferences.get(preference.getId());
            if (oldPreference != null)
            {
                _preferencesByName.get(oldPreference.getName()).remove(oldPreference);
            }

            addPreference(preference);
        }
    }

    @Override
    public ListenableFuture<Set<Preference>> getPreferences()
    {
        return _executor.submit(new PreferencesTask<Set<Preference>>("getPreferences")
        {
            @Override
            public Set<Preference> doOperation()
            {
                return doGetPreferences();
            }
        });
    }

    private Set<Preference> doGetPreferences()
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        Set<Preference> preferences = new TreeSet<>(PREFERENCE_COMPARATOR);
        for (Preference preference : _preferences.values())
        {
            if (principalsEqual(currentPrincipal, preference.getOwner()))
            {
                preferences.add(preference);
            }
        }
        return preferences;
    }

    @Override
    public ListenableFuture<Void> replace(final Collection<Preference> preferences)
    {
        return _executor.submit(new PreferencesTask<Void>("replace", preferences)
        {
            @Override
            public Void doOperation()
            {
                doReplaceByType(null, preferences);
                return null;
            }
        });
    }

    @Override
    public ListenableFuture<Void> replaceByType(final String type, final Collection<Preference> preferences)
    {
        return _executor.submit(new PreferencesTask<Void>("replaceByType", type, preferences)
        {
            @Override
            public Void doOperation()
            {
                doReplaceByType(type, preferences);
                return null;
            }
        });
    }

    private void doReplaceByType(final String type, final Collection<Preference> preferences)
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        Collection<Preference> augmentedPreferences = augmentForReplace(preferences);
        validateNewPreferencesForReplaceByType(type, augmentedPreferences);

        Collection<UUID> preferenceRecordsToRemove = new HashSet<>();
        Collection<PreferenceRecord> preferenceRecordsToAdd = new HashSet<>();
        for (Preference preference : _preferences.values())
        {
            if (principalsEqual(preference.getOwner(), currentPrincipal)
                && (type == null || Objects.equals(preference.getType(), type)))
            {
                preferenceRecordsToRemove.add(preference.getId());
            }
        }

        for (Preference preference : augmentedPreferences)
        {
            preferenceRecordsToAdd.add(PreferenceRecordImpl.fromPreference(preference));
        }
        _preferenceStore.replace(preferenceRecordsToRemove, preferenceRecordsToAdd);

        for (UUID id : preferenceRecordsToRemove)
        {
            Preference preference = _preferences.remove(id);
            _preferencesByName.get(preference.getName()).remove(preference);
        }

        for (Preference preference : augmentedPreferences)
        {
            addPreference(preference);
        }
    }

    @Override
    public ListenableFuture<Void> replaceByTypeAndName(final String type,
                                                       final String name,
                                                       final Preference newPreference)
    {
        return _executor.submit(new PreferencesTask<Void>("replaceByTypeAndName", type, name, newPreference)
        {
            @Override
            public Void doOperation()
            {
                doReplaceByTypeAndName(type, name, newPreference);
                return null;
            }
        });
    }

    private void doReplaceByTypeAndName(final String type, final String name, final Preference newPreference)
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        Preference augmentedPreference = newPreference == null ? null : augmentForReplace(Collections.singleton(newPreference)).iterator().next();
        validateNewPreferencesForReplaceByTypeAndName(type, name, augmentedPreference);

        UUID existingPreferenceId = null;
        Iterator<Preference> preferenceIterator = null;
        List<Preference> preferencesWithSameName = _preferencesByName.get(name);
        if (preferencesWithSameName != null)
        {
            preferenceIterator = preferencesWithSameName.iterator();
            while (preferenceIterator.hasNext())
            {
                Preference preference = preferenceIterator.next();
                if (principalsEqual(preference.getOwner(), currentPrincipal)
                    && Objects.equals(preference.getType(), type))
                {
                    existingPreferenceId = preference.getId();
                    break;
                }
            }
        }

        _preferenceStore.replace(existingPreferenceId != null ? Collections.singleton(existingPreferenceId) : Collections.emptyList(),
                                 augmentedPreference == null
                                         ? Collections.emptyList()
                                         : Collections.singleton(PreferenceRecordImpl.fromPreference(augmentedPreference)));

        if (existingPreferenceId != null)
        {
            _preferences.remove(existingPreferenceId);
            preferenceIterator.remove();
        }

        if (augmentedPreference != null)
        {
            addPreference(augmentedPreference);
        }
    }

    @Override
    public ListenableFuture<Void> delete(final String type, final String name, final UUID id)
    {
        return _executor.submit(new PreferencesTask<Void>("delete", type, name, id)
        {
            @Override
            public Void doOperation()
            {
                doDelete(type, name, id);
                return null;
            }
        });
    }

    private void doDelete(final String type, final String name, final UUID id)
    {
        if (type == null && name != null)
        {
            throw new IllegalArgumentException("Cannot specify name without specifying type");
        }

        if (id != null)
        {
            final Set<Preference> allPreferences = doGetPreferences();
            for (Preference preference : allPreferences)
            {
                if (id.equals(preference.getId()))
                {
                    if ((type == null || type.equals(preference.getType()))
                        && (name == null || name.equals(preference.getName())))
                    {
                        doReplaceByTypeAndName(preference.getType(), preference.getName(), null);
                    }
                    break;
                }
            }
        }
        else
        {
            if (type != null && name != null)
            {
                doReplaceByTypeAndName(type, name, null);
            }
            else
            {
                doReplaceByType(type, Collections.<Preference>emptySet());
            }
        }
    }

    @Override
    public ListenableFuture<Set<Preference>> getVisiblePreferences()
    {
        return _executor.submit(new PreferencesTask<Set<Preference>>("getVisiblePreferences")
        {
            @Override
            public Set<Preference> doOperation()
            {
                return doGetVisiblePreferences();
            }
        });
    }

    private Set<Preference> doGetVisiblePreferences()
    {
        Set<Principal> currentPrincipals = getPrincipalsOrThrow();

        Set<Preference> visiblePreferences = new TreeSet<>(PREFERENCE_COMPARATOR);
        for (Preference preference : _preferences.values())
        {
            if (principalsContain(currentPrincipals, preference.getOwner()))
            {
                visiblePreferences.add(preference);
                continue;
            }
            final Set<Principal> visibilityList = preference.getVisibilityList();
            if (visibilityList != null)
            {
                for (Principal principal : visibilityList)
                {
                    if (principalsContain(currentPrincipals, principal))
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
        checkForValidVisibilityLists(preferences);
        checkForConflictWithinCollection(preferences);
        ensureSameTypeAndOwnerForExistingPreferences(preferences);
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
        checkForValidVisibilityLists(Collections.singleton(newPreference));
        ensureSameTypeAndOwnerForExistingPreferences(Collections.singleton(newPreference));
    }

    private void validateNewPreferencesForUpdate(final Collection<Preference> preferences)
    {
        checkForValidVisibilityLists(preferences);
        checkForConflictWithExisting(preferences);
        checkForConflictWithinCollection(preferences);
    }

    private Collection<Preference> augmentForUpdate(final Collection<Preference> preferences)
    {
        HashSet<Preference> augmentedPreferences = new HashSet<>(preferences.size());
        for (final Preference preference : preferences)
        {
            Map<String, Object> attributes = new HashMap<>(preference.getAttributes());
            AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();
            Date currentTime = new Date();
            attributes.put(Preference.LAST_UPDATED_DATE_ATTRIBUTE, currentTime);
            attributes.put(Preference.CREATED_DATE_ATTRIBUTE, currentTime);
            attributes.put(Preference.OWNER_ATTRIBUTE, currentUser);
            if (preference.getId() == null)
            {
                attributes.put(Preference.ID_ATTRIBUTE, UUID.randomUUID());
            }
            else
            {
                Preference existingPreference = _preferences.get(preference.getId());
                if (existingPreference != null)
                {
                    attributes.put(Preference.CREATED_DATE_ATTRIBUTE, existingPreference.getCreatedDate());
                }
            }
            augmentedPreferences.add(PreferenceFactory.fromAttributes(preference.getAssociatedObject(), attributes));
        }
        return augmentedPreferences;
    }

    private Collection<Preference> augmentForReplace(final Collection<Preference> preferences)
    {
        HashSet<Preference> augmentedPreferences = new HashSet<>(preferences.size());
        for (final Preference preference : preferences)
        {
            Map<String, Object> attributes = new HashMap<>(preference.getAttributes());
            AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();
            Date currentTime = new Date();
            attributes.put(Preference.LAST_UPDATED_DATE_ATTRIBUTE, currentTime);
            attributes.put(Preference.CREATED_DATE_ATTRIBUTE, currentTime);
            attributes.put(Preference.OWNER_ATTRIBUTE, currentUser);
            if (preference.getId() == null)
            {
                attributes.put(Preference.ID_ATTRIBUTE, UUID.randomUUID());
            }
            augmentedPreferences.add(PreferenceFactory.fromAttributes(preference.getAssociatedObject(), attributes));
        }
        return augmentedPreferences;
    }

    private void checkForValidVisibilityLists(final Collection<Preference> preferences)
    {
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        if (currentSubject == null)
        {
            throw new IllegalStateException("Current thread does not have a user");
        }

        Set<Principal> principals = currentSubject.getPrincipals();

        for (Preference preference : preferences)
        {
            for (Principal visibilityPrincipal : preference.getVisibilityList())
            {
                if (!principalsContain(principals, visibilityPrincipal))
                {
                    String errorMessage =
                            String.format("Invalid visibilityList, this user does not hold principal '%s'",
                                          visibilityPrincipal);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }
    }

    private void ensureSameTypeAndOwnerForExistingPreferences(final Collection<Preference> preferences)
    {
        Principal currentPrincipal = getMainPrincipalOrThrow();

        for (Preference preference : preferences)
        {
            if (preference.getId() != null && _preferences.containsKey(preference.getId()))
            {
                Preference existingPreference = _preferences.get(preference.getId());
                if (!preference.getType().equals(existingPreference.getType())
                    || !principalsEqual(existingPreference.getOwner(), currentPrincipal))
                {
                    throw new IllegalArgumentException(String.format("Preference Id '%s' already exists",
                                                                     preference.getId()));
                }
            }
        }
    }

    private void checkForConflictWithExisting(final Collection<Preference> preferences)
    {
        ensureSameTypeAndOwnerForExistingPreferences(preferences);

        for (Preference preference : preferences)
        {
            List<Preference> preferencesWithSameName = _preferencesByName.get(preference.getName());
            if (preferencesWithSameName != null)
            {
                for (Preference preferenceWithSameName : preferencesWithSameName)
                {
                    if (principalsEqual(preferenceWithSameName.getOwner(), preference.getOwner())
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
        Principal currentPrincipal = AuthenticatedPrincipal.getCurrentUser();
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

    private void addPreference(final Preference preference)
    {
        _preferences.put(preference.getId(), preference);
        if (!_preferencesByName.containsKey(preference.getName()))
        {
            _preferencesByName.put(preference.getName(), new ArrayList<Preference>());
        }
        _preferencesByName.get(preference.getName()).add(preference);
    }

    private abstract class PreferencesTask<T> implements Task<T, RuntimeException>
    {
        private final Subject _subject;
        private final String _action;
        private final Object[] _arguments;
        private String _argumentString;


        private PreferencesTask(final String action, final Object... arguments)
        {
            _action = action;
            _arguments = arguments;
            _subject = Subject.getSubject(AccessController.getContext());
        }

        @Override
        public T execute() throws RuntimeException
        {
            return Subject.doAs(_subject, new PrivilegedAction<T>()
            {
                @Override
                public T run()
                {
                    return doOperation();
                }
            });
        }

        protected abstract T doOperation();

        @Override
        public String getObject()
        {
            return _associatedObject.getName();
        }

        @Override
        public String getAction()
        {
            return _action;
        }

        @Override
        public String getArguments()
        {
            if (_argumentString == null && _arguments != null)
            {
                _argumentString = _arguments.length == 1 ? String.valueOf(_arguments[0]) : Arrays.toString(_arguments);
            }
            return _argumentString;
        }
    }
}
