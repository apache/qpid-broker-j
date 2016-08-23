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

package org.apache.qpid.server.store.preferences;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceFactory;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;

public class PreferencesRecoverer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PreferencesRecoverer.class);
    private TaskExecutor _executor;

    public PreferencesRecoverer(final TaskExecutor executor)
    {
        _executor = executor;
    }


    public void recoverPreferences(ConfiguredObject<?> parent,
                                   Collection<PreferenceRecord> preferenceRecords,
                                   PreferenceStore preferencesStore)
    {
        Set<UUID> corruptedRecords = new HashSet<>();
        Map<UUID, Collection<PreferenceRecord>> objectToRecordMap = new HashMap<>();
        for (PreferenceRecord preferenceRecord : preferenceRecords)
        {
            UUID associatedObjectId = getAssociatedObjectId(preferenceRecord.getAttributes());
            if (associatedObjectId == null)
            {
                LOGGER.info("Could not find associated object for preference : {}", preferenceRecord.getId());
                corruptedRecords.add(preferenceRecord.getId());
            }
            else
            {
                Collection<PreferenceRecord> objectPreferences = objectToRecordMap.get(associatedObjectId);
                if (objectPreferences == null)
                {
                    objectPreferences = new HashSet<>();
                    objectToRecordMap.put(associatedObjectId, objectPreferences);
                }
                objectPreferences.add(preferenceRecord);
            }
        }

        setUserPreferences(parent, objectToRecordMap, preferencesStore, corruptedRecords);

        if (!objectToRecordMap.isEmpty())
        {
            LOGGER.warn("Could not recover preferences associated with: {}", objectToRecordMap.keySet());
            for (Collection<PreferenceRecord> records: objectToRecordMap.values())
            {
                for (PreferenceRecord record : records)
                {
                    corruptedRecords.add(record.getId());
                }
            }
        }

        if (!corruptedRecords.isEmpty())
        {
            LOGGER.warn("Removing unrecoverable corrupted preferences: {}", corruptedRecords);
            preferencesStore.replace(corruptedRecords, Collections.<PreferenceRecord>emptySet());
        }
    }

    private void setUserPreferences(ConfiguredObject<?> associatedObject,
                                    Map<UUID, Collection<PreferenceRecord>> objectToRecordMap,
                                    PreferenceStore preferenceStore, final Set<UUID> corruptedRecords)
    {
        final Collection<PreferenceRecord> preferenceRecords = objectToRecordMap.remove(associatedObject.getId());
        Collection<Preference> recoveredPreferences = new ArrayList<>();
        if (preferenceRecords != null)
        {
            for (PreferenceRecord preferenceRecord : preferenceRecords)
            {
                Map<String, Object> attributes = preferenceRecord.getAttributes();
                try
                {
                    Preference recoveredPreference = PreferenceFactory.fromAttributes(associatedObject, attributes);
                    validateRecoveredPreference(recoveredPreference);
                    recoveredPreferences.add(recoveredPreference);
                }
                catch (IllegalArgumentException e)
                {
                    LOGGER.info(String.format("Cannot recover preference '%s/%s'",
                                              preferenceRecord.getId(),
                                              attributes.get(Preference.NAME_ATTRIBUTE)), e);
                    corruptedRecords.add(preferenceRecord.getId());
                }
            }
        }
        associatedObject.setUserPreferences(new UserPreferencesImpl(_executor,
                                                                    associatedObject,
                                                                    preferenceStore,
                                                                    recoveredPreferences));

        if (!(associatedObject instanceof PreferencesRoot))
        {
            Collection<Class<? extends ConfiguredObject>> childrenCategories =
                    associatedObject.getModel().getChildTypes(associatedObject.getCategoryClass());
            for (Class<? extends ConfiguredObject> childCategory : childrenCategories)
            {
                Collection<? extends ConfiguredObject> children = associatedObject.getChildren(childCategory);
                for (ConfiguredObject<?> child : children)
                {
                    setUserPreferences(child, objectToRecordMap, preferenceStore, corruptedRecords);
                }
            }
        }
    }

    private void validateRecoveredPreference(final Preference recoveredPreference)
    {
        if (recoveredPreference.getId() == null)
        {
            throw new IllegalArgumentException("Recovered preference has no id");
        }
        if (recoveredPreference.getOwner() == null)
        {
            throw new IllegalArgumentException("Recovered preference has no owner");
        }
        if (recoveredPreference.getCreatedDate() == null)
        {
            throw new IllegalArgumentException("Recovered preference has no createdDate");
        }
        if (recoveredPreference.getLastUpdatedDate() == null)
        {
            throw new IllegalArgumentException("Recovered preference has no lastUpdatedDate");
        }
    }

    private UUID getAssociatedObjectId(final Map<String, Object> preferenceRecordAttributes)
    {
        if (preferenceRecordAttributes == null)
        {
            return null;
        }
        Object associatedObjectIdObject = preferenceRecordAttributes.get(Preference.ASSOCIATED_OBJECT_ATTRIBUTE);
        if (associatedObjectIdObject == null || !(associatedObjectIdObject instanceof String))
        {
            return null;
        }
        UUID associatedObjectId;
        try
        {
            associatedObjectId = UUID.fromString((String) associatedObjectIdObject);
        }
        catch (Exception e)
        {
            return null;
        }
        return associatedObjectId;
    }
}
