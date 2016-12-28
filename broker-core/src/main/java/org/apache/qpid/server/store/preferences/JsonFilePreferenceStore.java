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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.store.AbstractJsonFileStore;
import org.apache.qpid.server.store.StoreException;

public class JsonFilePreferenceStore extends AbstractJsonFileStore implements PreferenceStore
{
    private static final String DEFAULT_FILE_NAME = "userPreferences";
    private final String _storePath;
    private final String _posixFilePermissions;
    private final ObjectMapper _objectMapper;
    private final Map<UUID, StoredPreferenceRecord> _recordMap;
    private StoreState _storeState = StoreState.CLOSED;

    public JsonFilePreferenceStore(String path, String posixFilePermissions)
    {
        super();
        _storePath = path;
        _posixFilePermissions = posixFilePermissions;
        _objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true).enable(SerializationFeature.INDENT_OUTPUT);
        _recordMap = new LinkedHashMap<>();
    }

    @Override
    public synchronized Collection<PreferenceRecord> openAndLoad(final PreferenceStoreUpdater updater) throws StoreException
    {
        if (_storeState != StoreState.CLOSED)
        {
            throw new IllegalStateException(String.format("PreferenceStore cannot be opened when in state '%s'",
                                                          _storeState));
        }

        try
        {
            setup(DEFAULT_FILE_NAME,
                  _storePath,
                  _posixFilePermissions,
                  Collections.singletonMap("version", BrokerModel.MODEL_VERSION));
            StoreContent storeContent;
            try
            {
                storeContent = _objectMapper.readValue(getConfigFile(), StoreContent.class);
            }
            catch (IOException e)
            {
                throw new StoreException("Failed to read preferences from store", e);
            }

            ModelVersion storedVersion = ModelVersion.fromString(storeContent.getVersion());
            ModelVersion currentVersion =
                    new ModelVersion(BrokerModel.MODEL_MAJOR_VERSION, BrokerModel.MODEL_MINOR_VERSION);
            if (currentVersion.lessThan(storedVersion))
            {
                throw new IllegalStateException(String.format(
                        "Cannot downgrade preference store storedVersion from '%s' to '%s'",
                        currentVersion.toString(),
                        BrokerModel.MODEL_VERSION));
            }

            Collection<PreferenceRecord> records = Arrays.<PreferenceRecord>asList(storeContent.getPreferences());
            if (storedVersion.lessThan(currentVersion))
            {
                records = updater.updatePreferences(storedVersion.toString(), records);
                storeContent.setVersion(BrokerModel.MODEL_VERSION);
                storeContent.setPreferences(records.toArray(new StoredPreferenceRecord[records.size()]));
                save(storeContent);
            }

            for (StoredPreferenceRecord preferenceRecord : storeContent.getPreferences())
            {
                _recordMap.put(preferenceRecord.getId(), preferenceRecord);
            }

            _storeState = StoreState.OPENED;

            return records;
        }
        catch (Exception e)
        {
            _storeState = StoreState.ERRORED;
            close();
            throw e;
        }
    }

    @Override
    public synchronized void close()
    {
        if (_storeState != StoreState.CLOSED)
        {
            cleanup();
            _recordMap.clear();

            _storeState = StoreState.CLOSED;
        }
    }

    @Override
    public synchronized void updateOrCreate(final Collection<PreferenceRecord> preferenceRecords)
    {
        if (_storeState != StoreState.OPENED)
        {
            throw new IllegalStateException("PreferenceStore is not opened");
        }

        if (preferenceRecords.isEmpty())
        {
            return;
        }

        updateOrCreateInternal(preferenceRecords);
    }

    @Override
    public synchronized void replace(final Collection<UUID> preferenceRecordsToRemove,
                        final Collection<PreferenceRecord> preferenceRecordsToAdd)
    {
        if (_storeState != StoreState.OPENED)
        {
            throw new IllegalStateException("PreferenceStore is not opened");
        }

        if (preferenceRecordsToRemove.isEmpty() && preferenceRecordsToAdd.isEmpty())
        {
            return;
        }

        _recordMap.keySet().removeAll(preferenceRecordsToRemove);
        updateOrCreateInternal(preferenceRecordsToAdd);
    }

    @Override
    public synchronized void onDelete()
    {
        close();
        delete(_storePath);
    }

    @Override
    protected ObjectMapper getSerialisationObjectMapper()
    {
        return _objectMapper;
    }

    private void updateOrCreateInternal(final Collection<PreferenceRecord> preferenceRecords)
    {
        for (PreferenceRecord preferenceRecord : preferenceRecords)
        {
            _recordMap.put(preferenceRecord.getId(), new StoredPreferenceRecord(preferenceRecord));
        }

        final Collection<StoredPreferenceRecord> values = _recordMap.values();
        StoreContent newContent = new StoreContent(BrokerModel.MODEL_VERSION, values.toArray(new StoredPreferenceRecord[values.size()]));
        save(newContent);
    }

    private enum StoreState
    {
        CLOSED, OPENED, ERRORED;
    }

    private static class StoreContent
    {
        private String _version;
        private StoredPreferenceRecord[] _preferences = new StoredPreferenceRecord[0];

        public StoreContent()
        {
            super();
        }

        public StoreContent(final String modelVersion,
                            final StoredPreferenceRecord[] storedPreferenceRecords)
        {
            _version = modelVersion;
            _preferences = storedPreferenceRecords;
        }

        public String getVersion()
        {
            return _version;
        }

        public void setVersion(final String version)
        {
            _version = version;
        }

        public StoredPreferenceRecord[] getPreferences()
        {
            return _preferences;
        }

        public void setPreferences(final StoredPreferenceRecord[] preferences)
        {
            _preferences = preferences == null ? new StoredPreferenceRecord[0] : preferences;
        }
    }

    private static class StoredPreferenceRecord implements PreferenceRecord
    {
        private UUID _id;
        private Map<String, Object> _attributes;


        public StoredPreferenceRecord()
        {
            super();
        }

        public StoredPreferenceRecord(final PreferenceRecord preferenceRecord)
        {
            _id = preferenceRecord.getId();
            _attributes = Collections.unmodifiableMap(new LinkedHashMap<>(preferenceRecord.getAttributes()));
        }

        @Override
        public UUID getId()
        {
            return _id;
        }

        public void setId(final UUID id)
        {
            _id = id;
        }

        @Override
        public Map<String, Object> getAttributes()
        {
            return _attributes;
        }

        public void setAttributes(final Map<String, Object> attributes)
        {
            _attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        }
    }
}
