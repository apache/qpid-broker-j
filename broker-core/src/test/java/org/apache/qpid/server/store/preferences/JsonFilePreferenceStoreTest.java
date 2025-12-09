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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class JsonFilePreferenceStoreTest extends UnitTestBase
{
    private File _storeFile;
    private ObjectMapper _objectMapper;
    private PreferenceStoreUpdater _updater;
    private JsonFilePreferenceStore _store;

    @BeforeEach
    public void setUp() throws Exception
    {
        _storeFile = new File(TMP_FOLDER, getTestName() + System.currentTimeMillis() + ".preferences.json");
        _store = new JsonFilePreferenceStore(_storeFile.getPath(), SystemConfig.DEFAULT_POSIX_FILE_PERMISSIONS);
        _objectMapper = new ObjectMapper();
        _updater = mock(PreferenceStoreUpdater.class);
        when(_updater.getLatestVersion()).thenReturn(BrokerModel.MODEL_VERSION);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _store.close();
        FileUtils.delete(_storeFile, true);
    }

    @Test
    public void testOpenAndLoad() throws Exception
    {
        final UUID prefId = randomUUID();
        final Map<String, Object> attributes = Map.of("test1", "test2");
        createSingleEntryTestFile(prefId, attributes);

        final Collection<PreferenceRecord> records = _store.openAndLoad(_updater);

        assertEquals(1, (long) records.size(), "Unexpected size of stored preferences");

        final PreferenceRecord storeRecord = records.iterator().next();
        assertEquals(prefId, storeRecord.getId(), "Unexpected stored preference id");
        assertEquals(attributes, new HashMap<>(storeRecord.getAttributes()),
                "Unexpected stored preference attributes");

        verify(_updater, never()).updatePreferences(anyString(), anyCollection());
    }

    @Test
    public void testUpdateOrCreate() throws Exception
    {
        final UUID id = randomUUID();
        final Map<String, Object> attributes = Map.of("test1", "test2");
        final PreferenceRecord record = new PreferenceRecordImpl(id, attributes);

        _store.openAndLoad(_updater);
        _store.updateOrCreate(Set.of(record));

        assertSinglePreferenceRecordInStore(id, attributes);
    }

    @Test
    public void testReplace() throws Exception
    {
        final UUID prefId = randomUUID();
        final Map<String, Object> attributes = Map.of("test1", "test2");
        createSingleEntryTestFile(prefId, attributes);

        final UUID newPrefId = randomUUID();
        final Map<String, Object> newAttributes = Map.of("test3", "test4");
        final PreferenceRecord newRecord = new PreferenceRecordImpl(newPrefId, newAttributes);

        _store.openAndLoad(_updater);
        _store.replace(Set.of(prefId), Set.of(newRecord));

        assertSinglePreferenceRecordInStore(newPrefId, newAttributes);
    }

    @Test
    public void testReplaceToDelete() throws Exception
    {
        final UUID prefId = randomUUID();
        final Map<String, Object> attributes = Map.of("test1", "test2");
        createSingleEntryTestFile(prefId, attributes);

        _store.openAndLoad(_updater);
        _store.replace(Set.of(prefId), List.of());

        assertStoreVersionAndSizeAndGetData(0);
    }

    @Test
    public void testUpdateFailIfNotOpened()
    {
        assertThrows(IllegalStateException.class,
                () -> _store.updateOrCreate(List.of()),
                "Should not be able to update or create");
    }

    @Test
    public void testReplaceFailIfNotOpened()
    {
        assertThrows(IllegalStateException.class,
                () -> _store.replace(List.of(), List.of()),
                "Should not be able to replace");
    }

    private void createSingleEntryTestFile(final UUID prefId, final Map<String, Object> attributes) throws IOException
    {
        final Map<String, Object> record = Map.of("id", prefId,
                "attributes", attributes);
        final Map<String, Object> content = Map.of("version", BrokerModel.MODEL_VERSION,
                "preferences", Set.of(record));
        _objectMapper.writeValue(_storeFile, content);
    }

    private void assertSinglePreferenceRecordInStore(final UUID id, final Map<String, Object> attributes)
            throws java.io.IOException
    {
        final Collection preferences = assertStoreVersionAndSizeAndGetData(1);
        final Map preferenceMap = (Map) preferences.iterator().next();
        assertEquals(id.toString(), preferenceMap.get("id"), "Unexpected id");

        final Object storedAttributes = preferenceMap.get("attributes");
        assertNotNull(storedAttributes, "Attributes should not be null");
        assertEquals(attributes, new HashMap<>((Map) storedAttributes), "Unexpected attributes");
    }

    private Collection assertStoreVersionAndSizeAndGetData(final int expectedSize) throws IOException
    {
        final Map<String, Object> storedData =
                _objectMapper.readValue(_storeFile, new TypeReference<HashMap<String, Object>>() { });

        assertEquals(BrokerModel.MODEL_VERSION, storedData.get("version"), "Unexpected stored version");
        final Collection preferences = (Collection) storedData.get("preferences");

        assertEquals(expectedSize, (long) preferences.size(), "Unexpected size of preference records");
        return preferences;
    }
}