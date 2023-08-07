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

package org.apache.qpid.server.store.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.sleepycat.bind.tuple.ByteBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.model.preferences.PreferenceTestHelper;
import org.apache.qpid.server.store.berkeleydb.tuple.MapBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.UUIDTupleBinding;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class BDBPreferenceStoreTest extends UnitTestBase
{
    private File _storeFile;
    private PreferenceStoreUpdater _updater;
    private BDBPreferenceStore _preferenceStore;
    private List<PreferenceRecord> _testInitialRecords;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _storeFile = new File(TMP_FOLDER, getTestName() + System.currentTimeMillis() + ".preferences.bdb");
        boolean result = _storeFile.mkdirs();
        assertTrue(result, String.format("Test folder '%s' was not created", _storeFile.getAbsolutePath()));
        _updater = mock(PreferenceStoreUpdater.class);
        when(_updater.getLatestVersion()).thenReturn(BrokerModel.MODEL_VERSION);

        final ConfiguredObject<?> parent = mock(ConfiguredObject.class);
        when(parent.getContext()).thenReturn(Collections.emptyMap());
        when(parent.getContextKeys(anyBoolean())).thenReturn(Collections.emptySet());

        _preferenceStore = new BDBPreferenceStore(parent, _storeFile.getPath());

        _testInitialRecords = Arrays.asList(
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.singletonMap("name", "test")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.singletonMap("name", "test1")));
        populateTestData(_testInitialRecords, BrokerModel.MODEL_VERSION);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            if (_preferenceStore != null)
            {
                _preferenceStore.close();
            }
        }
        finally
        {
            if (_storeFile != null)
            {
                FileUtils.delete(_storeFile, true);
            }
        }
    }

    @Test
    public void testVersionAfterUpgrade()
    {
        FileUtils.delete(_storeFile, true);
        _storeFile.mkdirs();
        ModelVersion storeVersion =
                new ModelVersion(BrokerModel.MODEL_MAJOR_VERSION - 1, BrokerModel.MODEL_MINOR_VERSION);
        populateTestData(_testInitialRecords, storeVersion.toString());

        _preferenceStore.openAndLoad(_updater);

        ModelVersion storedVersion = _preferenceStore.getStoredVersion();

        assertEquals(BrokerModel.MODEL_VERSION, storedVersion.toString(), "Unexpected version");
    }

    @Test
    public void testOpenAndLoad()
    {
        Collection<PreferenceRecord> recovered = _preferenceStore.openAndLoad(_updater);
        assertEquals(AbstractBDBPreferenceStore.StoreState.OPENED, _preferenceStore.getStoreState(),
                     "Unexpected store state");

        assertNotNull(_preferenceStore.getEnvironmentFacade(), "Store was not properly opened");
        PreferenceTestHelper.assertRecords(_testInitialRecords, recovered);
    }

    @Test
    public void testClose() throws Exception
    {
        _preferenceStore.openAndLoad(_updater);
        _preferenceStore.close();
        assertEquals(AbstractBDBPreferenceStore.StoreState.CLOSED, _preferenceStore.getStoreState(),
                     "Unexpected store state");
        assertNull(_preferenceStore.getEnvironmentFacade(), "Store was not properly closed");
    }

    @Test
    public void testUpdateOrCreate()
    {
        _preferenceStore.openAndLoad(_updater);

        PreferenceRecord oldRecord = _testInitialRecords.get(0);

        Collection<PreferenceRecord> records = Arrays.asList(
                new PreferenceRecordImpl(oldRecord.getId(), Collections.singletonMap("name", "test2")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.singletonMap("name", "test3")));
        _preferenceStore.updateOrCreate(records);

        _preferenceStore.close();
        Collection<PreferenceRecord> recovered = _preferenceStore.openAndLoad(_updater);
        List<PreferenceRecord> expected = new ArrayList<>(records);
        expected.add(_testInitialRecords.get(1));
        PreferenceTestHelper.assertRecords(expected, recovered);
    }

    @Test
    public void testReplace()
    {
        _preferenceStore.openAndLoad(_updater);

        PreferenceRecord oldRecord1 = _testInitialRecords.get(0);
        PreferenceRecord oldRecord2 = _testInitialRecords.get(1);

        Collection<UUID> recordsToRemove = Collections.singleton(oldRecord1.getId());
        Collection<PreferenceRecord> recordsToAddUpdate = Arrays.asList(
                new PreferenceRecordImpl(oldRecord2.getId(), Collections.singletonMap("name", "test2")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.singletonMap("name", "test3")));
        _preferenceStore.replace(recordsToRemove, recordsToAddUpdate);

        _preferenceStore.close();
        Collection<PreferenceRecord> recovered = _preferenceStore.openAndLoad(_updater);
        PreferenceTestHelper.assertRecords(recordsToAddUpdate, recovered);
    }

    @Test
    public void testUpdateFailIfNotOpened()
    {
        try
        {
            _preferenceStore.updateOrCreate(Collections.emptyList());
            fail("Should not be able to update or create");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testReplaceFailIfNotOpened()
    {
        try
        {
            _preferenceStore.replace(Collections.emptyList(), Collections.emptyList());
            fail("Should not be able to replace");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }


    private void populateTestData(final List<PreferenceRecord> records, final String modelVersion)
    {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        try (Environment environment = new Environment(_storeFile, envConfig))
        {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            try (Database versionDb = environment.openDatabase(null, "USER_PREFERENCES_VERSION", dbConfig);
                 Database preferencesDb = environment.openDatabase(null, "USER_PREFERENCES", dbConfig))
            {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                UUIDTupleBinding keyBinding = UUIDTupleBinding.getInstance();
                MapBinding valueBinding = MapBinding.getInstance();
                for (PreferenceRecord record : records)
                {
                    keyBinding.objectToEntry(record.getId(), key);
                    valueBinding.objectToEntry(record.getAttributes(), value);
                    preferencesDb.put(null, key, value);
                }

                ByteBinding.byteToEntry((byte) 0, value);
                StringBinding.stringToEntry(modelVersion, key);
                versionDb.put(null, key, value);
            }
        }
    }
}
