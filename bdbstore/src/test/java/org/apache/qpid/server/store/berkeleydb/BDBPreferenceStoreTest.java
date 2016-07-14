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


import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;


import com.sleepycat.bind.tuple.ByteBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.preferences.PreferenceTestHelper;
import org.apache.qpid.server.store.berkeleydb.tuple.MapBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.UUIDTupleBinding;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.util.FileUtils;

public class BDBPreferenceStoreTest extends QpidTestCase
{
    private File _storeFile;
    private PreferenceStoreUpdater _updater;
    private BDBPreferenceStore _preferenceStore;
    private List<PreferenceRecord> _testInitialRecords;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _storeFile = new File(TMP_FOLDER, getTestName() + System.currentTimeMillis() + ".preferences.bdb");
        boolean result = _storeFile.mkdirs();
        assertTrue(String.format("Test folder '%s' was not created", _storeFile.getAbsolutePath()), result);
        _updater = mock(PreferenceStoreUpdater.class);
        when(_updater.getLatestVersion()).thenReturn(BrokerModel.MODEL_VERSION);

        final ConfiguredObject<?> parent = mock(ConfiguredObject.class);
        when(parent.getContext()).thenReturn(Collections.<String, String>emptyMap());
        when(parent.getContextKeys(anyBoolean())).thenReturn(Collections.<String>emptySet());

        _preferenceStore = new BDBPreferenceStore(parent, _storeFile.getPath());

        _testInitialRecords = Arrays.<PreferenceRecord>asList(
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "test")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "test1")));
        populateTestData(_testInitialRecords);
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _preferenceStore.close();
            FileUtils.delete(_storeFile, true);
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testOpenAndLoad() throws Exception
    {
        Collection<PreferenceRecord> recovered = _preferenceStore.openAndLoad(_updater);
        assertEquals("Unexpected store state",
                     AbstractBDBPreferenceStore.StoreState.OPENED,
                     _preferenceStore.getStoreState());
        assertNotNull("Store was not properly opened", _preferenceStore.getEnvironmentFacade());
        PreferenceTestHelper.assertRecords(_testInitialRecords, recovered);
    }

    public void testClose() throws Exception
    {
        _preferenceStore.openAndLoad(_updater);
        _preferenceStore.close();
        assertEquals("Unexpected store state",
                     AbstractBDBPreferenceStore.StoreState.CLOSED,
                     _preferenceStore.getStoreState());
        assertNull("Store was not properly closed", _preferenceStore.getEnvironmentFacade());
    }

    public void testUpdateOrCreate() throws Exception
    {
        _preferenceStore.openAndLoad(_updater);

        PreferenceRecord oldRecord = _testInitialRecords.get(0);

        Collection<PreferenceRecord> records = Arrays.<PreferenceRecord>asList(
                new PreferenceRecordImpl(oldRecord.getId(), Collections.<String, Object>singletonMap("name", "test2")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "test3")));
        _preferenceStore.updateOrCreate(records);

        _preferenceStore.close();
        Collection<PreferenceRecord> recovered = _preferenceStore.openAndLoad(_updater);
        List<PreferenceRecord> expected = new ArrayList<>(records);
        expected.add(_testInitialRecords.get(1));
        PreferenceTestHelper.assertRecords(expected, recovered);
    }

    public void testReplace() throws Exception
    {
        _preferenceStore.openAndLoad(_updater);

        PreferenceRecord oldRecord1 = _testInitialRecords.get(0);
        PreferenceRecord oldRecord2 = _testInitialRecords.get(1);

        Collection<UUID> recordsToRemove = Collections.singleton(oldRecord1.getId());
        Collection<PreferenceRecord> recordsToAddUpdate = Arrays.<PreferenceRecord>asList(
                new PreferenceRecordImpl(oldRecord2.getId(), Collections.<String, Object>singletonMap("name", "test2")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "test3")));
        _preferenceStore.replace(recordsToRemove, recordsToAddUpdate);

        _preferenceStore.close();
        Collection<PreferenceRecord> recovered = _preferenceStore.openAndLoad(_updater);
        PreferenceTestHelper.assertRecords(recordsToAddUpdate, recovered);
    }

    public void testUpdateFailIfNotOpened() throws Exception
    {
        try
        {
            _preferenceStore.updateOrCreate(Collections.<PreferenceRecord>emptyList());
            fail("Should not be able to update or create");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    public void testReplaceFailIfNotOpened() throws Exception
    {
        try
        {
            _preferenceStore.replace(Collections.<UUID>emptyList(), Collections.<PreferenceRecord>emptyList());
            fail("Should not be able to replace");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }


    private void populateTestData(final List<PreferenceRecord> records)
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
                StringBinding.stringToEntry(BrokerModel.MODEL_VERSION, key);
                versionDb.put(null, key, value);
            }
        }
    }
}
