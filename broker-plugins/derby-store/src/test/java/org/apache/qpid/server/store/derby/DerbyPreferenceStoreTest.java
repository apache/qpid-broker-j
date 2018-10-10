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

package org.apache.qpid.server.store.derby;

import static org.apache.qpid.server.model.preferences.PreferenceTestHelper.assertRecords;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.store.jdbc.AbstractJDBCPreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class DerbyPreferenceStoreTest extends UnitTestBase
{
    private PreferenceStoreUpdater _updater;
    private DerbyTestPreferenceStore _preferenceStore;
    private List<PreferenceRecord> _testRecords;
    private String _connectionUrl;
    private Connection _testConnection;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.DERBY)));

        _updater = mock(PreferenceStoreUpdater.class);
        when(_updater.getLatestVersion()).thenReturn(BrokerModel.MODEL_VERSION);

        final ConfiguredObject<?> parent = mock(ConfiguredObject.class);
        when(parent.getContext()).thenReturn(Collections.<String, String>emptyMap());
        when(parent.getContextKeys(anyBoolean())).thenReturn(Collections.<String>emptySet());

        _connectionUrl = DerbyUtils.createConnectionUrl(getTestName(), "memory:");
        _preferenceStore = new DerbyTestPreferenceStore(_connectionUrl);

        _testRecords = Arrays.<PreferenceRecord>asList(
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "test")),
                new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "test1")));
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_testConnection != null)
            {
                _testConnection.close();
            }
            if (_preferenceStore != null)
            {
                _preferenceStore.close();
            }
        }
        finally
        {
            if (_connectionUrl != null)
            {
                DerbyUtils.shutdownDatabase(_connectionUrl);
            }
        }
    }

    @Test
    public void testVersionAfterUpgrade() throws Exception
    {
        ModelVersion storeVersion =
                new ModelVersion(BrokerModel.MODEL_MAJOR_VERSION - 1, BrokerModel.MODEL_MINOR_VERSION);

        _testConnection = DriverManager.getConnection(_connectionUrl);
        try (Statement stmt = _testConnection.createStatement())
        {
            stmt.execute("CREATE TABLE PREFERENCES_VERSION ( version VARCHAR(20) NOT NULL )");
        }

        try (PreparedStatement pstmt = _testConnection.prepareStatement("INSERT INTO PREFERENCES_VERSION ( version ) VALUES ( ? )"))
        {
            pstmt.setString(1, storeVersion.toString());
            pstmt.execute();
        }

        _preferenceStore.openAndLoad(_updater);

        ModelVersion storedVersion = _preferenceStore.getPreferencesVersion();

        assertEquals("Unexpected version", BrokerModel.MODEL_VERSION, storedVersion.toString());
    }

    @Test
    public void testOpenAndLoadEmptyStore() throws Exception
    {
        Collection<PreferenceRecord> records = _preferenceStore.openAndLoad(_updater);
        assertEquals("Unexpected number of records", (long) 0, (long) records.size());

        _testConnection = DriverManager.getConnection(_connectionUrl);

        DerbyUtils.tableExists("PREFERENCES", _testConnection);
        DerbyUtils.tableExists("PREFERENCES_VERSION", _testConnection);

        List<String> versions = new ArrayList<>();
        try (PreparedStatement selectStatement = _testConnection.prepareStatement(
                "select version from PREFERENCES_VERSION"))
        {
            try (ResultSet resultSet = selectStatement.executeQuery())
            {
                while (resultSet.next())
                {
                    versions.add(resultSet.getString(1));
                }
            }
        }

        assertEquals("Unexpected versions size", (long) 1, (long) versions.size());
        assertEquals("Unexpected version", BrokerModel.MODEL_VERSION, versions.get(0));
    }

    @Test
    public void testOpenAndLoadNonEmptyStore() throws Exception
    {
        populateTestData();
        Collection<PreferenceRecord> records = _preferenceStore.openAndLoad(_updater);

        assertRecords(_testRecords, records);
    }

    @Test
    public void testClose() throws Exception
    {
        _preferenceStore.openAndLoad(_updater);
        _preferenceStore.close();

        try
        {
            _preferenceStore.updateOrCreate(_testRecords);
            fail("Business operation on closed store should fail");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testUpdateOrCreate() throws Exception
    {
        _preferenceStore.openAndLoad(_updater);
        _preferenceStore.updateOrCreate(_testRecords);

        _testConnection = DriverManager.getConnection(_connectionUrl);
        List<PreferenceRecord> records = getPreferenceRecords();

        assertRecords(_testRecords, records);
    }

    @Test
    public void testReplace() throws Exception
    {
        populateTestData();
        _preferenceStore.openAndLoad(_updater);

        Collection<PreferenceRecord> testRecords = new ArrayList<>();
        testRecords.add(new PreferenceRecordImpl(UUID.randomUUID(), Collections.<String, Object>singletonMap("name", "newOne")));

        _preferenceStore.replace(Collections.singleton(_testRecords.get(0).getId()), testRecords);

        testRecords.add(_testRecords.get(1));

        _testConnection = DriverManager.getConnection(_connectionUrl);
        List<PreferenceRecord> records = getPreferenceRecords();

        assertRecords(testRecords, records);
    }

    @Test
    public void testUpdateFailIfNotOpened() throws Exception
    {
        populateTestData();
        try
        {
            _preferenceStore.updateOrCreate(_testRecords);
            fail("Business operation on not opened store should fail");
        }
        catch (IllegalStateException e)
        {
            e.printStackTrace();
            // pass
        }
    }

    @Test
    public void testReplaceFailIfNotOpened() throws Exception
    {
        populateTestData();
        try
        {
            _preferenceStore.replace(Collections.<UUID>emptyList(), _testRecords);
            fail("Business operation on not opened store should fail");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }


    private void populateTestData()
    {
        DerbyTestPreferenceStore store = new DerbyTestPreferenceStore(_connectionUrl);
        try
        {
            store.openAndLoad(_updater);
            store.updateOrCreate(_testRecords);
        }
        finally
        {
            store.close();
        }
    }

    private List<PreferenceRecord> getPreferenceRecords() throws SQLException, java.io.IOException
    {
        List<PreferenceRecord> records = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        try (PreparedStatement selectStatement = _testConnection.prepareStatement(
                "select id,attributes from PREFERENCES"))
        {
            try (ResultSet resultSet = selectStatement.executeQuery())
            {
                while (resultSet.next())
                {
                    records.add(new PreferenceRecordImpl(
                            UUID.fromString(resultSet.getString(1)),
                            objectMapper.readValue(DerbyUtils.getBlobAsString(resultSet, 2), Map.class)));
                }
            }
        }
        return records;
    }

    private static class DerbyTestPreferenceStore extends AbstractJDBCPreferenceStore
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(DerbyTestPreferenceStore.class);
        private final String _connectionURL;


        public DerbyTestPreferenceStore(final String connectionUrl)
        {
            _connectionURL = connectionUrl;
            DerbyUtils.loadDerbyDriver();
        }

        @Override
        protected Logger getLogger()
        {
            return LOGGER;
        }

        @Override
        protected Connection getConnection() throws SQLException
        {
            return DriverManager.getConnection(_connectionURL);
        }

        @Override
        protected String getSqlBlobType()
        {
            return "blob";
        }

        @Override
        protected String getBlobAsString(final ResultSet rs, final int col) throws SQLException
        {
            return DerbyUtils.getBlobAsString(rs, col);
        }

        @Override
        protected void doDelete()
        {
            // noop
        }

        @Override
        protected void doClose()
        {
            // noop
        }

        ModelVersion getPreferencesVersion() throws SQLException
        {
            try(Connection connection = getConnection())
            {
                return super.getPreferencesVersion(connection);
            }
        }
    }
}
