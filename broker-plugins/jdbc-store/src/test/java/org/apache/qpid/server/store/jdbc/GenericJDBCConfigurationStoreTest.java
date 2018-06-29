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

package org.apache.qpid.server.store.jdbc;


import static org.apache.qpid.server.store.jdbc.TestJdbcUtils.assertTablesExistence;
import static org.apache.qpid.server.store.jdbc.TestJdbcUtils.getTableNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.AbstractDurableConfigurationStoreTestCase;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.virtualhostnode.jdbc.JDBCVirtualHostNode;

public class GenericJDBCConfigurationStoreTest extends AbstractDurableConfigurationStoreTestCase
{

    private String _connectionURL;

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
        {
            if (_connectionURL != null)
            {
                TestJdbcUtils.shutdownDerby(_connectionURL);
            }
        }
    }

    @Test
    public void testOnDelete() throws Exception
    {
        try (Connection connection = openConnection())
        {
            GenericJDBCConfigurationStore store = (GenericJDBCConfigurationStore) getConfigurationStore();
            Collection<String> expectedTables = Arrays.asList(store.getConfiguredObjectHierarchyTableName(),
                                                              store.getConfiguredObjectsTableName());
            assertTablesExistence(expectedTables, getTableNames(connection), true);
            store.closeConfigurationStore();
            assertTablesExistence(expectedTables, getTableNames(connection), true);
            store.onDelete(getVirtualHostNode());
            assertTablesExistence(expectedTables, getTableNames(connection), false);
        }
    }

    @Test
    public void testDeleteAction()
    {
        GenericJDBCConfigurationStore store = (GenericJDBCConfigurationStore) getConfigurationStore();
        AtomicBoolean deleted = new AtomicBoolean();
        store.addDeleteAction(object -> deleted.set(true));

        store.closeConfigurationStore();
        store.onDelete(getVirtualHostNode());
        assertEquals("Delete action was not invoked", true, deleted.get());
    }

    @Test
    public void testUpgradeStoreStructure() throws Exception
    {
        GenericJDBCConfigurationStore store = (GenericJDBCConfigurationStore) getConfigurationStore();
        store.closeConfigurationStore();
        store.onDelete(getVirtualHostNode());

        GenericJDBCConfigurationStore store2 = (GenericJDBCConfigurationStore) createConfigStore();

        UUID hostId = UUID.randomUUID();
        UUID queueId = UUID.randomUUID();
        try (Connection connection = openConnection())
        {
            assertTablesExistence(Arrays.asList("QPID_CONFIGURED_OBJECTS", "QPID_CONFIGURED_OBJECT_HIERARCHY"),
                                  getTableNames(connection), false);
            try (Statement stmt = connection.createStatement())
            {
                stmt.execute("CREATE TABLE QPID_CONFIGURED_OBJECTS ( id VARCHAR(36) not null,"
                             + " object_type varchar(255), attributes blob,  PRIMARY KEY (id))");
                stmt.execute("CREATE TABLE QPID_CONFIGURED_OBJECT_HIERARCHY ( child_id VARCHAR(36) not null,"
                             + " parent_type varchar(255), parent_id VARCHAR(36),  PRIMARY KEY (child_id, parent_type))");
            }

            try (PreparedStatement insertStmt = connection.prepareStatement(
                    "INSERT INTO QPID_CONFIGURED_OBJECTS ( id, object_type, attributes) VALUES (?,?,?)"))
            {
                insertStmt.setString(1, hostId.toString());
                insertStmt.setString(2, "VirtualHost");
                final byte[] attributesAsBytes = "{\"name\":\"testHost\"}".getBytes(StandardCharsets.UTF_8);
                try (ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes))
                {
                    insertStmt.setBinaryStream(3, bis, attributesAsBytes.length);
                }
                insertStmt.execute();
            }
            try (PreparedStatement insertStmt = connection.prepareStatement(
                    "INSERT INTO QPID_CONFIGURED_OBJECTS ( id, object_type, attributes) VALUES (?,?,?)"))
            {
                insertStmt.setString(1, queueId.toString());
                insertStmt.setString(2, "Queue");
                final byte[] attributesAsBytes = "{\"name\":\"testQueue\"}".getBytes(StandardCharsets.UTF_8);
                try (ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes))
                {
                    insertStmt.setBinaryStream(3, bis, attributesAsBytes.length);
                }
                insertStmt.execute();
            }

            try (PreparedStatement insertStmt = connection.prepareStatement(
                    "INSERT INTO QPID_CONFIGURED_OBJECT_HIERARCHY "
                    + " ( child_id, parent_type, parent_id) VALUES (?,?,?)"))
            {
                insertStmt.setString(1, queueId.toString());
                insertStmt.setString(2, "VirtualHost");
                insertStmt.setString(3, hostId.toString());

                insertStmt.execute();
            }
        }

        store2.init(getVirtualHostNode());
        store2.upgradeStoreStructure();

        try (Connection connection = openConnection())
        {
            try
            {
                assertTablesExistence(Arrays.asList("QPID_CONFIGURED_OBJECTS", "QPID_CONFIGURED_OBJECT_HIERARCHY"),
                                      getTableNames(connection), false);

                assertTablesExistence(Collections.singletonList("QPID_CFG_VERSION"), getTableNames(connection), true);
            }
            finally
            {
                JdbcUtils.dropTables(connection,
                                     store2.getLogger(),
                                     Arrays.asList("QPID_CONFIGURED_OBJECTS", "QPID_CONFIGURED_OBJECT_HIERARCHY"));
            }
        }

        ConfiguredObjectRecordHandler handler = mock(ConfiguredObjectRecordHandler.class);
        store2.openConfigurationStore(handler);

        verify(handler).handle(matchesRecord(hostId, "VirtualHost", map("name", "testHost")));
        verify(handler).handle(matchesRecord(queueId,
                                             "Queue",
                                             map("name", "testQueue"),
                                             Collections.singletonMap("VirtualHost", hostId)));
    }

    @Test
    public void testUpgradeStoreStructureFromUnknownVersion() throws Exception
    {
        GenericJDBCConfigurationStore store = (GenericJDBCConfigurationStore) getConfigurationStore();
        store.closeConfigurationStore();
        store.onDelete(getVirtualHostNode());

        GenericJDBCConfigurationStore store2 = (GenericJDBCConfigurationStore) createConfigStore();

        try (Connection connection = openConnection())
        {
            assertTablesExistence(Collections.singletonList("QPID_CFG_VERSION"),
                                  getTableNames(connection), false);
            try (Statement stmt = connection.createStatement())
            {
                stmt.execute("CREATE TABLE QPID_CFG_VERSION ( version int not null )");
            }
            try (PreparedStatement insertStmt = connection.prepareStatement(
                    "INSERT INTO QPID_CFG_VERSION ( version) VALUES (?)"))
            {
                insertStmt.setInt(1, 0);
                insertStmt.execute();
            }
        }

        store2.init(getVirtualHostNode());

        try
        {
            store2.upgradeStoreStructure();
            fail("Exception is expected");
        }
        catch (StoreException e)
        {
            // pass
        }
        finally
        {
            try (Connection connection = openConnection())
            {
                JdbcUtils.dropTables(connection,
                                     store2.getLogger(),
                                     Arrays.asList("QPID_CFG_VERSION"));
            }
        }
    }

    @Override
    protected VirtualHostNode createVirtualHostNode(final String storeLocation, final ConfiguredObjectFactory factory)
    {
        _connectionURL = "jdbc:derby:memory:/" + getTestName();
        final JDBCVirtualHostNode parent = mock(JDBCVirtualHostNode.class);
        when(parent.getConnectionUrl()).thenReturn(_connectionURL + ";create=true");
        return parent;
    }

    @Override
    protected DurableConfigurationStore createConfigStore() throws Exception
    {
        return new GenericJDBCConfigurationStore(VirtualHost.class);
    }

    private Connection openConnection() throws SQLException
    {
        return TestJdbcUtils.openConnection(_connectionURL);
    }
}
