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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.AbstractDurableConfigurationStoreTestCase;
import org.apache.qpid.server.store.DurableConfigurationStore;
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

    public void testOnDelete() throws Exception
    {
        try(Connection connection = openConnection())
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

    public void testDeleteAction()
    {
        GenericJDBCConfigurationStore store = (GenericJDBCConfigurationStore) getConfigurationStore();
        AtomicBoolean deleted = new AtomicBoolean();
        store.addDeleteAction(object -> deleted.set(true));

        store.closeConfigurationStore();
        store.onDelete(getVirtualHostNode());
        assertEquals("Delete action was not invoked", true, deleted.get());
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
