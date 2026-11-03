/*
 *
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
package org.apache.qpid.server.protocol.v1_0.store.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;

import org.apache.qpid.server.protocol.v1_0.store.FlowToDiskMetaDataTestCase;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.jdbc.GenericJDBCMessageStore;
import org.apache.qpid.server.virtualhost.jdbc.JDBCVirtualHost;

class JDBCFlowToDiskMetaDataTest extends FlowToDiskMetaDataTestCase
{
    private String _connectionUrl;
    private GenericJDBCMessageStore _store;

    @Override
    protected MessageStore createMessageStore()
    {
        _connectionUrl = "jdbc:derby:memory:/" + getTestName();
        final JDBCVirtualHost<?> virtualHost = mock(JDBCVirtualHost.class);
        when(virtualHost.getConnectionUrl()).thenReturn(_connectionUrl + ";create=true");
        when(virtualHost.getTableNamePrefix()).thenReturn("TEST_");
        _store = new GenericJDBCMessageStore();
        _store.openMessageStore(virtualHost);
        return _store;
    }

    @AfterEach
    void dropDatabase()
    {
        if (_store != null)
        {
            _store.closeMessageStore();
        }
        if (_connectionUrl != null)
        {
            final SQLException exception = assertThrows(SQLException.class, () ->
                    DriverManager.getConnection(_connectionUrl + ";drop=true"));
            assertEquals("08006", exception.getSQLState());
        }
    }
}
