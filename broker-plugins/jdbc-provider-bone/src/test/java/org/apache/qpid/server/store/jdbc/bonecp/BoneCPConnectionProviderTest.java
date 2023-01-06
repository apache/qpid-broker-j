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
package org.apache.qpid.server.store.jdbc.bonecp;

import static org.apache.qpid.server.store.jdbc.bonecp.BoneCPConnectionProvider.DEFAULT_MAX_CONNECTIONS_PER_PARTITION;
import static org.apache.qpid.server.store.jdbc.bonecp.BoneCPConnectionProvider.DEFAULT_MIN_CONNECTIONS_PER_PARTITION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.jolbox.bonecp.BoneCPConfig;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class BoneCPConnectionProviderTest extends UnitTestBase
{
    @Test
    public void testCreateBoneCPConfig()
    {

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("qpid.jdbcstore.bonecp.idleMaxAgeInMinutes", "123");
        attributes.put("qpid.jdbcstore.bonecp.connectionTimeoutInMs", "1234");
        attributes.put("qpid.jdbcstore.bonecp.connectionTestStatement", "select 1");
        attributes.put("qpid.jdbcstore.bonecp.logStatementsEnabled", "true");
        attributes.put("qpid.jdbcstore.bonecp.partitionCount", "12");

        String connectionUrl = "jdbc:mariadb://localhost:3306/test";
        String username = "usr";
        String password = "pwd";
        BoneCPConfig config =
                BoneCPConnectionProvider.createBoneCPConfig(connectionUrl, username, password, attributes);
        assertEquals(connectionUrl, config.getJdbcUrl());
        assertEquals(username, config.getUsername());
        assertEquals(password, config.getPassword());
        assertEquals(123, config.getIdleMaxAgeInMinutes(), "Unexpected idleMaxAgeInMinutes");
        assertEquals(1234, config.getConnectionTimeoutInMs(), "Unexpected connectionTimeout");
        assertEquals("select 1", config.getConnectionTestStatement(), "Unexpected connectionTestStatement");
        assertTrue(config.isLogStatementsEnabled(), "Unexpected logStatementsEnabled");
        assertEquals(DEFAULT_MAX_CONNECTIONS_PER_PARTITION, config.getMaxConnectionsPerPartition(),
                "Unexpected maxConnectionsPerPartition");
        assertEquals(DEFAULT_MIN_CONNECTIONS_PER_PARTITION, config.getMinConnectionsPerPartition(),
                "Unexpected minConnectionsPerPartition");
        assertEquals(12, config.getPartitionCount(), "Unexpected partitionCount");
    }
}
