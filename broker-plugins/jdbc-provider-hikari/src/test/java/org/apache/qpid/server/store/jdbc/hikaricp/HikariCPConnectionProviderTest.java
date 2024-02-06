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
package org.apache.qpid.server.store.jdbc.hikaricp;

import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProvider.DEFAULT_MAXIMUM_POOLSIZE;
import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProvider.DEFAULT_MINIMUM_IDLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class HikariCPConnectionProviderTest extends UnitTestBase
{
    @Test
    public void testCreateHikariCPConfig()
    {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("qpid.jdbcstore.hikaricp.idleTimeout", "123");
        attributes.put("qpid.jdbcstore.hikaricp.connectionTimeout", "1234");
        attributes.put("qpid.jdbcstore.hikaricp.connectionTestQuery", "select 1");

        final FileKeyStore<?> keyStore = mock(FileKeyStore.class);
        when(keyStore.getPath()).thenReturn("/etc/cert/key.p12");
        when(keyStore.getPassword()).thenReturn("12345678");
        final String keyStorePathParam = "sslkey";
        final String keyStorePasswordParam = "sslpassword";
        final FileTrustStore<?> trustStore = mock(FileTrustStore.class);
        when(trustStore.getPath()).thenReturn("/etc/cert/trust.p12");
        when(trustStore.getPassword()).thenReturn("12345678");
        final String trustStorePathParam = "sslcert";
        final String trustStorePasswordParam = "certpassword";

        final String connectionUrl = "jdbc:mariadb://localhost:3306/test";
        final String username = "usr";
        final String password = "pwd";
        final HikariConfig config = HikariCPConnectionProvider.createHikariCPConfig(connectionUrl, username, password,
                keyStore, keyStorePathParam, keyStorePasswordParam, trustStore, trustStorePathParam,
                trustStorePasswordParam, attributes);
        assertEquals(connectionUrl, config.getJdbcUrl());
        assertEquals(username, config.getUsername());
        assertEquals(password, config.getPassword());
        assertEquals(123, config.getIdleTimeout(), "Unexpected idleTimeout");
        assertEquals(1234, config.getConnectionTimeout(), "Unexpected connectionTimeout");
        assertEquals("select 1", config.getConnectionTestQuery(), "Unexpected connectionTestQuery()");
        assertEquals("/etc/cert/key.p12", config.getDataSourceProperties().get("sslkey"), "Unexpected sslkey");
        assertEquals("12345678", config.getDataSourceProperties().get("sslpassword"), "Unexpected sslpassword");
        assertEquals("/etc/cert/trust.p12", config.getDataSourceProperties().get("sslcert"), "Unexpected sslcert");
        assertEquals("12345678", config.getDataSourceProperties().get("certpassword"), "Unexpected certpassword");
        assertEquals(DEFAULT_MAXIMUM_POOLSIZE, config.getMaximumPoolSize(), "Unexpected maximumPoolSize");
        assertEquals(DEFAULT_MINIMUM_IDLE, config.getMinimumIdle(), "Unexpected minimumIdle");
    }
}
