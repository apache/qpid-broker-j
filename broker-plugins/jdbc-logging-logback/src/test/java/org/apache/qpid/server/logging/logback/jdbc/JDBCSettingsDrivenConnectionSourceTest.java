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

package org.apache.qpid.server.logging.logback.jdbc;

import static org.apache.qpid.server.logging.logback.jdbc.JDBCLoggerHelper.ROOT_LOGGER;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.jdbc.JDBCSettings;

public class JDBCSettingsDrivenConnectionSourceTest extends InMemoryDatabaseTestBase
{
    private JDBCSettingsDrivenConnectionSource _connectionSource;

    @Before
    public void setUp()
    {
        final JDBCSettings jdbcSettings = mock(JDBCSettings.class);
        when(jdbcSettings.getConnectionUrl()).thenReturn(getTestDatabaseUrl());

        final ConfiguredObject<?> object = mock(ConfiguredObject.class);
        _connectionSource = new JDBCSettingsDrivenConnectionSource(object, jdbcSettings);
        _connectionSource.setContext(ROOT_LOGGER.getLoggerContext());
    }

    @After
    public void tearDown()
    {
        if (_connectionSource != null)
        {
            _connectionSource.stop();
        }
    }

    @Test
    public void testStart() throws SQLException
    {
        try
        {
            _connectionSource.getConnection();
            fail("connection should fail for non started ConnectionSource");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }

        _connectionSource.start();

        final Connection connection = _connectionSource.getConnection();
        connection.close();
    }

    @Test
    public void testStop() throws SQLException
    {
        _connectionSource.start();
        _connectionSource.stop();
        try
        {
            _connectionSource.getConnection();
            fail("connection should fail for stopped ConnectionSource");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testGetConnection() throws SQLException
    {
        _connectionSource.start();
        final Connection connection = _connectionSource.getConnection();
        connection.close();
    }
}
