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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.core.db.ConnectionSourceBase;
import ch.qos.logback.core.db.dialect.SQLDialectCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.jdbc.ConnectionProvider;
import org.apache.qpid.server.store.jdbc.JDBCSettings;
import org.apache.qpid.server.store.jdbc.JdbcUtils;

public class JDBCSettingsDrivenConnectionSource extends ConnectionSourceBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCSettingsDrivenConnectionSource.class);

    private final ConfiguredObject<?> _object;
    private final JDBCSettings _jdbcSettings;
    private final AtomicReference<ConnectionProvider> _connectionProvider;

    JDBCSettingsDrivenConnectionSource(final ConfiguredObject<?> object, final JDBCSettings jdbcSettings)
    {
        _object = object;
        _jdbcSettings = jdbcSettings;
        _connectionProvider = new AtomicReference<>();
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        final ConnectionProvider connectionProvider = _connectionProvider.get();
        if (connectionProvider == null)
        {
            throw new IllegalConfigurationException("Connection provider does not exist");
        }
        return connectionProvider.getConnection();
    }

    @Override
    public void start()
    {
        _connectionProvider.getAndUpdate(provider -> provider == null ? create() : provider);
        discoverConnectionProperties();
        if (!supportsGetGeneratedKeys() && getSQLDialectCode() == SQLDialectCode.UNKNOWN_DIALECT)
        {
            addWarn("Connection does not support GetGeneratedKey method and could not discover the dialect.");
        }
        super.start();
    }

    @Override
    public void stop()
    {
        super.stop();

        final ConnectionProvider connectionProvider = _connectionProvider.getAndSet(null);
        if (connectionProvider != null)
        {
            try
            {
                connectionProvider.close();
            }
            catch (SQLException e)
            {
                LOGGER.warn("Unable to close connection provider", e);
            }
        }
    }

    private ConnectionProvider create()
    {
        try
        {
            return JdbcUtils.createConnectionProvider(_object, _jdbcSettings, LOGGER);
        }
        catch (SQLException e)
        {
            throw new IllegalConfigurationException("Cannot create connection provider", e);
        }
    }

    @Override
    public String toString()
    {
        return String.format("JDBCSettingsDrivenConnectionSource{_jdbcSettings=%s}", _jdbcSettings);
    }
}
