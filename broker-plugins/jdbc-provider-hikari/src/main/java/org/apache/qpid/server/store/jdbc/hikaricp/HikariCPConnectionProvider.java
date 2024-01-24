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

import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.JDBC_STORE_PREFIX;
import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.MAX_POOLSIZE;
import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.MIN_IDLE;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.store.jdbc.ConnectionProvider;

public class HikariCPConnectionProvider implements ConnectionProvider
{
    public static final int DEFAULT_MIN_IDLE = 20;
    public static final int DEFAULT_MAX_POOLSIZE = 40;

    private final HikariDataSource _dataSource;

    public HikariCPConnectionProvider(final String connectionUrl,
                                      final String username,
                                      final String password,
                                      final Map<String, String> providerAttributes)
    {
        final HikariConfig config = createHikariCPConfig(connectionUrl, username, password, providerAttributes);
        _dataSource = new HikariDataSource(config);
    }

    static HikariConfig createHikariCPConfig(final String connectionUrl,
                                             final String username,
                                             final String password,
                                             final Map<String, String> providerAttributes)
    {
        final Map<String, String> attributes = new HashMap<>(providerAttributes);
        attributes.putIfAbsent(MIN_IDLE, String.valueOf(DEFAULT_MIN_IDLE));
        attributes.putIfAbsent(MAX_POOLSIZE, String.valueOf(DEFAULT_MAX_POOLSIZE));

        final int prefixLength = (JDBC_STORE_PREFIX + "hikaricp.").length();
        final Map<String, String> propertiesMap = attributes.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().substring(prefixLength), Map.Entry::getValue));

        final Properties properties = new Properties();
        properties.putAll(propertiesMap);

        try
        {
            final HikariConfig config = new HikariConfig(properties);
            config.setJdbcUrl(connectionUrl);
            if (username != null)
            {
                config.setUsername(username);
                config.setPassword(password);
            }
            return config;
        }
        catch (Exception e)
        {
            throw new IllegalConfigurationException("Unexpected exception on applying HikariCP configuration", e);
        }

    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return _dataSource.getConnection();
    }

    @Override
    public void close()
    {
        _dataSource.close();
    }
}
