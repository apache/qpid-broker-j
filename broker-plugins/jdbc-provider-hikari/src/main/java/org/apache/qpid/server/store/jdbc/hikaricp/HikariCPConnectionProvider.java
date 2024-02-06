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
import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.MAXIMUM_POOLSIZE;
import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.MINIMUM_IDLE;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.store.jdbc.ConnectionProvider;

public class HikariCPConnectionProvider implements ConnectionProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HikariCPConnectionProvider.class);
    private static final String ADDING_DATASOURCE_PROPERTY = "Adding dataSource property '{}' with value '{}'";

    public static final int DEFAULT_MINIMUM_IDLE = 20;
    public static final int DEFAULT_MAXIMUM_POOLSIZE = 40;

    private final HikariDataSource _dataSource;

    public HikariCPConnectionProvider(final String connectionUrl,
                                      final String username,
                                      final String password,
                                      final KeyStore<?> keyStore,
                                      final String keyStorePathPropertyName,
                                      final String keyStorePasswordPropertyName,
                                      final TrustStore<?> trustStore,
                                      final String trustStorePathPropertyName,
                                      final String trustStorePasswordPropertyName,
                                      final Map<String, String> providerAttributes)
    {
        final HikariConfig config = createHikariCPConfig(connectionUrl, username, password, keyStore, keyStorePathPropertyName,
                keyStorePasswordPropertyName, trustStore, trustStorePathPropertyName, trustStorePasswordPropertyName, providerAttributes);
        _dataSource = new HikariDataSource(config);
    }

    static HikariConfig createHikariCPConfig(final String connectionUrl,
                                             final String username,
                                             final String password,
                                             final KeyStore<?> keyStore,
                                             final String keyStorePathPropertyName,
                                             final String keyStorePasswordPropertyName,
                                             final TrustStore<?> trustStore,
                                             final String trustStorePathPropertyName,
                                             final String trustStorePasswordPropertyName,
                                             final Map<String, String> providerAttributes)
    {
        final Map<String, String> attributes = new HashMap<>(providerAttributes);
        attributes.putIfAbsent(MINIMUM_IDLE, String.valueOf(DEFAULT_MINIMUM_IDLE));
        attributes.putIfAbsent(MAXIMUM_POOLSIZE, String.valueOf(DEFAULT_MAXIMUM_POOLSIZE));

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
            }
            if (password != null)
            {
                config.setPassword(password);
            }
            if (keyStore instanceof FileKeyStore)
            {
                if (keyStorePathPropertyName != null)
                {
                    final String path = ((FileKeyStore) keyStore).getPath();
                    LOGGER.debug(ADDING_DATASOURCE_PROPERTY, keyStorePathPropertyName, path);
                    config.addDataSourceProperty(keyStorePathPropertyName, path);
                }
                if (keyStorePasswordPropertyName != null)
                {
                    final String pwd = ((FileKeyStore) keyStore).getPassword() == null ? "null" : "******";
                    LOGGER.debug(ADDING_DATASOURCE_PROPERTY, keyStorePasswordPropertyName, pwd);
                    config.addDataSourceProperty(keyStorePasswordPropertyName, ((FileKeyStore) keyStore).getPassword());
                }
            }
            if (trustStore instanceof FileTrustStore)
            {
                if (trustStorePathPropertyName != null)
                {
                    final String path = ((FileTrustStore) trustStore).getPath();
                    LOGGER.debug(ADDING_DATASOURCE_PROPERTY, trustStorePathPropertyName, path);
                    config.addDataSourceProperty(trustStorePathPropertyName, ((FileTrustStore) trustStore).getPath());
                }
                if (trustStorePasswordPropertyName != null)
                {
                    final String pwd = ((FileTrustStore) trustStore).getPassword() == null ? "null" : "******";
                    LOGGER.debug(ADDING_DATASOURCE_PROPERTY, trustStorePasswordPropertyName, pwd);
                    config.addDataSourceProperty(trustStorePasswordPropertyName, ((FileTrustStore) trustStore).getPassword());
                }
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
