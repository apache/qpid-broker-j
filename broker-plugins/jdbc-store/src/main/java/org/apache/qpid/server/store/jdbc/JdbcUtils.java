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

package org.apache.qpid.server.store.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.StoreException;

public class JdbcUtils
{
    public static void closeConnection(final Connection conn, final Logger logger)
    {
        if(conn != null)
        {
            try
            {
                conn.close();
            }
            catch (SQLException e)
            {
                logger.error("Problem closing connection", e);
            }
        }
    }

    public static void closePreparedStatement(final PreparedStatement stmt, final Logger logger)
    {
        if (stmt != null)
        {
            try
            {
                stmt.close();
            }
            catch(SQLException e)
            {
                logger.error("Problem closing prepared statement", e);
            }
        }
    }

    public static boolean tableExists(final String tableName, final Connection conn) throws SQLException
    {
        DatabaseMetaData metaData = conn.getMetaData();
        // Some databases are not case sensitive in their table names and/or report back a different case for the
        // name of the table than the one originally used to create it
        return tableExistsCase(tableName.toUpperCase(), metaData) || tableExistsCase(tableName.toLowerCase(), metaData)
               || (!tableName.equals(tableName.toUpperCase())
                   && !tableName.equals(tableName.toLowerCase())
                   && tableExistsCase(tableName, metaData));

    }

    public static ConnectionProvider createConnectionProvider(final ConfiguredObject<?> parent,
                                                              final JDBCSettings settings,
                                                              final Logger logger)
            throws SQLException
    {
        String connectionPoolType = settings.getConnectionPoolType() == null
                ? DefaultConnectionProviderFactory.TYPE
                : settings.getConnectionPoolType();

        JDBCConnectionProviderFactory connectionProviderFactory =
                JDBCConnectionProviderFactory.FACTORIES.get(connectionPoolType);
        if (connectionProviderFactory == null)
        {
            logger.warn("Unknown connection pool type: {}.  No connection pooling will be used", connectionPoolType);
            connectionProviderFactory = new DefaultConnectionProviderFactory();
        }

        Map<String, String> providerAttributes = new HashMap<>();
        Set<String> providerAttributeNames = new HashSet<>(connectionProviderFactory.getProviderAttributeNames());
        providerAttributeNames.retainAll(parent.getContextKeys(false));
        for (String attr : providerAttributeNames)
        {
            providerAttributes.put(attr, parent.getContextValue(String.class, attr));
        }

        return connectionProviderFactory.getConnectionProvider(settings.getConnectionUrl(),
                                                               settings.getUsername(),
                                                               settings.getPassword(),
                                                               providerAttributes);
    }

    static ConnectionProvider createConnectionProvider(final ConfiguredObject<?> parent, final Logger logger)
    {
        JDBCSettings settings = (JDBCSettings) parent;
        try
        {
            return createConnectionProvider(parent, settings, logger);
        }
        catch (SQLException e)
        {
            throw new StoreException(String.format(
                    "Failed to create connection provider for connectionUrl: '%s' and username: '%s'",
                    settings.getConnectionUrl(),
                    settings.getUsername()), e);
        }
    }

    public static void dropTables(final Connection connection, final Logger logger, Collection<String> tableNames)
    {
        for (String tableName : tableNames)
        {
            try(Statement statement = connection.createStatement())
            {
                statement.execute(String.format("DROP TABLE %s",  tableName));
            }
            catch(SQLException e)
            {
                logger.warn("Failed to drop table '" + tableName + "' :" + e);
            }
        }
    }
    private static boolean tableExistsCase(final String tableName, final DatabaseMetaData metaData) throws SQLException
    {
        try (ResultSet rs = metaData.getTables(null, null, tableName, null))
        {
            return rs.next();
        }
    }
}
