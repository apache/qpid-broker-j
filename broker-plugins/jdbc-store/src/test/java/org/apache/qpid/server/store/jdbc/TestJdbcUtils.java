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
package org.apache.qpid.server.store.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TestJdbcUtils
{

    static void assertTablesExistence(Collection<String> expectedTables,
                                      Collection<String> actualTables,
                                      boolean exists) throws SQLException
    {
        for (String tableName : expectedTables)
        {
            assertEquals(String.format("Table %s %s", tableName, (exists ? " is not found" : " actually exist")),
                         exists,
                         actualTables.contains(tableName));
        }
    }

    static Set<String> getTableNames(Connection connection) throws SQLException
    {
        Set<String> tableNames = new HashSet<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"}))
        {
            while (tables.next())
            {
                tableNames.add(tables.getString("TABLE_NAME"));
            }
        }
        return tableNames;
    }

    public static void shutdownDerby(String connectionURL) throws SQLException
    {
        Connection connection = null;
        try
        {
            connection = DriverManager.getConnection(connectionURL + ";shutdown=true");
        }
        catch(SQLException e)
        {
            if (e.getSQLState().equalsIgnoreCase("08006"))
            {
                //expected and represents a clean shutdown of this database only, do nothing.
            }
            else
            {
                throw e;
            }
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    static Connection openConnection(final String connectionURL) throws SQLException
    {
        return DriverManager.getConnection(connectionURL);
    }
}
