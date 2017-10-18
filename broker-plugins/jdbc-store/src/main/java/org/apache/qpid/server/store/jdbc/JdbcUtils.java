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

import org.slf4j.Logger;

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
               || tableName.equals(tableName.toUpperCase()) || tableName.equals(tableName.toLowerCase())
               || tableExistsCase(tableName, metaData);

    }

    private static boolean tableExistsCase(final String tableName, final DatabaseMetaData metaData) throws SQLException
    {
        try (ResultSet rs = metaData.getTables(null, null, tableName, null))
        {
            return rs.next();
        }
    }
}
