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
 *
 */

package org.apache.qpid.server.protocol.v1_0.store.jdbc;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreTestCase;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.JDBCDetails;

public class JDBCLinkStoreTest extends LinkStoreTestCase
{
    @Override
    protected LinkStore createLinkStore()
    {
        final JDBCDetails details = mock(JDBCDetails.class);
        when(details.getBlobType()).thenReturn("blob");
        when(details.getTimestampType()).thenReturn("timestamp");
        when(details.isUseBytesMethodsForBlob()).thenReturn(false);

        JDBCContainer jdbcContainer = mock(JDBCContainer.class);
        when(jdbcContainer.getJDBCDetails()).thenReturn(details);
        when(jdbcContainer.getTableNamePrefix()).thenReturn("testTablePrefix");
        when(jdbcContainer.getConnection()).thenAnswer(invocation -> DriverManager.getConnection(getUrl() + ";create=true"));

        return new JDBCLinkStore(jdbcContainer);
    }

    @Override
    protected void deleteLinkStore()
    {
        Connection connection = null;
        try
        {
            connection = DriverManager.getConnection(getUrl());
        }
        catch (SQLException e)
        {
            if (e.getSQLState().equalsIgnoreCase("08006"))
            {
                //expected and represents a clean shutdown of this database only, do nothing.
            }
            else
            {
                throw new RuntimeException(e);
            }
        }
        finally
        {
            if (connection != null)
            {
                try
                {
                    connection.close();
                }
                catch (SQLException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private String getUrl()
    {
        return String.format("jdbc:derby:memory:/%s", getTestName());
    }
}
