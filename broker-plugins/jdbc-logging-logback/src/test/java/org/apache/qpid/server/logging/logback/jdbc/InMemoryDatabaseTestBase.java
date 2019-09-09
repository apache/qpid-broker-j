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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;

import org.apache.qpid.test.utils.UnitTestBase;

public class InMemoryDatabaseTestBase extends UnitTestBase
{
    private Connection _setupConnection;

    @Before
    public void startUpDatabase() throws SQLException
    {
        _setupConnection = DriverManager.getConnection(getTestDatabaseUrl());
    }

    @After
    public void shutDownDatabase() throws SQLException
    {
        try (final Statement statement = _setupConnection.createStatement())
        {
            statement.execute("DROP ALL OBJECTS");
        }
        finally
        {
            _setupConnection.close();
        }
    }

    String getTestDatabaseUrl()
    {
        return String.format("jdbc:h2:mem:%s", getTestName());
    }

    protected Connection getConnection()
    {
        return _setupConnection;
    }
}
