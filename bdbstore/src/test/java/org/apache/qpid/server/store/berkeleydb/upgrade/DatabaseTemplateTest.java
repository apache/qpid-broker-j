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
package org.apache.qpid.server.store.berkeleydb.upgrade;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class DatabaseTemplateTest extends UnitTestBase
{
    private static final String SOURCE_DATABASE = "sourceDatabase";
    private Environment _environment;
    private Database _sourceDatabase;

    @Before
    public void setUp() throws Exception
    {
        _environment = mock(Environment.class);
        _sourceDatabase = mock(Database.class);
        when(_environment.openDatabase(any(Transaction.class), same(SOURCE_DATABASE), isA(DatabaseConfig.class)))
                .thenReturn(_sourceDatabase);
        when(_environment.openDatabase(isNull(), same(SOURCE_DATABASE), isA(DatabaseConfig.class)))
                .thenReturn(_sourceDatabase);
    }

    @Test
    public void testExecuteWithTwoDatabases()
    {
        String targetDatabaseName = "targetDatabase";
        Database targetDatabase = mock(Database.class);

        Transaction txn = mock(Transaction.class);

        when(_environment.openDatabase(same(txn), same(targetDatabaseName), isA(DatabaseConfig.class)))
                .thenReturn(targetDatabase);

        DatabaseTemplate databaseTemplate = new DatabaseTemplate(_environment, SOURCE_DATABASE, targetDatabaseName, txn);

        DatabaseRunnable databaseOperation = mock(DatabaseRunnable.class);
        databaseTemplate.run(databaseOperation);

        verify(databaseOperation).run(_sourceDatabase, targetDatabase, txn);
        verify(_sourceDatabase).close();
        verify(targetDatabase).close();
    }

    @Test
    public void testExecuteWithOneDatabases()
    {
        DatabaseTemplate databaseTemplate = new DatabaseTemplate(_environment, SOURCE_DATABASE, null, null);

        DatabaseRunnable databaseOperation = mock(DatabaseRunnable.class);
        databaseTemplate.run(databaseOperation);

        verify(databaseOperation).run(eq(_sourceDatabase), isNull(), isNull());
        verify(_sourceDatabase).close();
    }

}
