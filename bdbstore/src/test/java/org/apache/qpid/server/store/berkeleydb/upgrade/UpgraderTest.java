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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore;
import org.apache.qpid.server.store.berkeleydb.tuple.ByteBufferBinding;

public class UpgraderTest extends AbstractUpgradeTestCase
{
    private Upgrader _upgrader;

    @Override
    protected String getStoreDirectoryName()
    {
        return "bdbstore-v4";
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        super.setUp();
        _upgrader = new Upgrader(_environment, getVirtualHost());
    }

    private int getStoreVersion(Environment environment)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        int storeVersion = -1;
        try(Database versionDb = environment.openDatabase(null, Upgrader.VERSION_DB_NAME, dbConfig);
            Cursor cursor = versionDb.openCursor(null, null))
        {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS)
            {
                int version = IntegerBinding.entryToInt(key);
                if (storeVersion < version)
                {
                    storeVersion = version;
                }
            }
        }
        return storeVersion;
    }

    @Test
    public void testUpgrade()
    {
        assertEquals(-1, getStoreVersion(_environment), "Unexpected store version");
        _upgrader.upgradeIfNecessary();
        assertEquals(BDBConfigurationStore.VERSION, getStoreVersion(_environment), "Unexpected store version");
        assertContent();
    }

    @Test
    public void testEmptyDatabaseUpgradeDoesNothing()
    {
        File nonExistentStoreLocation = new File(TMP_FOLDER, getTestName());
        deleteDirectoryIfExists(nonExistentStoreLocation);

        nonExistentStoreLocation.mkdir();
        Environment emptyEnvironment = createEnvironment(nonExistentStoreLocation);
        try
        {
            _upgrader = new Upgrader(emptyEnvironment, getVirtualHost());
            _upgrader.upgradeIfNecessary();

            List<String> databaseNames = emptyEnvironment.getDatabaseNames();
            List<String> expectedDatabases = new ArrayList<String>();
            expectedDatabases.add(Upgrader.VERSION_DB_NAME);
            assertEquals(expectedDatabases, databaseNames,
                         "Expectedonly VERSION table in initially empty store after upgrade: ");
            assertEquals(BDBConfigurationStore.VERSION, getStoreVersion(emptyEnvironment), "Unexpected store version");

        }
        finally
        {
            emptyEnvironment.close();
            nonExistentStoreLocation.delete();
        }
    }

    private void assertContent()
    {
        final ByteBufferBinding contentBinding = ByteBufferBinding.getInstance();
        CursorOperation contentCursorOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction, DatabaseEntry key,
                    DatabaseEntry value)
            {
                long id = LongBinding.entryToLong(key);
                assertTrue(id > 0, "Unexpected id");
                QpidByteBuffer content = contentBinding.entryToObject(value);
                assertNotNull(content, "Unexpected content");
                assertTrue(content.hasRemaining(), "Expected content");
            }
        };
        new DatabaseTemplate(_environment, "MESSAGE_CONTENT", null).run(contentCursorOperation);
    }
}
