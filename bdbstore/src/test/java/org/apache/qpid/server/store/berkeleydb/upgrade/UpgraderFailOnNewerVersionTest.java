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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class UpgraderFailOnNewerVersionTest extends AbstractUpgradeTestCase
{
    private Upgrader _upgrader;

    @Override
    protected String getStoreDirectoryName()
    {
        return "bdbstore-v999";
    }

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        _upgrader = new Upgrader(_environment, getVirtualHost());
    }

    private int getStoreVersion()
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        int storeVersion = -1;
        try(Database versionDb = _environment.openDatabase(null, Upgrader.VERSION_DB_NAME, dbConfig);
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
    public void testUpgrade() throws Exception
    {
        assertEquals("Unexpected store version", 999, getStoreVersion());
        try
        {
            _upgrader.upgradeIfNecessary();
            fail("Store should not be able to be upgraded");
        }
        catch(ServerScopedRuntimeException ex)
        {
            assertEquals("Incorrect exception thrown", "Database version 999 is higher than the most recent known version: "
                                                        + BDBConfigurationStore.VERSION, ex.getMessage());
        }
    }

}
