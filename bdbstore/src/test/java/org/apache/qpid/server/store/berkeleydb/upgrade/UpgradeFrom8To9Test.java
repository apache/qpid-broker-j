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

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

import org.junit.jupiter.api.Test;

public class UpgradeFrom8To9Test extends AbstractUpgradeTestCase
{
    private static final String PREFERENCES_DB_NAME = "USER_PREFERENCES";
    private static final String PREFERENCES_VERSION_DB_NAME = "USER_PREFERENCES_VERSION";

    @Override
    protected String getStoreDirectoryName()
    {
        return "bdbstore-v8";
    }

    @Test
    public void testPerformUpgrade()
    {
        UpgradeFrom8To9 upgrade = new UpgradeFrom8To9();
        upgrade.performUpgrade(_environment, UpgradeInteractionHandler.DEFAULT_HANDLER, getVirtualHost());

        assertDatabaseRecordCount(PREFERENCES_DB_NAME, 0);
        assertDatabaseRecordCount(PREFERENCES_VERSION_DB_NAME, 1);

        List<String> versions = loadVersions();
        assertEquals(1, versions.size(), "Unexpected number of versions loaded");
        assertEquals("6.1", versions.get(0), "Unexpected version");
    }

    private List<String> loadVersions()
    {
        final List<String> versions = new ArrayList<>();
        CursorOperation configuredObjectsCursor = new CursorOperation()
        {
            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                                     DatabaseEntry key, DatabaseEntry value)
            {
                String version = StringBinding.entryToString(key);
                versions.add(version);
            }
        };
        new DatabaseTemplate(_environment, PREFERENCES_VERSION_DB_NAME, null).run(configuredObjectsCursor);
        return versions;
    }
}
