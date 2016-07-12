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

package org.apache.qpid.server.store.berkeleydb.upgrade;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.StoreException;

@SuppressWarnings("unused")
public class UpgradeFrom8To9 extends AbstractStoreUpgrade
{

    private static final String DEFAULT_VERSION = "6.1";

    @Override
    public void performUpgrade(final Environment environment,
                               final UpgradeInteractionHandler handler,
                               final ConfiguredObject<?> parent)
    {
        reportStarting(environment, 8);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        final Transaction transaction = environment.beginTransaction(null, null);
        try
        {
            Database userPreferencesDb = environment.openDatabase(transaction, "USER_PREFERENCES", dbConfig);
            userPreferencesDb.close();

            try (Database userPreferencesVersionDb = environment.openDatabase(transaction,
                                                                              "USER_PREFERENCES_VERSION",
                                                                              dbConfig))
            {
                if (userPreferencesVersionDb.count() == 0L)
                {
                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry value = new DatabaseEntry();
                    StringBinding.stringToEntry(DEFAULT_VERSION, key);
                    LongBinding.longToEntry(System.currentTimeMillis(), value);
                    OperationStatus status = userPreferencesVersionDb.put(transaction, key, value);
                    if (status != OperationStatus.SUCCESS)
                    {
                        throw new StoreException("Error initialising user preference version: " + status);
                    }
                }
            }

            transaction.commit();
            reportFinished(environment, 9);
        }
        catch (RuntimeException e)
        {
            try
            {
                if (transaction.isValid())
                {
                    transaction.abort();
                }
            }
            finally
            {
                throw e;
            }
        }
    }
}
