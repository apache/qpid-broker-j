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
package org.apache.qpid.server.store.berkeleydb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;

import com.sleepycat.je.TransactionConfig;
import org.apache.qpid.server.model.ConfiguredObject;

public interface EnvironmentFacade
{
    @SuppressWarnings("serial")
    final Map<String, String> ENVCONFIG_DEFAULTS = Collections.unmodifiableMap(new HashMap<String, String>()
    {{
        put(EnvironmentConfig.LOCK_N_LOCK_TABLES, "7");
        // Turn off stats generation - feature introduced (and on by default) from BDB JE 5.0.84
        put(EnvironmentConfig.STATS_COLLECT, "false");
        put(EnvironmentConfig.FILE_LOGGING_LEVEL, "OFF");
        put(EnvironmentConfig.CONSOLE_LOGGING_LEVEL, "OFF");
        put(EnvironmentConfig.CLEANER_UPGRADE_TO_LOG_VERSION, "-1");
        // Automatic cleaner adjustment has been seen to stop the cleaner working at all, disable it
        // Newer versions of BDB JE 6.3 ignore this setting.
        put(EnvironmentConfig.CLEANER_ADJUST_UTILIZATION, "false");
    }});

    String CACHE_MODE_PROPERTY_NAME = "qpid.bdb.cache_mode";
    CacheMode CACHE_MODE_DEFAULT = CacheMode.EVICT_LN;
    String LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME = "qpid.bdb.je.cleaner_protected_files_limit";
    int DEFAULT_LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT = 10;


    void upgradeIfNecessary(ConfiguredObject<?> parent);

    Database openDatabase(String databaseName, DatabaseConfig databaseConfig);
    Database clearDatabase(String databaseName, DatabaseConfig databaseConfig);

    Sequence openSequence(Database database, DatabaseEntry sequenceKey, SequenceConfig sequenceConfig);

    Transaction beginTransaction(TransactionConfig transactionConfig);

    void commit(Transaction tx, boolean sync);
    <X> ListenableFuture<X> commitAsync(Transaction tx, X val);

    RuntimeException handleDatabaseException(String contextMessage, RuntimeException e);

    void closeDatabase(String name);
    void close();

    long getTotalLogSize();

    void reduceSizeOnDisk();

    void flushLog();

    void setCacheSize(long cacheSize);

    void flushLogFailed(RuntimeException failure);

    void updateMutableConfig(ConfiguredObject<?> object);

    int cleanLog();

    void checkpoint(final boolean force);

    Map<String,Map<String,Object>> getEnvironmentStatistics(boolean reset);

    Map<String, Object> getTransactionStatistics(boolean reset);

    Map<String,Object> getDatabaseStatistics(String database, boolean reset);
}
