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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.berkeleydb.upgrade.Upgrader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.berkeleydb.logging.Slf4jLoggingHandler;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class StandardEnvironmentFacade implements EnvironmentFacade
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardEnvironmentFacade.class);

    private final String _storePath;
    private final ConcurrentMap<String, Database> _cachedDatabases = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatabaseEntry, Sequence> _cachedSequences = new ConcurrentHashMap<>();
    private final AtomicReference<Environment> _environment;

    private final Committer _committer;
    private final File _environmentPath;

    public StandardEnvironmentFacade(StandardEnvironmentConfiguration configuration)
    {
        _storePath = configuration.getStorePath();

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Creating environment at environment path " + _storePath);
        }

        _environmentPath = new File(_storePath);
        if (!_environmentPath.exists())
        {
            if (!_environmentPath.mkdirs())
            {
                throw new IllegalArgumentException("Environment path " + _environmentPath + " could not be read or created. "
                                                   + "Ensure the path is correct and that the permissions are correct.");
            }
        }

        String name = configuration.getName();
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setCacheMode(configuration.getCacheMode());
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, "OFF");
        envConfig.setConfigParam(EnvironmentConfig.CONSOLE_LOGGING_LEVEL, "OFF");
        envConfig.setLoggingHandler(new Slf4jLoggingHandler("["+configuration.getName()+"]"));

        LOGGER.debug("Cache mode {}", envConfig.getCacheMode());

        Map<String, String> params = new HashMap<>(EnvironmentFacade.ENVCONFIG_DEFAULTS);
        params.putAll(configuration.getParameters());

        for (Map.Entry<String, String> configItem : params.entrySet())
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Setting EnvironmentConfig key "
                             + configItem.getKey()
                             + " to '"
                             + configItem.getValue()
                             + "'");
            }
            envConfig.setConfigParam(configItem.getKey(), configItem.getValue());
        }

        envConfig.setExceptionListener(new LoggingAsyncExceptionListener());

        EnvHomeRegistry.getInstance().registerHome(_environmentPath);

        boolean success = false;
        try
        {
            _environment = new AtomicReference<>(new Environment(_environmentPath, envConfig));
            success = true;
        }
        finally
        {
            if (!success)
            {
                EnvHomeRegistry.getInstance().deregisterHome(_environmentPath);
            }
        }

        _committer =  new CoalescingCommiter(name, this);
        _committer.start();
    }


    @Override
    public Transaction beginTransaction(TransactionConfig transactionConfig)
    {
        return getEnvironment().beginTransaction(null, transactionConfig);
    }

    @Override
    public void commit(com.sleepycat.je.Transaction tx, boolean syncCommit)
    {
        try
        {
            tx.commitNoSync();
        }
        catch (RuntimeException de)
        {
            LOGGER.error("Got DatabaseException on commit, closing environment", de);

            closeEnvironmentSafely();
            handleCommitException(tx, de);
        }
        _committer.commit(tx, syncCommit);
    }

    @Override
    public <X> ListenableFuture<X> commitAsync(final Transaction tx, final X val)
    {
        try
        {
            tx.commitNoSync();
        }
        catch (RuntimeException de)
        {
            LOGGER.error("Got DatabaseException on commit, closing environment", de);

            closeEnvironmentSafely();
            handleCommitException(tx, de);
        }
        return _committer.commitAsync(tx, val);
    }

    @Override
    public void close()
    {
        try
        {
            _committer.stop();

            closeSequences();
            closeDatabases();
        }
        finally
        {
            try
            {
                closeEnvironment();
            }
            finally
            {
                EnvHomeRegistry.getInstance().deregisterHome(_environmentPath);
            }
        }
    }

    @Override
    public long getTotalLogSize()
    {
        return getEnvironment().getStats(null).getTotalLogSize();
    }

    @Override
    public void reduceSizeOnDisk()
    {
        BDBUtils.runCleaner(getEnvironment());
    }

    @Override
    public void flushLog()
    {
        try
        {
            getEnvironment().flushLog(true);
        }
        catch (RuntimeException e)
        {
            throw handleDatabaseException("Exception whilst syncing data to disk", e);
        }
    }

    @Override
    public void setCacheSize(long cacheSize)
    {
        Environment environment = getEnvironment();
        EnvironmentMutableConfig mutableConfig = environment.getMutableConfig();
        mutableConfig.setCacheSize(cacheSize);
        environment.setMutableConfig(mutableConfig);
    }

    @Override
    public void flushLogFailed(final RuntimeException e)
    {
        LOGGER.error("Closing store environment due to failure on syncing data to disk", e);

        try
        {
            close();
        }
        catch (Exception ex)
        {
            LOGGER.error("Exception closing store environment", ex);
        }
    }

    private void closeSequences()
    {
        RuntimeException firstThrownException = null;
        for (DatabaseEntry  sequenceKey : _cachedSequences.keySet())
        {
            try
            {
                closeSequence(sequenceKey);
            }
            catch(DatabaseException de)
            {
                if (firstThrownException == null)
                {
                    firstThrownException = de;
                }
            }
        }
        if (firstThrownException != null)
        {
            throw firstThrownException;
        }
    }

    private void closeDatabases()
    {
        RuntimeException firstThrownException = null;
        for (String databaseName : _cachedDatabases.keySet())
        {
            try
            {
                closeDatabase(databaseName);
            }
            catch(DatabaseException e)
            {
                if (firstThrownException == null)
                {
                    firstThrownException = e;
                }
            }
        }
        if (firstThrownException != null)
        {
            throw firstThrownException;
        }
    }

    private void closeEnvironmentSafely()
    {
        Environment environment = _environment.getAndSet(null);
        if (environment != null)
        {
            if (environment.isValid())
            {
                try
                {
                    closeDatabases();
                }
                catch(Exception e)
                {
                    LOGGER.error("Exception closing environment databases", e);
                }
            }
            try
            {
                environment.close();
            }
            catch (DatabaseException ex)
            {
                LOGGER.error("Exception closing store environment", ex);
            }
            catch (IllegalStateException ex)
            {
                LOGGER.error("Exception closing store environment", ex);
            }
        }
    }

    private Environment getEnvironment()
    {
        final Environment environment = _environment.get();
        if (environment == null)
        {
            throw new IllegalStateException("Environment is null.");
        }
        return environment;
    }

    @Override
    public void upgradeIfNecessary(ConfiguredObject<?> parent)
    {
        Upgrader upgrader = new Upgrader(getEnvironment(), parent);
        upgrader.upgradeIfNecessary();
    }

    private void closeEnvironment()
    {
        Environment environment = _environment.getAndSet(null);
        if (environment != null)
        {
            // Clean the log before closing. This makes sure it doesn't contain
            // redundant data. Closing without doing this means the cleaner may
            // not get a chance to finish.
            try
            {
                BDBUtils.runCleaner(environment);
            }
            finally
            {
                environment.close();
            }
        }
    }

    @Override
    public RuntimeException handleDatabaseException(String contextMessage, RuntimeException e)
    {
        Environment environment = _environment.get();
        if (environment != null && !environment.isValid())
        {
            closeEnvironmentSafely();
        }
        if (e instanceof StoreException)
        {
            return e;
        }
        return new StoreException(contextMessage, e);
    }

    @Override
    public Database openDatabase(String name, DatabaseConfig databaseConfig)
    {
        Database cachedHandle = _cachedDatabases.get(name);
        if (cachedHandle == null)
        {
            Database handle = getEnvironment().openDatabase(null, name, databaseConfig);
            Database existingHandle = _cachedDatabases.putIfAbsent(name, handle);
            if (existingHandle == null)
            {
                cachedHandle = handle;
            }
            else
            {
                cachedHandle = existingHandle;
                handle.close();
            }
        }
        return cachedHandle;
    }


    @Override
    public Database clearDatabase(String name, DatabaseConfig databaseConfig)
    {
        closeDatabase(name);
        getEnvironment().removeDatabase(null, name);
        return openDatabase(name, databaseConfig);
    }

    @Override
    public Sequence openSequence(final Database database,
                                 final DatabaseEntry sequenceKey,
                                 final SequenceConfig sequenceConfig)
    {
        Sequence cachedSequence = _cachedSequences.get(sequenceKey);
        if (cachedSequence == null)
        {
            Sequence handle = database.openSequence(null, sequenceKey, sequenceConfig);
            Sequence existingHandle = _cachedSequences.putIfAbsent(sequenceKey, handle);
            if (existingHandle == null)
            {
                cachedSequence = handle;
            }
            else
            {
                cachedSequence = existingHandle;
                handle.close();
            }
        }
        return cachedSequence;
    }


    private void closeSequence(final DatabaseEntry sequenceKey)
    {
        Sequence cachedHandle = _cachedSequences.remove(sequenceKey);
        if (cachedHandle != null)
        {
            cachedHandle.close();
        }
    }

    @Override
    public void closeDatabase(final String name)
    {
        Database cachedHandle = _cachedDatabases.remove(name);
        if (cachedHandle != null)
        {
            cachedHandle.close();
        }
    }

    private void handleCommitException(final Transaction tx, RuntimeException e)
    {
        if (e instanceof IllegalStateException && !tx.isValid())
        {
            throw new ConnectionScopedRuntimeException("Commit aborted", e);
        }
        else
        {
            throw handleDatabaseException("Commit failed", e);
        }
    }
}
