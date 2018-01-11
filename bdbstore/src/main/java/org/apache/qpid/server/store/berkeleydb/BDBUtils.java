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

package org.apache.qpid.server.store.berkeleydb;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;

public class BDBUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BDBUtils.class);

    public static final DatabaseConfig DEFAULT_DATABASE_CONFIG = new DatabaseConfig().setTransactional(true).setAllowCreate(true);

    private static final Pattern NON_REP_JE_PARAM_PATTERN = Pattern.compile("^je\\.(?!rep\\.).*");
    private static final Pattern REP_JE_PARAM_PATTERN = Pattern.compile("^je\\.rep\\..*");

    public static void abortTransactionSafely(Transaction tx, EnvironmentFacade environmentFacade)
    {
        try
        {
            if (tx != null)
            {
                tx.abort();
            }
        }
        catch (RuntimeException e)
        {
            // We need the possible side effect of the facade restarting the environment but don't care about the exception
            environmentFacade.handleDatabaseException("Cannot abort transaction", e);
        }
    }

    public synchronized static void runCleaner(final Environment environment)
    {
        if (environment == null || !environment.isValid())
        {
            return;
        }

        boolean cleanerWasRunning = Boolean.parseBoolean(environment.getConfig().getConfigParam(EnvironmentConfig.ENV_RUN_CLEANER));

        try
        {
            if (cleanerWasRunning)
            {
                environment.getConfig().setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, Boolean.FALSE.toString());
            }

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Cleaning logs");
            }

            boolean cleaned = false;
            while (environment.cleanLog() > 0)
            {
                cleaned = true;
            }
            if (cleaned)
            {
                LOGGER.debug("Cleaned log");

                CheckpointConfig force = new CheckpointConfig();
                force.setForce(true);
                environment.checkpoint(force);

                LOGGER.debug("Checkpoint force complete");
            }
        }
        finally
        {
            if (cleanerWasRunning)
            {
                environment.getConfig().setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, Boolean.TRUE.toString());
            }
        }

    }

    public static Map<String, String> getReplicatedEnvironmentConfigurationParameters(ConfiguredObject<?> object)
    {
        return Collections.unmodifiableMap(getContextSettingsWithNameMatchingRegExpPattern(object, REP_JE_PARAM_PATTERN));
    }

    public static Map<String, String> getEnvironmentConfigurationParameters(ConfiguredObject<?> object)
    {
        Map<String, String> parameters = getContextSettingsWithNameMatchingRegExpPattern(object, NON_REP_JE_PARAM_PATTERN);
        if (!parameters.containsKey(EnvironmentConfig.MAX_MEMORY) && !parameters.containsKey(EnvironmentConfig.MAX_MEMORY_PERCENT))
        {
            parameters.put(EnvironmentConfig.MAX_MEMORY, String.valueOf(BDBVirtualHost.BDB_MIN_CACHE_SIZE));
        }
        return Collections.unmodifiableMap(parameters);
    }

    private static Map<String, String> getContextSettingsWithNameMatchingRegExpPattern(ConfiguredObject<?> object, Pattern pattern)
    {
        Map<String, String> targetMap = new HashMap<>();
        for (String name : object.getContextKeys(false))
        {
            if (pattern.matcher(name).matches())
            {
                String contextValue = object.getContextValue(String.class, name);
                targetMap.put(name, contextValue);
            }
        }

        return targetMap;
    }

    public static CacheMode getCacheMode(final ConfiguredObject<?> object)
    {
        if (object.getContextKeys(false).contains(EnvironmentFacade.CACHE_MODE_PROPERTY_NAME))
        {
            try
            {
                return object.getContextValue(CacheMode.class, EnvironmentFacade.CACHE_MODE_PROPERTY_NAME);
            }
            catch (IllegalArgumentException iae)
            {
                LOGGER.warn("Failed to parse {} as {}",
                            object.getContextValue(String.class, EnvironmentFacade.CACHE_MODE_PROPERTY_NAME),
                            CacheMode.class,
                            iae);
            }
        }
        return EnvironmentFacade.CACHE_MODE_DEFAULT;
    }

    public static <T> T getContextValue(final ConfiguredObject<?> parent,
                                        final Class<T> paremeterClass,
                                        final String parameterName,
                                        final T defaultValue)
    {
        if (parent.getContextKeys(false).contains(parameterName))
        {
            return parent.getContextValue(paremeterClass, parameterName);
        }
        else
        {
            return defaultValue;
        }
    }

    public static <T> T getContextValue(final ConfiguredObject<?> parent,
                                        final Class<T> paremeterClass,
                                        final Type type,
                                        final String parameterName,
                                        final T defaultValue)
    {
        if (parent.getContextKeys(false).contains(parameterName))
        {
            return parent.getContextValue(paremeterClass, type, parameterName);
        }
        else
        {
            return defaultValue;
        }
    }
}
