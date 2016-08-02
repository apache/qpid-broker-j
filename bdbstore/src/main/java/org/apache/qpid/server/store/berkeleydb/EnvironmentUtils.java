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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.utilint.IntegralLongAvg;
import com.sleepycat.je.utilint.Stat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;


import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BINS_BYLEVEL;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BIN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_DELETED_LN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_INS_BYLEVEL;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_IN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_LN_COUNT;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_MAINTREE_MAXDEPTH;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_RELATCHES_REQUIRED;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_ROOT_SPLITS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_BIN_ENTRIES_HISTOGRAM;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_BEGINS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_COMMITS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XAABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XACOMMITS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_XAPREPARES;


final public class EnvironmentUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtils.class);

    private EnvironmentUtils()
    {
    }

    public static Map<String,Object> getTransactionStatistics(Environment environment, boolean reset)
    {
        StatsConfig config = new StatsConfig();
        config.setClear(reset);
        config.setFast(false);
        final TransactionStats stats = environment.getTransactionStats(config);
        Map<String,Object> results = new LinkedHashMap<>();

        results.put(TXN_ACTIVE.getName(), stats.getNActive());
        results.put(TXN_BEGINS.getName(), stats.getNBegins());
        results.put(TXN_COMMITS.getName(), stats.getNCommits());
        results.put(TXN_ABORTS.getName(), stats.getNAborts());
        results.put(TXN_XAPREPARES.getName(), stats.getNXAPrepares());
        results.put(TXN_XACOMMITS.getName(), stats.getNXACommits());
        results.put(TXN_XAABORTS.getName(), stats.getNXAAborts());

        return results;
    }


    public static Map<String,Map<String,Object>> getEnvironmentStatistics(Environment environment, boolean reset)
    {
        StatsConfig config = new StatsConfig();
        config.setClear(reset);
        config.setFast(false);
        EnvironmentStats stats = environment.getStats(config);
        Collection<StatGroup> statGroups = stats.getStatGroups();
        return getStatsFromStatGroup(statGroups);
    }

    public static Map<String,Object> getDatabaseStatistics(Environment environment, String database, boolean reset)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        DbInternal.setUseExistingConfig(dbConfig, true);
        try (Database db = environment.openDatabase(null, database, dbConfig))
        {
            StatsConfig config = new StatsConfig();
            config.setClear(reset);
            config.setFast(false);

            BtreeStats stats = (BtreeStats) db.getStats(config);

            Map<String, Object> results = new TreeMap<>();
            results.put(BTREE_BIN_COUNT.getName(), stats.getBottomInternalNodeCount());
            results.put(BTREE_DELETED_LN_COUNT.getName(), stats.getDeletedLeafNodeCount());
            results.put(BTREE_IN_COUNT.getName(), stats.getInternalNodeCount());
            results.put(BTREE_LN_COUNT.getName(), stats.getLeafNodeCount());
            results.put(BTREE_MAINTREE_MAXDEPTH.getName(), stats.getMainTreeMaxDepth());
            results.put(BTREE_INS_BYLEVEL.getName(), Arrays.asList(stats.getINsByLevel()));
            results.put(BTREE_BINS_BYLEVEL.getName(), Arrays.asList(stats.getBINsByLevel()));
            results.put(BTREE_BIN_ENTRIES_HISTOGRAM.getName(), Arrays.asList(stats.getBINEntriesHistogram()));
            results.put(BTREE_RELATCHES_REQUIRED.getName(), stats.getRelatches());
            results.put(BTREE_ROOT_SPLITS.getName(), stats.getRootSplits());

            return results;
        }

    }

    private static Map<String, Map<String, Object>> getStatsFromStatGroup(final Collection<StatGroup> statGroups)
    {
        Map<String,Map<String,Object>> results = new LinkedHashMap<>();
        for(StatGroup group : statGroups)
        {
            Map<String,Object> groupResults = new TreeMap<>();
            for(Map.Entry<StatDefinition, Stat<?>> entry : group.getStats().entrySet())
            {
                if(!entry.getValue().isNotSet())
                {
                    Object value = entry.getValue().get();
                    if(value instanceof IntegralLongAvg)
                    {
                        value = ((Number) value).doubleValue();
                    }
                    groupResults.put(entry.getKey().getName(), value);

                }
            }
            if(!groupResults.isEmpty())
            {
                results.put(group.getName(), groupResults);
            }
        }
        return results;
    }

    public static void updateMutableConfig(Environment environment, Set<String> paramsSetByDefault, boolean includeHA, final ConfiguredObject<?> object)
    {
        EnvironmentMutableConfig mutableConfig = environment.getMutableConfig();

        final Set<String> contextVariables = object.getContextKeys(false);

        for(Map.Entry<String, ConfigParam> entry : EnvironmentParams.SUPPORTED_PARAMS.entrySet())
        {
            String paramName = entry.getKey();
            ConfigParam param = entry.getValue();

            if(param.isMutable() && (includeHA || !param.isForReplication()))
            {
                boolean contextValueSet = contextVariables.contains(paramName);
                boolean currentlySetInEnv = mutableConfig.isConfigParamSet(paramName);
                String contextValue = contextValueSet ? object.getContextValue(String.class, paramName) : null;
                contextValueSet = (contextValue != null);

                try
                {
                    if(contextValueSet)
                    {
                        if(!currentlySetInEnv || !contextValue.equals(mutableConfig.getConfigParam(paramName)))
                        {
                            mutableConfig.setConfigParam(paramName, contextValue);
                            LOGGER.debug("Setting BDB configuration parameter '{}' to value '{}'.", param, contextValue);
                        }
                    }
                    else if(currentlySetInEnv && !paramsSetByDefault.contains(paramName))
                    {
                        mutableConfig.setConfigParam(paramName, param.getDefault());
                        LOGGER.debug("Setting BDB configuration parameter '{}' to its default value.", param);
                    }
                }
                catch (IllegalArgumentException e)
                {
                    LOGGER.warn("Unable to set BDB configuration parameter '{}' to value '{}'.", param, contextValue, e);
                }

            }
        }
    }

}
