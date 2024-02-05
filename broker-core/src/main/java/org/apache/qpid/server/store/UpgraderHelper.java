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
 *
 */

package org.apache.qpid.server.store;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class UpgraderHelper
{
    static final String CONTEXT = "context";

    static final String CP_TYPE = "connectionPoolType";
    static final String BONECP = "BONECP";
    static final String HIKARICP = "HIKARICP";

    static final String PARTITION_COUNT_PARAM = "qpid.jdbcstore.bonecp.partitionCount";
    static final String MAX_POOL_SIZE_OLD_PARAM = "qpid.jdbcstore.bonecp.maxConnectionsPerPartition";
    static final String MIN_IDLE_OLD_PARAM = "qpid.jdbcstore.bonecp.minConnectionsPerPartition";

    static final String MAX_POOL_SIZE_PARAM = "qpid.jdbcstore.hikaricp.maximumPoolSize";
    static final String MIN_IDLE_PARAM = "qpid.jdbcstore.hikaricp.minimumIdle";

    static final Map<String, String> RENAME_MAPPING = Map.of(MAX_POOL_SIZE_OLD_PARAM, MAX_POOL_SIZE_PARAM,
            MIN_IDLE_OLD_PARAM, MIN_IDLE_PARAM);

    public static final Map<String, String> MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES = new HashMap<>();
    static
    {
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.protocolWhiteList",
                                                                                     "qpid.security.tls.protocolAllowList");
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.protocolBlackList",
                                                                                     "qpid.security.tls.protocolDenyList");
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.cipherSuiteWhiteList",
                                                                                     "qpid.security.tls.cipherSuiteAllowList");
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.cipherSuiteBlackList",
                                                                                     "qpid.security.tls.cipherSuiteDenyList");
    }

    public static Map<String, String> renameContextVariables(final Map<String, String> context,
                                                             final Map<String, String> oldToNewNameMapping)
    {
        final Map<String, String> newContext = new HashMap<>(context);
        oldToNewNameMapping.forEach((oldName, newName) -> {
            if (newContext.containsKey(oldName))
            {
                final Object object = newContext.remove(oldName);
                final String value = object == null ? null : String.valueOf(object);
                newContext.put(newName, value);
            }
        });
        return newContext;
    }

    public static Map<String, String> reverse(Map<String, String> map)
    {
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    /** Upgrades connection pool from BoneCP to HikariCP (model version 9.0 to 9.1) */
    public static ConfiguredObjectRecord upgradeConnectionPool(final ConfiguredObjectRecord record)
    {
        final Map<String, Object> attributes = record.getAttributes();

        final Map<String, Object> updatedAttributes = new HashMap<>(record.getAttributes());
        if (BONECP.equals(attributes.get(CP_TYPE)))
        {
            updatedAttributes.put(CP_TYPE, HIKARICP);
        }

        final Object contextObject = attributes.get(CONTEXT);

        if (contextObject instanceof Map)
        {
            final Map <String, String> context = (Map<String, String>) contextObject;
            final Map<String, String> newContext = UpgraderHelper.renameContextVariables(context, RENAME_MAPPING);

            final int partitionCount = newContext.get(PARTITION_COUNT_PARAM) != null
                    ? Integer.parseInt(String.valueOf(newContext.remove(PARTITION_COUNT_PARAM))) : 0;
            final int maximumPoolSize = newContext.get(MAX_POOL_SIZE_PARAM) != null && partitionCount != 0
                    ? Integer.parseInt(String.valueOf(newContext.get(MAX_POOL_SIZE_PARAM))) * partitionCount : 40;
            final int minIdle = newContext.get(MIN_IDLE_PARAM) != null && partitionCount != 0
                    ? Integer.parseInt(String.valueOf(newContext.get(MIN_IDLE_PARAM))) * partitionCount : 20;

            if (BONECP.equals(attributes.get(CP_TYPE)))
            {
                newContext.put(MAX_POOL_SIZE_PARAM, String.valueOf(maximumPoolSize));
                newContext.put(MIN_IDLE_PARAM, String.valueOf(minIdle));
            }
            else if ("Broker".equals(record.getType()))
            {
                if (newContext.containsKey(MAX_POOL_SIZE_PARAM))
                {
                    newContext.put(MAX_POOL_SIZE_PARAM, String.valueOf(maximumPoolSize));
                }
                if (newContext.containsKey(MIN_IDLE_PARAM))
                {
                    newContext.put(MIN_IDLE_PARAM, String.valueOf(minIdle));
                }
            }

            updatedAttributes.put(CONTEXT, newContext);
        }
        return new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
    }
}
