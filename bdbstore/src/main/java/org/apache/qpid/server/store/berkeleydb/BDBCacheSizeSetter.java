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

import java.util.Collection;
import java.util.HashSet;

import com.sleepycat.je.EnvironmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;

public class BDBCacheSizeSetter extends AbstractConfigurationChangeListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BDBCacheSizeSetter.class);

    @Override
    public void stateChanged(ConfiguredObject<?> configuredObject, State oldState, State newState)
    {
        if (!(configuredObject instanceof BDBEnvironmentContainer))
        {
            throw new IllegalArgumentException("BDBCacheSizeSetter can only be set on a BDBEnvironmentContainer");
        }
        if (oldState != newState)
        {
            if ((newState == State.ACTIVE) || (oldState == State.ACTIVE))
            {
                assignJECacheSizes(configuredObject);
            }
        }
    }

    private static void assignJECacheSizes(ConfiguredObject<?> configuredObject)
    {
        Broker<?> broker = configuredObject.getModel().getAncestor(Broker.class, configuredObject.getCategoryClass(), configuredObject);

        if (broker == null)
        {
            throw new ServerScopedRuntimeException("Cannot find broker");
        }

        long totalCacheSize = broker.getContextValue(Long.class, BDBVirtualHost.QPID_BROKER_BDB_TOTAL_CACHE_SIZE);
        Collection<VirtualHostNode<?>> nodes = broker.getVirtualHostNodes();
        Collection<BDBEnvironmentContainer> bdbEnvironmentContainers = new HashSet<>();
        for (VirtualHostNode<?> virtualHostNode: nodes)
        {
            if (virtualHostNode instanceof BDBEnvironmentContainer && virtualHostNode.getState() == State.ACTIVE)
            {
                Long explicitJECacheSizeForVHostNode = getExplicitJECacheSize(virtualHostNode);
                if (explicitJECacheSizeForVHostNode == null)
                {
                    bdbEnvironmentContainers.add((BDBEnvironmentContainer) virtualHostNode);
                }
                else
                {
                    LOGGER.debug("VirtualHostNode {} has an explicit JE cacheSize of {}", virtualHostNode.getName(), explicitJECacheSizeForVHostNode);
                    totalCacheSize -= explicitJECacheSizeForVHostNode;
                }
            }

            VirtualHost<?> virtualHost = virtualHostNode.getVirtualHost();
            if (virtualHost instanceof BDBEnvironmentContainer && virtualHost.getState() == State.ACTIVE)
            {
                Long explicitJECacheSizeForVHost = getExplicitJECacheSize(virtualHost);
                if (explicitJECacheSizeForVHost == null)
                {
                    bdbEnvironmentContainers.add((BDBEnvironmentContainer) virtualHost);
                }
                else
                {
                    LOGGER.debug("VirtualHost {} has an explicit JE cacheSize of {}", virtualHost.getName(), explicitJECacheSizeForVHost);
                    totalCacheSize -= explicitJECacheSizeForVHost;
                }
            }
        }
        int numberOfJEEnvironments = bdbEnvironmentContainers.size();
        if (numberOfJEEnvironments > 0)
        {
            long cacheSize = totalCacheSize / numberOfJEEnvironments;
            if (cacheSize < BDBVirtualHost.BDB_MIN_CACHE_SIZE)
            {
                cacheSize = BDBVirtualHost.BDB_MIN_CACHE_SIZE;
            }
            LOGGER.debug("Setting JE cache size: totalCacheSize: {}; numberOfJEEnvironment: {}; cacheSize: {}", totalCacheSize, numberOfJEEnvironments, cacheSize);
            for (BDBEnvironmentContainer bdbEnvironmentContainer : bdbEnvironmentContainers)
            {
                bdbEnvironmentContainer.setBDBCacheSize(cacheSize);
            }
        }
    }

    private static Long getExplicitJECacheSize(ConfiguredObject<?> configuredObject)
    {
        try
        {
            Long jeMaxMemory = configuredObject.getContextValue(Long.class, EnvironmentConfig.MAX_MEMORY);
            if (jeMaxMemory != null && jeMaxMemory > 0)
            {
                return jeMaxMemory;
            }
        } catch (IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            Integer jeMaxMemoryPercent = configuredObject.getContextValue(Integer.class, EnvironmentConfig.MAX_MEMORY_PERCENT);
            if (jeMaxMemoryPercent != null && jeMaxMemoryPercent > 0)
            {
                return (Runtime.getRuntime().maxMemory() * (long) jeMaxMemoryPercent) / 100;
            }
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
        return null;
    }
}
