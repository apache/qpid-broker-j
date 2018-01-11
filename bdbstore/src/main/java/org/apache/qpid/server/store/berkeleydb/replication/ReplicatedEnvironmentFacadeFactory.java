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
package org.apache.qpid.server.store.berkeleydb.replication;

import java.lang.reflect.Type;
import java.util.Map;

import com.sleepycat.je.CacheMode;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.berkeleydb.BDBUtils;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacadeFactory;
import org.apache.qpid.server.store.berkeleydb.HASettings;

public class ReplicatedEnvironmentFacadeFactory implements EnvironmentFacadeFactory
{
    @Override
    public EnvironmentFacade createEnvironmentFacade(final ConfiguredObject<?> parent)
    {
        final HASettings settings = (HASettings) parent;

        ReplicatedEnvironmentConfiguration configuration = new ReplicatedEnvironmentConfiguration()
        {
            @Override
            public boolean isDesignatedPrimary()
            {
                return settings.isDesignatedPrimary();
            }

            @Override
            public String getStorePath()
            {
                return settings.getStorePath();
            }

            @Override
            public CacheMode getCacheMode()
            {
                return BDBUtils.getCacheMode(parent);
            }

            @Override
            public Map<String, String> getParameters()
            {
                return BDBUtils.getEnvironmentConfigurationParameters(parent);
            }

            @Override
            public Map<String, String> getReplicationParameters()
            {
                return BDBUtils.getReplicatedEnvironmentConfigurationParameters(parent);
            }

            @Override
            public String getHelperNodeName()
            {
                return settings.getHelperNodeName();
            }

            @Override
            public int getQuorumOverride()
            {
                return settings.getQuorumOverride();
            }

            @Override
            public int getPriority()
            {
                return settings.getPriority();
            }

            @Override
            public String getName()
            {
                return parent.getName();
            }

            @Override
            public String getHostPort()
            {
                return settings.getAddress();
            }

            @Override
            public String getHelperHostPort()
            {
                return settings.getHelperAddress();
            }

            @Override
            public String getGroupName()
            {
                return settings.getGroupName();
            }

            @Override
            public <T> T getFacadeParameter(final Class<T> paremeterClass, final String parameterName, final T defaultValue)
            {
                return BDBUtils.getContextValue(parent, paremeterClass, parameterName, defaultValue);
            }

            @Override
            public <T> T getFacadeParameter(final Class<T> paremeterClass,
                                            final Type type,
                                            final String parameterName,
                                            final T defaultValue)
            {
                return BDBUtils.getContextValue(parent, paremeterClass, type, parameterName, defaultValue);
            }
        };
        return new ReplicatedEnvironmentFacade(configuration);
    }
}
