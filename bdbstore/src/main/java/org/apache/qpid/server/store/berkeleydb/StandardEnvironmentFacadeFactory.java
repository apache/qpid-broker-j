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

import java.lang.reflect.Type;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.rep.impl.RepParams;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.FileBasedSettings;

public class StandardEnvironmentFacadeFactory implements EnvironmentFacadeFactory
{

    static
    {
        // Force loading of RepParams class and adding its ConfigParam objects into EnvironmentParams.SUPPORTED_PARAMS
        // It is needed to avoid CME thrown from Iterator of EnvironmentParams.SUPPORTED_PARAMS on creation of Environment
        // when BDB HA VHN instance is activated at the same time causing loading of RepParams class and modification of
        // EnvironmentParams.SUPPORTED_PARAMS
        RepParams.GROUP_NAME.getName();
    }

    @SuppressWarnings("unchecked")
    @Override
    public EnvironmentFacade createEnvironmentFacade(final ConfiguredObject<?> parent)
    {
        final FileBasedSettings settings = (FileBasedSettings)parent;
        final String storeLocation = settings.getStorePath();

        StandardEnvironmentConfiguration sec = new StandardEnvironmentConfiguration()
        {
            @Override
            public String getName()
            {
                return parent.getName();
            }

            @Override
            public String getStorePath()
            {
                return storeLocation;
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
            public <T> T getFacadeParameter(final Class<T> clazz, final String parameterName, final T defaultValue)
            {
                return BDBUtils.getContextValue(parent, clazz, parameterName, defaultValue);
            }

            @Override
            public <T> T getFacadeParameter(final Class<T> paremeterClass, final Type type, final String parameterName, final T defaultValue)
            {
                return BDBUtils.getContextValue(parent, paremeterClass, type, parameterName, defaultValue);
            }
        };

        return new StandardEnvironmentFacade(sec);
    }
}
