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

import java.io.File;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.util.FileUtils;

public class BDBPreferenceStore extends AbstractBDBPreferenceStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BDBPreferenceStore.class);

    private final String _storePath;
    private final EnvironmentFacadeFactory _environmentFactory;
    private volatile EnvironmentFacade _environmentFacade;

    public BDBPreferenceStore(final ConfiguredObject<?> parent, final String storePath)
    {
        _storePath = storePath;
        _environmentFactory = new EnvironmentFacadeFactory()
        {
            @Override
            public EnvironmentFacade createEnvironmentFacade(final ConfiguredObject<?> object)
            {
                return new StandardEnvironmentFacade(new StandardEnvironmentConfiguration()
                {
                    @Override
                    public String getName()
                    {
                        return parent.getName();
                    }

                    @Override
                    public String getStorePath()
                    {
                        return storePath;
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
                });
            }
        };
    }

    @Override
    public Collection<PreferenceRecord> openAndLoad(final PreferenceStoreUpdater updater) throws StoreException
    {
        StoreState storeState = getStoreState();
        if (StoreState.OPENED.equals(storeState) || StoreState.OPENING.equals(storeState))
        {
            throw new IllegalStateException("PreferenceStore is already opened");
        }

        _environmentFacade = _environmentFactory.createEnvironmentFacade(null);
        return super.openAndLoad(updater);
    }

    @Override
    protected void doDelete()
    {
        if (_storePath != null)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Deleting preference store " + _storePath);
            }

            File preferenceStoreFile = new File(_storePath);
            if (!FileUtils.delete(preferenceStoreFile, true))
            {
                LOGGER.info("Failed to delete the preference store at location " + _storePath);
            }
        }
    }

    @Override
    protected void doClose()
    {
        try
        {
            _environmentFacade.close();
            _environmentFacade = null;
        }
        catch (RuntimeException e)
        {
            throw new StoreException("Exception occurred on preference store close", e);
        }
    }

    @Override
    protected EnvironmentFacade getEnvironmentFacade()
    {
        return _environmentFacade;
    }

    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }
}
