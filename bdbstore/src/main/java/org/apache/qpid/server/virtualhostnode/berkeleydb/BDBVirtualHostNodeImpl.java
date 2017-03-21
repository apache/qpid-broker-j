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
package org.apache.qpid.server.virtualhostnode.berkeleydb;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.qpid.server.logging.messages.ConfigStoreMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.BDBCacheSizeSetter;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.virtualhostnode.AbstractStandardVirtualHostNode;

@ManagedObject(type = BDBVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE, category = false,
               validChildTypes = "org.apache.qpid.server.virtualhostnode.berkeleydb.BDBVirtualHostNodeImpl#getSupportedChildTypes()")
public class BDBVirtualHostNodeImpl extends AbstractStandardVirtualHostNode<BDBVirtualHostNodeImpl> implements BDBVirtualHostNode<BDBVirtualHostNodeImpl>
{
    public static final String VIRTUAL_HOST_NODE_TYPE = "BDB";

    @ManagedAttributeField
    private String _storePath;

    @ManagedObjectFactoryConstructor(conditionallyAvailable = true, condition = "org.apache.qpid.server.JECheck#isAvailable()")
    public BDBVirtualHostNodeImpl(Map<String, Object> attributes, Broker<?> parent)
    {
        super(attributes, parent);
        addChangeListener(new BDBCacheSizeSetter());
    }

    @Override
    protected void writeLocationEventLog()
    {
        getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.STORE_LOCATION(getStorePath()));
    }

    @Override
    protected DurableConfigurationStore createConfigurationStore()
    {
        return new BDBConfigurationStore(VirtualHost.class);
    }

    @Override
    public String getStorePath()
    {
        return _storePath;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [id=" + getId() + ", name=" + getName() + ", storePath=" + getStorePath() + "]";
    }

    public static Map<String, Collection<String>> getSupportedChildTypes()
    {
        return Collections.singletonMap(VirtualHost.class.getSimpleName(), getSupportedVirtualHostTypes(true));
    }

    @Override
    public void setBDBCacheSize(long cacheSize)
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                environmentFacade.setCacheSize(cacheSize);
            }
        }
    }

    @Override
    public void updateMutableConfig()
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                environmentFacade.updateMutableConfig(this);
            }
        }
    }

    @Override
    public int cleanLog()
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                return environmentFacade.cleanLog();
            }
        }
        return 0;
    }

    @Override
    public void checkpoint(final boolean force)
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                environmentFacade.checkpoint(force);
            }
        }
    }

    @Override
    public Map<String, Map<String, Object>> environmentStatistics(final boolean reset)
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                return environmentFacade.getEnvironmentStatistics(reset);
            }
        }
        return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> transactionStatistics(final boolean reset)
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                return environmentFacade.getTransactionStatistics(reset);
            }
        }
        return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> databaseStatistics(String database, final boolean reset)
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            EnvironmentFacade environmentFacade = bdbConfigurationStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                return environmentFacade.getDatabaseStatistics(database, reset);
            }
        }
        return Collections.emptyMap();
    }

    @Override
    public EnvironmentFacade getEnvironmentFacade()
    {
        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) getConfigurationStore();
        if (bdbConfigurationStore != null)
        {
            return bdbConfigurationStore.getEnvironmentFacade();
        }
        return null;
    }

    @Override
    public PreferenceStore getPreferenceStore()
    {
        return ((BDBConfigurationStore) getConfigurationStore()).getPreferenceStore();
    }
}
