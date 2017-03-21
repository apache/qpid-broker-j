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
package org.apache.qpid.server.virtualhost.berkeleydb;

import java.util.Collections;
import java.util.Map;

import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.berkeleydb.BDBMessageStore;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.BDBCacheSizeSetter;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;

@ManagedObject(category = false, type = BDBVirtualHostImpl.VIRTUAL_HOST_TYPE)
public class BDBVirtualHostImpl extends AbstractVirtualHost<BDBVirtualHostImpl> implements BDBVirtualHost<BDBVirtualHostImpl>
{
    public static final String VIRTUAL_HOST_TYPE = "BDB";

    @ManagedAttributeField
    private String _storePath;

    @ManagedAttributeField
    private Long _storeUnderfullSize;

    @ManagedAttributeField
    private Long _storeOverfullSize;

    @ManagedObjectFactoryConstructor(conditionallyAvailable = true, condition = "org.apache.qpid.server.JECheck#isAvailable()")
    public BDBVirtualHostImpl(final Map<String, Object> attributes,
                              final VirtualHostNode<?> virtualHostNode)
    {
        super(attributes, virtualHostNode);
        addChangeListener(new BDBCacheSizeSetter());
    }


    @Override
    protected MessageStore createMessageStore()
    {
        return new BDBMessageStore();
    }

    @Override
    public String getStorePath()
    {
        return _storePath;
    }

    @Override
    public Long getStoreUnderfullSize()
    {
        return _storeUnderfullSize;
    }

    @Override
    public Long getStoreOverfullSize()
    {
        return _storeOverfullSize;
    }

    @Override
    public void setBDBCacheSize(long cacheSize)
    {
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                environmentFacade.setCacheSize(cacheSize);
            }
        }
    }

    @Override
    public void updateMutableConfig()
    {
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                environmentFacade.updateMutableConfig(this);
            }
        }
    }

    @Override
    public int cleanLog()
    {
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
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
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
            if (environmentFacade != null)
            {
                environmentFacade.checkpoint(force);
            }
        }
    }

    @Override
    public Map<String, Map<String, Object>> environmentStatistics(final boolean reset)
    {
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
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
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
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
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            EnvironmentFacade environmentFacade = bdbMessageStore.getEnvironmentFacade();
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
        BDBMessageStore bdbMessageStore = (BDBMessageStore) getMessageStore();
        if (bdbMessageStore != null)
        {
            return bdbMessageStore.getEnvironmentFacade();
        }
        return null;
    }
}
