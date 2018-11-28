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
package org.apache.qpid.server.virtualhostnode;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.messages.ConfigStoreMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.RemoteReplicationNode;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.VirtualHostStoreUpgraderAndRecoverer;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreProvider;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public abstract class AbstractStandardVirtualHostNode<X extends AbstractStandardVirtualHostNode<X>> extends AbstractVirtualHostNode<X>
                implements VirtualHostNode<X>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStandardVirtualHostNode.class);

    public AbstractStandardVirtualHostNode(Map<String, Object> attributes,
                                           Broker<?> parent)
    {
        super(parent, attributes);
    }

    @Override
    protected ListenableFuture<Void> activate()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Activating virtualhost node " + this);
        }

        getConfigurationStore().init(this);


        getConfigurationStore().upgradeStoreStructure();

        getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.CREATED());

        writeLocationEventLog();

        getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.RECOVERY_START());

        VirtualHostStoreUpgraderAndRecoverer upgrader = new VirtualHostStoreUpgraderAndRecoverer(this);
        ConfiguredObjectRecord[] initialRecords  = null;
        try
        {
            initialRecords = getInitialRecords();
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Could not process initial configuration", e);
        }

        final boolean isNew = upgrader.upgradeAndRecover(getConfigurationStore(), initialRecords);
        if(initialRecords.length > 0)
        {
            setAttributes(Collections.<String, Object>singletonMap(VIRTUALHOST_INITIAL_CONFIGURATION, "{}"));
        }

        getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.RECOVERY_COMPLETE());

        QueueManagingVirtualHost<?>  host = getVirtualHost();

        if (host != null)
        {
            final QueueManagingVirtualHost<?> recoveredHost = host;
            final ListenableFuture<Void> openFuture;
            recoveredHost.setFirstOpening(isNew && initialRecords.length == 0);
            openFuture = Subject.doAs(getSubjectWithAddedSystemRights(),
                                      new PrivilegedAction<ListenableFuture<Void>>()
                                      {
                                          @Override
                                          public ListenableFuture<Void> run()
                                          {
                                              return recoveredHost.openAsync();

                                          }
                                      });
            return openFuture;
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        final VirtualHost<?> virtualHost = getVirtualHost();
        final MessageStore messageStore = virtualHost == null ? null : virtualHost.getMessageStore();

        return doAfterAlways(closeVirtualHostIfExists(),
                             () -> {
                                 if (messageStore != null)
                                 {
                                     messageStore.closeMessageStore();
                                     messageStore.onDelete(virtualHost);
                                 }

                                 if (AbstractStandardVirtualHostNode.this instanceof PreferenceStoreProvider)
                                 {
                                     PreferenceStore preferenceStore =
                                             ((PreferenceStoreProvider) AbstractStandardVirtualHostNode.this).getPreferenceStore();
                                     if (preferenceStore != null)
                                     {
                                         preferenceStore.onDelete();
                                     }
                                 }
                                 DurableConfigurationStore configurationStore = getConfigurationStore();
                                 if (configurationStore != null)
                                 {
                                     configurationStore.closeConfigurationStore();
                                     configurationStore.onDelete(AbstractStandardVirtualHostNode.this);
                                 }
                                 onCloseOrDelete();
                             });
    }

    @Override
    public QueueManagingVirtualHost<?> getVirtualHost()
    {
        VirtualHost<?> vhost = super.getVirtualHost();
        if(vhost == null || vhost instanceof QueueManagingVirtualHost)
        {
            return (QueueManagingVirtualHost<?>)vhost;
        }
        else
        {
            throw new IllegalStateException(this + " has a virtual host which is not a queue managing virtual host " + vhost);
        }
    }

    @Override
    protected ConfiguredObjectRecord enrichInitialVirtualHostRootRecord(final ConfiguredObjectRecord vhostRecord)
    {
        ConfiguredObjectRecord replacementRecord;
        if (vhostRecord.getAttributes().get(ConfiguredObject.NAME) == null)
        {
            Map<String, Object> updatedAttributes = new LinkedHashMap<>(vhostRecord.getAttributes());
            updatedAttributes.put(ConfiguredObject.NAME, getName());
            if (!updatedAttributes.containsKey(VirtualHost.MODEL_VERSION))
            {
                updatedAttributes.put(VirtualHost.MODEL_VERSION, getBroker().getModelVersion());
            }
            replacementRecord = new ConfiguredObjectRecordImpl(vhostRecord.getId(),
                                                               vhostRecord.getType(),
                                                               updatedAttributes,
                                                               vhostRecord.getParents());
        }
        else if (vhostRecord.getAttributes().get(VirtualHost.MODEL_VERSION) == null)
        {
            Map<String, Object> updatedAttributes = new LinkedHashMap<>(vhostRecord.getAttributes());

            updatedAttributes.put(VirtualHost.MODEL_VERSION, getBroker().getModelVersion());

            replacementRecord = new ConfiguredObjectRecordImpl(vhostRecord.getId(),
                                                               vhostRecord.getType(),
                                                               updatedAttributes,
                                                               vhostRecord.getParents());
        }
        else
        {
            replacementRecord = vhostRecord;
        }

        return replacementRecord;
    }


    protected abstract void writeLocationEventLog();

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() +  "[id=" + getId() + ", name=" + getName() + ", state=" + getState() + "]";
    }

    @Override
    public Collection<RemoteReplicationNode<?>> getRemoteReplicationNodes()
    {
        return Collections.emptyList();
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        DurableConfigurationStore store = createConfigurationStore();
        if (store != null)
        {
            try
            {
                store.init(this);
            }
            catch (Exception e)
            {
                throw new IllegalConfigurationException("Cannot open node configuration store:" + e.getMessage(), e);
            }
            finally
            {
                try
                {
                    store.closeConfigurationStore();
                }
                catch(Exception e)
                {
                    LOGGER.warn("Failed to close database", e);
                }
            }
        }
    }
}
