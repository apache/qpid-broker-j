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

package org.apache.qpid.server.virtualhost.berkeleydb;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.LinkRegistry;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;

/**
  Object that represents the VirtualHost whilst the VirtualHostNode is in the replica role.  The
  real virtualhost will be elsewhere in the group.
 */
@ManagedObject( category = false, type = "BDB_HA_REPLICA", register = false )
public class BDBHAReplicaVirtualHostImpl extends AbstractConfiguredObject<BDBHAReplicaVirtualHostImpl> implements BDBHAReplicaVirtualHost<BDBHAReplicaVirtualHostImpl>
{
    private final Broker<?> _broker;
    private final VirtualHostPrincipal _principal;

    @ManagedObjectFactoryConstructor(conditionallyAvailable = true, condition = "org.apache.qpid.server.JECheck#isAvailable()")
    public BDBHAReplicaVirtualHostImpl(final Map<String, Object> attributes, VirtualHostNode<?> virtualHostNode)
    {
        super(parentsMap(virtualHostNode), attributes);

        _broker = virtualHostNode.getParent(Broker.class);
        _principal = new VirtualHostPrincipal(this);
        setState(State.UNAVAILABLE);
    }

    @Override
    public boolean isActive()
    {
        return false;
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        throwUnsupportedForReplica();
    }

    @Override
    public String getModelVersion()
    {
        return BrokerModel.MODEL_VERSION;
    }

    @Override
    public Broker<?> getBroker()
    {
        return _broker;
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                             final Map<String, Object> attributes,
                                                                             final ConfiguredObject... otherParents)
    {
        throwUnsupportedForReplica();
        return null;
    }

    @Override
    public MessageDestination getAttainedMessageDestination(final String name)
    {
        return null;
    }

    @Override
    public String getRedirectHost(final AmqpPort<?> port)
    {
        return null;
    }

    @Override
    public Collection<? extends Connection<?>> getConnections()
    {
        return Collections.emptyList();
    }


    @Override
    public MessageSource getAttainedMessageSource(final String name)
    {
        return null;
    }

    @Override
    public MessageDestination getDefaultDestination()
    {
        return null;
    }

    @Override
    public MessageStore getMessageStore()
    {
        return null;
    }

    @Override
    public <T extends MessageSource> T createMessageSource(final Class<T> clazz, final Map<String, Object> attributes)
    {
        throwUnsupportedForReplica();
        return null;
    }

    @Override
    public <T extends MessageDestination> T createMessageDestination(final Class<T> clazz,
                                                                     final Map<String, Object> attributes)
    {
        throwUnsupportedForReplica();
        return null;
    }

    @Override
    public boolean hasMessageSources()
    {
        return false;
    }

    @Override
    public DtxRegistry getDtxRegistry()
    {
        return null;
    }

    @Override
    public LinkRegistry getLinkRegistry(final String remoteContainerId)
    {
        return null;
    }

    @Override
    public EventLogger getEventLogger()
    {
        return null;
    }

    @Override
    public boolean authoriseCreateConnection(final AMQPConnection<?> connection)
    {
        return false;
    }

    @Override
    public List<String> getGlobalAddressDomains()
    {
        return Collections.emptyList();
    }

    @Override
    public Principal getPrincipal()
    {
        return _principal;
    }

    @Override
    public void registerConnection(final AMQPConnection<?> connection)
    {
        throwUnsupportedForReplica();
    }

    @Override
    public void deregisterConnection(final AMQPConnection<?> connection)
    {
        throwUnsupportedForReplica();
    }

    private void throwUnsupportedForReplica()
    {
        throw new IllegalStateException("The virtual host state of " + getState()
                                        + " does not permit this operation.");
    }

}
