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
package org.apache.qpid.server.virtualhost;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.messages.VirtualHostMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.DtxRegistry;

public abstract class AbstractNonConnectionAcceptingVirtualHost<X extends AbstractNonConnectionAcceptingVirtualHost<X>>
        extends AbstractConfiguredObject<X> implements VirtualHost<X>
{
    private final VirtualHostPrincipal _principal;

    public AbstractNonConnectionAcceptingVirtualHost(final ConfiguredObject<?> parent,
                                                     final Map<String, Object> attributes)
    {
        super(parent, attributes);

        _principal = new VirtualHostPrincipal(this);
        setState(State.UNAVAILABLE);
    }

    @Override
    public String getRedirectHost(final AmqpPort<?> port)
    {
        return null;
    }

    @Override
    public Principal getPrincipal()
    {
        return _principal;
    }

    @Override
    public boolean registerConnection(final AMQPConnection<?> connection,
                                      final ConnectionEstablishmentPolicy connectionEstablishmentPolicy)
    {
        throwUnsupported();
        return false;
    }

    @Override
    public void deregisterConnection(final AMQPConnection<?> connection)
    {
        throwUnsupported();
    }

    protected void throwUnsupported()
    {
        throw new IllegalStateException("The virtual host '" + getName() + "' does not permit this operation.");
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
        throwUnsupported();
        return null;
    }

    @Override
    public <T extends MessageDestination> T createMessageDestination(final Class<T> clazz,
                                                                     final Map<String, Object> attributes)
    {
        throwUnsupported();
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
    public <T extends LinkModel> T getSendingLink(final String remoteContainerId, final String linkName)
    {
        throwUnsupported();
        return null;
    }

    @Override
    public <T extends LinkModel> T getReceivingLink(final String remoteContainerId, final String linkName)
    {
        throwUnsupported();
        return null;
    }

    @Override
    public <T extends LinkModel> Collection<T> findSendingLinks(final Pattern containerIdPattern,
                                                                final Pattern linkNamePattern)
    {
        throwUnsupported();
        return null;
    }

    @Override
    public <T extends LinkModel> void visitSendingLinks(final LinkRegistryModel.LinkVisitor<T> visitor)
    {
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
    public String getLocalAddress(final String routingAddress)
    {
        return routingAddress;
    }

    @Override
    public boolean isActive()
    {
        return false;
    }

    @Override
    public String getModelVersion()
    {
        return BrokerModel.MODEL_VERSION;
    }

    @Override
    public String getProductVersion()
    {
        return getAncestor(Broker.class).getProductVersion();
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                             final Map<String, Object> attributes)
    {
        throwUnsupported();
        return null;
    }

    @Override
    public MessageDestination getAttainedMessageDestination(final String name, final boolean mayCreate)
    {
        return null;
    }

    @Override
    protected void logOperation(final String operation)
    {
        getAncestor(Broker.class).getEventLogger().message(VirtualHostMessages.OPERATION(operation));
    }
}
