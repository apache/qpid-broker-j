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
package org.apache.qpid.server.management.amqp;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.BaseMessageInstance;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.SystemAddressSpaceCreator;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.protocol.LinkRegistry;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.MemoryMessageStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.DtxNotSupportedException;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNode;

public class ManagementAddressSpace implements NamedAddressSpace
{

    public static final String MANAGEMENT_ADDRESS_SPACE_NAME = "$management";
    private static final String MANAGEMENT_NODE_NAME = "$management";

    private final String _name;
    private final SystemAddressSpaceCreator.AddressSpaceRegistry _addressSpaceRegistry;
    private final ManagementNode _managementNode;
    private final VirtualHostPropertiesNode _propertiesNode;
    private final MessageStore _messageStore;
    private final MessageDestination _defaultDestination = new DefaultDestination();
    private final List<AMQPConnection<?>> _connections = new CopyOnWriteArrayList<>();
    private final LinkRegistry _linkRegistry = new NonDurableLinkRegistry();
    private final Broker<?> _broker;
    private final Principal _principal;

    public ManagementAddressSpace(final SystemAddressSpaceCreator.AddressSpaceRegistry addressSpaceRegistry)
    {
        this(MANAGEMENT_ADDRESS_SPACE_NAME, addressSpaceRegistry);
    }

    public ManagementAddressSpace(String name, final SystemAddressSpaceCreator.AddressSpaceRegistry addressSpaceRegistry)
    {
        _name = name;
        _addressSpaceRegistry = addressSpaceRegistry;
        _broker = addressSpaceRegistry.getBroker();

        _managementNode = new ManagementNode(this, addressSpaceRegistry.getBroker(), null);
        _propertiesNode = new VirtualHostPropertiesNode(this);
        _messageStore = new MemoryMessageStore();
        _principal = new ManagementAddressSpacePrincipal(this);
    }


    @Override
    public MessageSource getAttainedMessageSource(final String name)
    {
        if(_managementNode.getName().equals(name))
        {
            return _managementNode;
        }
        else if(_propertiesNode.getName().equals(name))
        {
            return _propertiesNode;
        }
        return null;
    }

    @Override
    public MessageDestination getAttainedMessageDestination(final String name)
    {
        if(_managementNode.getName().equals(name))
        {
            return _managementNode;
        }
        return null;
    }

    @Override
    public void registerConnection(final AMQPConnection<?> connection)
    {
        _connections.add(connection);
    }

    @Override
    public void deregisterConnection(final AMQPConnection<?> connection)
    {
        _connections.remove(connection);
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
    public boolean isActive()
    {
        return true;
    }

    @Override
    public MessageDestination getDefaultDestination()
    {
        return _defaultDestination;
    }

    @Override
    public LinkRegistry getLinkRegistry(final String remoteContainerId)
    {
        return _linkRegistry;
    }

    @Override
    public boolean authoriseCreateConnection(final AMQPConnection<?> connection)
    {
        _broker.authorise(Operation.ACTION("manage"));
        return true;
    }

    @Override
    public DtxRegistry getDtxRegistry()
    {
        throw new DtxNotSupportedException("Distributed Transactions are not supported within this address space");
    }

    @Override
    public MessageStore getMessageStore()
    {
        return _messageStore;
    }

    @Override
    public <T extends MessageSource> T createMessageSource(final Class<T> clazz, final Map<String, Object> attributes)
    {
        return null;
    }

    @Override
    public <T extends MessageDestination> T createMessageDestination(final Class<T> clazz,
                                                                     final Map<String, Object> attributes)
    {
        return null;
    }

    @Override
    public boolean hasMessageSources()
    {
        return true;
    }

    @Override
    public Collection<? extends Connection<?>> getConnections()
    {
        return Collections.unmodifiableList(_connections);
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public List<String> getGlobalAddressDomains()
    {
        return Collections.emptyList();
    }

    private class DefaultDestination implements MessageDestination
    {
        @Override
        public String getName()
        {
            return ExchangeDefaults.DEFAULT_EXCHANGE_NAME;
        }

        @Override
        public <M extends ServerMessage<? extends StorableMessageMetaData>> int send(final M message,
                                                                                     final String routingAddress,
                                                                                     final InstanceProperties instanceProperties,
                                                                                     final ServerTransaction txn,
                                                                                     final Action<? super BaseMessageInstance> postEnqueueAction)
        {
            MessageDestination destination = getAttainedMessageDestination(routingAddress);
            if(destination == null || destination == this)
            {
                return 0;
            }
            else
            {
                return destination.send(message, routingAddress, instanceProperties, txn, postEnqueueAction);
            }
        }
    }

    private class NonDurableLinkRegistry implements LinkRegistry
    {
        @Override
        public LinkModel getDurableSendingLink(final String name)
        {
            return null;
        }

        @Override
        public boolean registerSendingLink(final String name, final LinkModel link)
        {
            throw new ConnectionScopedRuntimeException("Durable links are not supported");
        }

        @Override
        public boolean unregisterSendingLink(final String name)
        {
            return false;
        }

        @Override
        public LinkModel getDurableReceivingLink(final String name)
        {
            return null;
        }

        @Override
        public boolean registerReceivingLink(final String name, final LinkModel link)
        {
            throw new ConnectionScopedRuntimeException("Durable links are not supported");
        }
    }
}
