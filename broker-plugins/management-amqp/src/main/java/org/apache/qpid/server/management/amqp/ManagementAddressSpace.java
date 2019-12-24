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

import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemAddressSpaceCreator;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MemoryMessageStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.DtxNotSupportedException;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.ConnectionEstablishmentPolicy;
import org.apache.qpid.server.virtualhost.LinkRegistryFactory;
import org.apache.qpid.server.virtualhost.LinkRegistryModel;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNode;

public class ManagementAddressSpace implements NamedAddressSpace
{

    public static final String MANAGEMENT_ADDRESS_SPACE_NAME = "$management";
    private static final String MANAGEMENT_NODE_NAME = "$management";

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagementAddressSpace.class);

    private final String _name;
    private final SystemAddressSpaceCreator.AddressSpaceRegistry _addressSpaceRegistry;
    private final ManagementNode _managementNode;
    private final VirtualHostPropertiesNode _propertiesNode;
    private final MessageStore _messageStore;
    private final MessageDestination _defaultDestination = new DefaultDestination();
    private final List<AMQPConnection<?>> _connections = new CopyOnWriteArrayList<>();
    private final Broker<?> _broker;
    private final Principal _principal;
    private final UUID _id;
    private final ConcurrentMap<Object, ConcurrentMap<String, ProxyMessageSource>> _connectionSpecificDestinations = new ConcurrentHashMap<>();
    private final LinkRegistryModel _linkRegistry;

    public ManagementAddressSpace(final SystemAddressSpaceCreator.AddressSpaceRegistry addressSpaceRegistry)
    {
        this(MANAGEMENT_ADDRESS_SPACE_NAME, addressSpaceRegistry);
    }

    public ManagementAddressSpace(String name, final SystemAddressSpaceCreator.AddressSpaceRegistry addressSpaceRegistry)
    {
        _name = name;
        _addressSpaceRegistry = addressSpaceRegistry;
        _broker = addressSpaceRegistry.getBroker();

        _managementNode = new ManagementNode(this, addressSpaceRegistry.getBroker());
        _propertiesNode = new VirtualHostPropertiesNode(this);
        _messageStore = new MemoryMessageStore();
        _principal = new ManagementAddressSpacePrincipal(this);
        _id = UUID.nameUUIDFromBytes((_broker.getId().toString()+"/"+name).getBytes(StandardCharsets.UTF_8));

        Iterator<LinkRegistryFactory>
                linkRegistryFactories = (new QpidServiceLoader()).instancesOf(LinkRegistryFactory.class).iterator();
        if (linkRegistryFactories.hasNext())
        {
            final LinkRegistryFactory linkRegistryFactory = linkRegistryFactories.next();
            if (linkRegistryFactories.hasNext())
            {
                throw new RuntimeException("Found multiple implementations of LinkRegistry");
            }
            _linkRegistry = linkRegistryFactory.create(this);
            _linkRegistry.open();
        }
        else
        {
            _linkRegistry = null;
        }
    }


    @Override
    public UUID getId()
    {
        return _id;
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
        else
        {
            return getProxyNode(name);
        }
    }

    @Override
    public MessageDestination getAttainedMessageDestination(final String name, final boolean mayCreate)
    {
        if(_managementNode.getName().equals(name))
        {
            return _managementNode;
        }
        else
        {
            MessageDestination connectionSpecificDestinations = getProxyNode(name);
            if (connectionSpecificDestinations != null) return connectionSpecificDestinations;
        }

        return null;
    }

    ProxyMessageSource getProxyNode(final String name)
    {
        LOGGER.debug("RG: looking for proxy source {}", name);
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        Set<SessionPrincipal> sessionPrincipals = currentSubject.getPrincipals(SessionPrincipal.class);
        if (!sessionPrincipals.isEmpty())
        {
            Object connectionReference = sessionPrincipals.iterator().next().getSession().getConnectionReference();
            Map<String, ProxyMessageSource>
                    connectionSpecificDestinations = _connectionSpecificDestinations.get(connectionReference);
            if(connectionSpecificDestinations != null)
            {
                LOGGER.debug("RG: ", connectionSpecificDestinations);

                return connectionSpecificDestinations.get(name);
            }
        }
        return null;
    }

    ManagementNode getManagementNode()
    {
        return _managementNode;
    }

    @Override
    public boolean registerConnection(final AMQPConnection<?> connection,
                                      final ConnectionEstablishmentPolicy connectionEstablishmentPolicy)
    {
        _connections.add(connection);
        return true;
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
    public <T extends LinkModel> T getSendingLink(final String remoteContainerId, final String linkName)
    {
        return (T)_linkRegistry.getSendingLink(remoteContainerId, linkName);
    }

    @Override
    public <T extends LinkModel> T getReceivingLink(final String remoteContainerId, final String linkName)
    {
        return (T)_linkRegistry.getReceivingLink(remoteContainerId, linkName);
    }

    @Override
    public <T extends LinkModel> Collection<T> findSendingLinks(final Pattern containerIdPattern,
                                                                final Pattern linkNamePattern)
    {
        return _linkRegistry.findSendingLinks(containerIdPattern, linkNamePattern);
    }

    @Override
    public <T extends LinkModel> void visitSendingLinks(final LinkRegistryModel.LinkVisitor<T> visitor)
    {
        _linkRegistry.visitSendingLinks(visitor);
    }

    @Override
    public boolean authoriseCreateConnection(final AMQPConnection<?> connection)
    {
        _broker.authorise(Operation.PERFORM_ACTION("manage"));
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
        if(clazz == MessageSource.class)
        {
            return (T) createProxyNode(attributes);
        }
        else
        {
            return null;
        }
    }

    private ProxyMessageSource createProxyNode(final Map<String, Object> attributes)
    {
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        Set<SessionPrincipal> sessionPrincipals = currentSubject.getPrincipals(SessionPrincipal.class);
        if (!sessionPrincipals.isEmpty())
        {
            final ProxyMessageSource proxyMessageSource = new ProxyMessageSource(this, attributes);
            final AMQPSession<?,?> session = sessionPrincipals.iterator().next().getSession();
            final Object connectionReference = session.getConnectionReference();
            ConcurrentMap<String, ProxyMessageSource> connectionSpecificDestinations =
                    _connectionSpecificDestinations.get(connectionReference);
            if (connectionSpecificDestinations == null)
            {
                connectionSpecificDestinations = new ConcurrentHashMap<>();
                if(_connectionSpecificDestinations.putIfAbsent(connectionReference, connectionSpecificDestinations) == null)
                {
                    session.getAMQPConnection().addDeleteTask(new Action()
                    {
                        @Override
                        public void performAction(final Object object)
                        {
                            _connectionSpecificDestinations.remove(connectionReference);
                        }
                    });
                }
            }
            connectionSpecificDestinations.put(proxyMessageSource.getName(), proxyMessageSource);
            return proxyMessageSource;
        }
        else
        {
            return null;
        }
    }

    void removeProxyMessageSource(final Object connectionReference, final String name)
    {
        ConcurrentMap<String, ProxyMessageSource> connectionSpecificDestinations =
                _connectionSpecificDestinations.get(connectionReference);
        if(connectionSpecificDestinations != null)
        {
            connectionSpecificDestinations.remove(name);
        }
    }


    @Override
    public <T extends MessageDestination> T createMessageDestination(final Class<T> clazz,
                                                                     final Map<String, Object> attributes)
    {

        if(clazz == MessageDestination.class)
        {
            return (T) createProxyNode(attributes);
        }
        else
        {
            return null;
        }
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

    @Override
    public String getLocalAddress(final String routingAddress)
    {
        return routingAddress;
    }

    private class DefaultDestination implements MessageDestination
    {
        @Override
        public NamedAddressSpace getAddressSpace()
        {
            return ManagementAddressSpace.this;
        }

        @Override
        public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
                throws AccessControlException
        {

        }

        @Override
        public String getName()
        {
            return ExchangeDefaults.DEFAULT_EXCHANGE_NAME;
        }

        @Override
        public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(final M message,
                                                                                                   final String routingAddress,
                                                                                                   final InstanceProperties instanceProperties)
        {
            MessageDestination destination = getAttainedMessageDestination(routingAddress, false);
            if(destination == null || destination == this)
            {
                return new RoutingResult<>(message);
            }
            else
            {
                return destination.route(message, routingAddress, instanceProperties);
            }
        }

        @Override
        public boolean isDurable()
        {
            return true;
        }

        @Override
        public void linkAdded(final MessageSender sender, final PublishingLink link)
        {

        }

        @Override
        public void linkRemoved(final MessageSender sender, final PublishingLink link)
        {

        }

        @Override
        public MessageDestination getAlternateBindingDestination()
        {
            return null;
        }

        @Override
        public void removeReference(final DestinationReferrer destinationReferrer)
        {
        }

        @Override
        public void addReference(final DestinationReferrer destinationReferrer)
        {
        }
    }
}
