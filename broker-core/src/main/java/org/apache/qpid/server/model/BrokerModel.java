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
package org.apache.qpid.server.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public final class BrokerModel extends Model
{

    /*
     * API version for the broker model
     *
     * 1.0 Initial version
     * 1.1 Addition of mandatory virtual host type / different types of virtual host
     * 1.3 Truststore/Keystore type => trustStoreType / type => keyStoreType
     * 1.4 Separate messageStoreSettings from virtualhost
     * 2.0 Introduce VirtualHostNode as a child of a Broker instead of VirtualHost
     * 3.0 Add VH aliases;
     *     Remove Broker#supportedVirtualHostNodeTypes, #supportedVirtualHostTypes, #supportedAuthenticationProviders,
     *            supportedPreferencesProviderTypes, VH#supportedExchangeTypes, VH#supportedQueueTypes
     *     Renamed FileTrustStore/FileKeyStore.path => FileTrustStore/FileKeyStore.storeUrl
     * 6.0 Add BrokerLogger as a child of Broker
     *     Replace the defaultVirtualHost (at Broker) with defaultVirtualHostNode flag (at VHN)
     *     Make Connections children of Ports instead of VHosts
     *     Bring model version and Qpid version into sync
     * 6.1 Remove JMX
     *     Remove PreferencesProvider
     * 7.0 Remove bindings, Consumer sole parent is Queue
     *     Remodelled alternateExchange as alternateBindings
     *     Remodelled Queue grouping attributes
     * 7.1 Operations for transition into DELETE state are replaced with delete method on AbstractConfiguredObject similar to open/close
     *     Introduced pluggable service AuthIdentityConnectionPropertyEnricher
     *     Introduced attribute Port#bindingAddress
     *     Introduced attribute Queue#expiryPolicy and context variable 'queue.defaultExpiryPolicy'
     *     Introduced property 'abstract' in annotation ManagedAttributeValueType
     *     Attributes property 'initialization' is set to 'materialize' for
     *         FileKeyStore#keyManagerFactoryAlgorithm and FileKeyStore#keyStoreType
     *         FileTrustStore#trustManagerFactoryAlgorithm and FileTrustStore#trustStoreType
     *     Introduced attribute FileKeyStore#useHostNameMatching
     *     Introduced context variable 'broker.failStartupWithErroredChildScope'
     *     Introduced ACL rule owner attribute
     *     // changes below are back ported into 7.0
     *     Introduced context variables for named caches on VirtualHost
     *     Introduced statistic Broker#inboundMessageSizeHighWatermark
     *     Introduced statistics Connection#lastInboundMessageTime, Connection#lastOutboundMessageTime and Connection#lastMessageTime
     *     Introduced statistics AmqpPort#totalConnectionCount
     *     Parameter bindingKey is made mandatory in Exchange#bind and Exchange#unbind
     *     Attribute OAuth2AuthenticationProvider#clientSecret is not mandatory anymore
     *     Introduced statistics QueueManagingVirtualHost#totalConnectionCount and QueueManagingVirtualHost#InboundMessageSizeHighWatermark
     *     BDBHAVirtualHostNode attributes name, groupName, address are made immutable
     *
     * 8.0
     *    Added new broker statistics: processCpuTime, processCpuLoad
     *    Added new context variables for queues and exchanges to configure behaviour on unknown declared arguments
     */
    public static final int MODEL_MAJOR_VERSION = 8;
    public static final int MODEL_MINOR_VERSION = 0;
    public static final String MODEL_VERSION = MODEL_MAJOR_VERSION + "." + MODEL_MINOR_VERSION;
    private static final Model MODEL_INSTANCE = new BrokerModel();
    private final Map<Class<? extends ConfiguredObject>, Class<? extends ConfiguredObject>> _parents =
            new HashMap<>();

    private final Map<Class<? extends ConfiguredObject>, Collection<Class<? extends ConfiguredObject>>> _children =
            new HashMap<Class<? extends ConfiguredObject>, Collection<Class<? extends ConfiguredObject>>>();

    private final Set<Class<? extends ConfiguredObject>> _supportedTypes =
            new HashSet<Class<? extends ConfiguredObject>>();
    private final ConfiguredObjectTypeRegistry _typeRegistry;

    private Class<? extends ConfiguredObject> _rootCategory;
    private final ConfiguredObjectFactory _objectFactory;

    private BrokerModel()
    {
        setRootCategory(SystemConfig.class);

        addRelationship(SystemConfig.class, Broker.class);

        addRelationship(Broker.class, BrokerLogger.class);
        addRelationship(Broker.class, VirtualHostNode.class);
        addRelationship(Broker.class, TrustStore.class);
        addRelationship(Broker.class, KeyStore.class);
        addRelationship(Broker.class, Port.class);
        addRelationship(Broker.class, AccessControlProvider.class);
        addRelationship(Broker.class, AuthenticationProvider.class);
        addRelationship(Broker.class, GroupProvider.class);
        addRelationship(Broker.class, Plugin.class);

        addRelationship(BrokerLogger.class, BrokerLogInclusionRule.class);

        addRelationship(VirtualHostNode.class, VirtualHost.class);
        addRelationship(VirtualHostNode.class, RemoteReplicationNode.class);

        addRelationship(VirtualHost.class, VirtualHostLogger.class);
        addRelationship(VirtualHost.class, VirtualHostAccessControlProvider.class);
        addRelationship(VirtualHost.class, Exchange.class);
        addRelationship(VirtualHost.class, Queue.class);

        addRelationship(VirtualHostLogger.class, VirtualHostLogInclusionRule.class);

        addRelationship(Port.class, VirtualHostAlias.class);
        addRelationship(Port.class, Connection.class);

        addRelationship(AuthenticationProvider.class, User.class);

        addRelationship(GroupProvider.class, Group.class);
        addRelationship(Group.class, GroupMember.class);

        addRelationship(Connection.class, Session.class);

        addRelationship(Queue.class, Consumer.class);

        _objectFactory = new ConfiguredObjectFactoryImpl(this);
        _typeRegistry = new ConfiguredObjectTypeRegistry((new QpidServiceLoader()).instancesOf(ConfiguredObjectRegistration.class),
                                                         (new QpidServiceLoader()).instancesOf(ConfiguredObjectAttributeInjector.class),
                                                         getSupportedCategories(),
                                                         _objectFactory);
    }

    @Override
    public final ConfiguredObjectTypeRegistry getTypeRegistry()
    {
        return _typeRegistry;
    }


    public static Model getInstance()
    {
        return MODEL_INSTANCE;
    }

    @Override
    public Class<? extends ConfiguredObject> getRootCategory()
    {
        return _rootCategory;
    }

    @Override
    public Class<? extends ConfiguredObject> getParentType(final Class<? extends ConfiguredObject> child)
    {
        return _parents.get(child);
    }

    @Override
    public int getMajorVersion()
    {
        return MODEL_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion()
    {
        return MODEL_MINOR_VERSION;
    }

    @Override
    public ConfiguredObjectFactory getObjectFactory()
    {
        return _objectFactory;
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getChildTypes(Class<? extends ConfiguredObject> parent)
    {
        Collection<Class<? extends ConfiguredObject>> childTypes = _children.get(parent);
        return childTypes == null ? Collections.<Class<? extends ConfiguredObject>>emptyList()
                : Collections.unmodifiableCollection(childTypes);
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getSupportedCategories()
    {
        return Collections.unmodifiableSet(_supportedTypes);
    }

    public void setRootCategory(final Class<? extends ConfiguredObject> rootCategory)
    {
        _rootCategory = rootCategory;
    }

    private void addRelationship(Class<? extends ConfiguredObject> parent, Class<? extends ConfiguredObject> child)
    {
        if(!_parents.containsKey(child))
        {
            _parents.put(child,parent);
        }
        else
        {
            throw new IllegalArgumentException("Child class " + child.getSimpleName() + " already has parent " + _parents.get(child).getSimpleName());
        }

        Collection<Class<? extends ConfiguredObject>> children = _children.get(parent);
        if (children == null)
        {
            children = new ArrayList<Class<? extends ConfiguredObject>>();
            _children.put(parent, children);
        }
        children.add(child);

        _supportedTypes.add(parent);
        _supportedTypes.add(child);

    }

}
