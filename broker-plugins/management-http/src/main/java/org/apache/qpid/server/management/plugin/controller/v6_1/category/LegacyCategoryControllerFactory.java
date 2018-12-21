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
package org.apache.qpid.server.management.plugin.controller.v6_1.category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.management.plugin.controller.CategoryControllerFactory;
import org.apache.qpid.server.management.plugin.controller.CategoryController;

import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.v6_1.LegacyManagementControllerFactory;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class LegacyCategoryControllerFactory implements CategoryControllerFactory
{
    static final String CATEGORY_BROKER = BrokerController.TYPE;
    private static final String CATEGORY_BROKER_LOGGER = "BrokerLogger";
    private static final String CATEGORY_BROKER_LOG_INCLUSION_RULE = "BrokerLogInclusionRule";
    private static final String CATEGORY_AUTHENTICATION_PROVIDER = "AuthenticationProvider";
    private static final String CATEGORY_USER = "User";
    private static final String CATEGORY_ACCESS_CONTROL_PROVIDER = "AccessControlProvider";
    private static final String CATEGORY_PLUGIN = "Plugin";
    private static final String CATEGORY_TRUST_STORE = "TrustStore";
    private static final String CATEGORY_KEY_STORE = "KeyStore";
    static final String CATEGORY_PORT = PortController.TYPE;
    private static final String CATEGORY_VIRTUAL_HOST_ALIAS = "VirtualHostAlias";
    private static final String CATEGORY_GROUP_PROVIDER = "GroupProvider";
    private static final String CATEGORY_GROUP = "Group";
    private static final String CATEGORY_GROUP_MEMBER = "GroupMember";
    static final String CATEGORY_VIRTUAL_HOST_NODE = "VirtualHostNode";
    private static final String CATEGORY_REMOTE_REPLICATION_NODE = "RemoteReplicationNode";
    static final String CATEGORY_VIRTUAL_HOST = VirtualHostController.TYPE;
    private static final String CATEGORY_VIRTUAL_HOST_LOGGER = "VirtualHostLogger";
    private static final String CATEGORY_VIRTUAL_HOST_LOG_INCLUSION_RULE = "VirtualHostLogInclusionRule";
    private static final String CATEGORY_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER = "VirtualHostAccessControlProvider";
    static final String CATEGORY_EXCHANGE = ExchangeController.TYPE;
    static final String CATEGORY_QUEUE = QueueController.TYPE;
    private static final String CATEGORY_BINDING = BindingController.TYPE;
    static final String CATEGORY_CONSUMER = ConsumerController.TYPE;
    static final String CATEGORY_CONNECTION = "Connection";
    static final String CATEGORY_SESSION = SessionController.TYPE;
    static final String CATEGORY_SYSTEM_CONFIG = "SystemConfig";
    static final Set<String> SUPPORTED_CATEGORIES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(CATEGORY_BROKER,
                                                                    CATEGORY_BROKER_LOGGER,
                                                                    CATEGORY_BROKER_LOG_INCLUSION_RULE,
                                                                    CATEGORY_AUTHENTICATION_PROVIDER,
                                                                    CATEGORY_USER,
                                                                    CATEGORY_ACCESS_CONTROL_PROVIDER,
                                                                    CATEGORY_PLUGIN,
                                                                    CATEGORY_TRUST_STORE,
                                                                    CATEGORY_KEY_STORE,
                                                                    CATEGORY_PORT,
                                                                    CATEGORY_VIRTUAL_HOST_ALIAS,
                                                                    CATEGORY_GROUP_PROVIDER,
                                                                    CATEGORY_GROUP,
                                                                    CATEGORY_GROUP_MEMBER,
                                                                    CATEGORY_VIRTUAL_HOST_NODE,
                                                                    CATEGORY_REMOTE_REPLICATION_NODE,
                                                                    CATEGORY_VIRTUAL_HOST,
                                                                    CATEGORY_VIRTUAL_HOST_LOGGER,
                                                                    CATEGORY_VIRTUAL_HOST_LOG_INCLUSION_RULE,
                                                                    CATEGORY_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER,
                                                                    CATEGORY_EXCHANGE,
                                                                    CATEGORY_QUEUE,
                                                                    CATEGORY_BINDING,
                                                                    CATEGORY_CONSUMER,
                                                                    CATEGORY_CONNECTION,
                                                                    CATEGORY_SESSION
                                                                   )));

    private static final Map<String, String> DEFAULT_TYPES = Collections.unmodifiableMap(new HashMap<String, String>()
    {
        {
            put(CATEGORY_BROKER_LOGGER, "Broker");
            put(CATEGORY_TRUST_STORE, "FileTrustStore");
            put(CATEGORY_KEY_STORE, "FileKeyStore");
            put(CATEGORY_GROUP, "ManagedGroup");
            put(CATEGORY_GROUP_MEMBER, "ManagedGroupMember");
            put(CATEGORY_VIRTUAL_HOST, "ProvidedStore");
            put(CATEGORY_QUEUE, "standard");
        }
    });

    @Override
    public CategoryController createController(final String type,
                                               final LegacyManagementController legacyManagementController)
    {
        switch (type)
        {
            case CATEGORY_ACCESS_CONTROL_PROVIDER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_BROKER:
                return new BrokerController(legacyManagementController,
                                            legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_BROKER_LOGGER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_BROKER_LOG_INCLUSION_RULE:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER_LOGGER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_AUTHENTICATION_PROVIDER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_USER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_AUTHENTICATION_PROVIDER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_PORT:
                return new PortController(legacyManagementController,
                                          legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_VIRTUAL_HOST_ALIAS:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_VIRTUAL_HOST_ALIAS},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_PLUGIN:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_TRUST_STORE:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_KEY_STORE:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_GROUP_PROVIDER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_GROUP:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_GROUP_PROVIDER},
                                                    null,
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_GROUP_MEMBER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_GROUP},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_VIRTUAL_HOST_NODE:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_BROKER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_REMOTE_REPLICATION_NODE:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_VIRTUAL_HOST_NODE},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_VIRTUAL_HOST:
                return new VirtualHostController(legacyManagementController,
                                                 legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_VIRTUAL_HOST_LOGGER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_VIRTUAL_HOST},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_VIRTUAL_HOST_LOG_INCLUSION_RULE:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_VIRTUAL_HOST_LOGGER},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_VIRTUAL_HOST},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_EXCHANGE:
                return new ExchangeController(legacyManagementController,
                                              legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_QUEUE:
                return new QueueController(legacyManagementController,
                                           legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_BINDING:
                return new BindingController(legacyManagementController
                );
            case CATEGORY_CONNECTION:
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    new String[]{CATEGORY_PORT, CATEGORY_VIRTUAL_HOST},
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_SESSION:
                return new SessionController(legacyManagementController,
                                             legacyManagementController.getTypeControllersByCategory(type));
            case CATEGORY_CONSUMER:
                return new ConsumerController(legacyManagementController
                );
            default:
                throw new IllegalArgumentException(String.format("Unsupported category '%s'", type));
        }
    }

    @Override
    public Set<String> getSupportedCategories()
    {
        return SUPPORTED_CATEGORIES;
    }

    @Override
    public String getModelVersion()
    {
        return LegacyManagementControllerFactory.MODEL_VERSION;
    }

    @Override
    public String getType()
    {
        return LegacyCategoryControllerFactory.class.getName();
    }
}
