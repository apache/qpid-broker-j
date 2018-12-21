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
package org.apache.qpid.server.management.plugin.controller.v7_0.category;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.management.plugin.controller.CategoryController;
import org.apache.qpid.server.management.plugin.controller.CategoryControllerFactory;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.v7_0.LegacyManagementControllerFactory;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class LegacyCategoryControllerFactory implements CategoryControllerFactory
{
    static final String CATEGORY_BROKER = "Broker";
    private static final String CATEGORY_BROKER_LOGGER = "BrokerLogger";
    private static final String CATEGORY_BROKER_LOG_INCLUSION_RULE = "BrokerLogInclusionRule";
    private static final String CATEGORY_AUTHENTICATION_PROVIDER = "AuthenticationProvider";
    private static final String CATEGORY_USER = "User";
    private static final String CATEGORY_ACCESS_CONTROL_PROVIDER = "AccessControlProvider";
    private static final String CATEGORY_PLUGIN = "Plugin";
    private static final String CATEGORY_TRUST_STORE = "TrustStore";
    private static final String CATEGORY_KEY_STORE = "KeyStore";
    private static final String CATEGORY_PORT = "Port";
    private static final String CATEGORY_VIRTUAL_HOST_ALIAS = "VirtualHostAlias";
    private static final String CATEGORY_GROUP_PROVIDER = "GroupProvider";
    private static final String CATEGORY_GROUP = "Group";
    private static final String CATEGORY_GROUP_MEMBER = "GroupMember";
    private static final String CATEGORY_VIRTUAL_HOST_NODE = "VirtualHostNode";
    private static final String CATEGORY_REMOTE_REPLICATION_NODE = "RemoteReplicationNode";
    static final String CATEGORY_VIRTUAL_HOST = "VirtualHost";
    private static final String CATEGORY_VIRTUAL_HOST_LOGGER = "VirtualHostLogger";
    private static final String CATEGORY_VIRTUAL_HOST_LOG_INCLUSION_RULE = "VirtualHostLogInclusionRule";
    private static final String CATEGORY_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER = "VirtualHostAccessControlProvider";
    private static final String CATEGORY_EXCHANGE = "Exchange";
    private static final String CATEGORY_QUEUE = "Queue";
    private static final String CATEGORY_CONSUMER = "Consumer";
    private static final String CATEGORY_CONNECTION = "Connection";
    private static final String CATEGORY_SESSION = "Session";
    static final String CATEGORY_SYSTEM_CONFIG = "SystemConfig";
    static final Map<String, String> SUPPORTED_CATEGORIES =
            Collections.unmodifiableMap(new HashMap<String, String>()
            {
                {
                    put(CATEGORY_BROKER_LOGGER, CATEGORY_BROKER);
                    put(CATEGORY_BROKER_LOG_INCLUSION_RULE, CATEGORY_BROKER_LOGGER);
                    put(CATEGORY_AUTHENTICATION_PROVIDER, CATEGORY_BROKER);
                    put(CATEGORY_USER, CATEGORY_AUTHENTICATION_PROVIDER);
                    put(CATEGORY_ACCESS_CONTROL_PROVIDER, CATEGORY_BROKER);
                    put(CATEGORY_PLUGIN, CATEGORY_BROKER);
                    put(CATEGORY_TRUST_STORE, CATEGORY_BROKER);
                    put(CATEGORY_KEY_STORE, CATEGORY_BROKER);
                    put(CATEGORY_PORT, CATEGORY_BROKER);
                    put(CATEGORY_VIRTUAL_HOST_ALIAS, CATEGORY_PORT);
                    put(CATEGORY_GROUP_PROVIDER, CATEGORY_BROKER);
                    put(CATEGORY_GROUP, CATEGORY_GROUP_PROVIDER);
                    put(CATEGORY_GROUP_MEMBER, CATEGORY_GROUP);
                    put(CATEGORY_VIRTUAL_HOST_NODE, CATEGORY_BROKER);
                    put(CATEGORY_REMOTE_REPLICATION_NODE, CATEGORY_VIRTUAL_HOST_NODE);
                    put(CATEGORY_VIRTUAL_HOST, CATEGORY_VIRTUAL_HOST_NODE);
                    put(CATEGORY_VIRTUAL_HOST_LOGGER, CATEGORY_VIRTUAL_HOST);
                    put(CATEGORY_VIRTUAL_HOST_LOG_INCLUSION_RULE, CATEGORY_VIRTUAL_HOST_LOGGER);
                    put(CATEGORY_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER, CATEGORY_VIRTUAL_HOST);
                    put(CATEGORY_EXCHANGE, CATEGORY_VIRTUAL_HOST);
                    put(CATEGORY_QUEUE, CATEGORY_VIRTUAL_HOST);
                    put(CATEGORY_CONSUMER, CATEGORY_QUEUE);
                    put(CATEGORY_CONNECTION, CATEGORY_VIRTUAL_HOST);
                    put(CATEGORY_SESSION, CATEGORY_CONNECTION);
                    put(CATEGORY_BROKER, CATEGORY_SYSTEM_CONFIG);
                }
            });

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
        if (SUPPORTED_CATEGORIES.containsKey(type))
        {
            if (CATEGORY_VIRTUAL_HOST.equals(type) || CATEGORY_BROKER.equals(type))
            {
                return new ContainerController(legacyManagementController,
                                               type,
                                               SUPPORTED_CATEGORIES.get(type),
                                               DEFAULT_TYPES.get(type),
                                               legacyManagementController.getTypeControllersByCategory(type));
            }
            else
            {
                return new LegacyCategoryController(legacyManagementController,
                                                    type,
                                                    SUPPORTED_CATEGORIES.get(type),
                                                    DEFAULT_TYPES.get(type),
                                                    legacyManagementController.getTypeControllersByCategory(type));
            }
        }
        else
        {
            throw new IllegalArgumentException(String.format("Unsupported type '%s'", type));
        }
    }

    @Override
    public Set<String> getSupportedCategories()
    {
        return SUPPORTED_CATEGORIES.keySet();
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
