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
package org.apache.qpid.server.connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.util.ParameterizedTypes;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.plugin.ConnectionValidator;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;


@PluggableService
public class ConnectionVersionValidator implements ConnectionValidator
{
    public static final String VIRTUALHOST_ALLOWED_CONNECTION_VERSION = "virtualhost.allowedConnectionVersion";
    public static final String VIRTUALHOST_LOGGED_CONNECTION_VERSION = "virtualhost.loggedConnectionVersion";
    public static final String VIRTUALHOST_REJECTED_CONNECTION_VERSION = "virtualhost.rejectedConnectionVersion";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionVersionValidator.class);

    private static class BoundedCache extends LinkedHashMap<List<String>, Boolean>
    {
        private static final int CACHE_SIZE = 20;

        @Override
        protected boolean removeEldestEntry(final Map.Entry<List<String>, Boolean> eldest)
        {
            return size() >= CACHE_SIZE;
        }
    }

    private final Map<String, Set<List<String>>> _cachedLists = Collections.synchronizedMap(new HashMap<String, Set<List<String>>>());

    public ConnectionVersionValidator()
    {
        _cachedLists.put(VIRTUALHOST_ALLOWED_CONNECTION_VERSION, Collections.synchronizedSet(Collections.newSetFromMap(new BoundedCache())));
        _cachedLists.put(VIRTUALHOST_LOGGED_CONNECTION_VERSION, Collections.synchronizedSet(Collections.newSetFromMap(new BoundedCache())));
        _cachedLists.put(VIRTUALHOST_REJECTED_CONNECTION_VERSION, Collections.synchronizedSet(Collections.newSetFromMap(new BoundedCache())));
    }

    @Override
    public boolean validateConnectionCreation(final AMQPConnection<?> connection,
                                              final QueueManagingVirtualHost<?> virtualHost)
    {
        String connectionVersion = connection.getClientVersion();
        if (connectionVersion == null)
        {
            connectionVersion = "";
        }

        boolean valid = true;
        if (!connectionMatches(virtualHost, VIRTUALHOST_ALLOWED_CONNECTION_VERSION, connectionVersion))
        {
            if (connectionMatches(virtualHost, VIRTUALHOST_LOGGED_CONNECTION_VERSION, connectionVersion))
            {
                virtualHost.getBroker().getEventLogger().message(ConnectionMessages.CLIENT_VERSION_LOG(connection.getClientVersion()));
            }
            else if (connectionMatches(virtualHost, VIRTUALHOST_REJECTED_CONNECTION_VERSION, connectionVersion))
            {
                virtualHost.getBroker().getEventLogger().message(ConnectionMessages.CLIENT_VERSION_REJECT(connection.getClientVersion()));
                valid = false;
            }
        }

        return valid;
    }

    private boolean connectionMatches(VirtualHost<?> virtualHost, String listName, final String connectionVersion)
    {
        final List<String> versionRegexList = getContextValueList(virtualHost, listName);
        if (versionRegexList != null)
        {
            for (String versionRegEx : versionRegexList)
            {
                try
                {
                    if (connectionVersion.matches(versionRegEx))
                    {
                        return true;
                    }
                }
                catch (PatternSyntaxException e)
                {
                    if (_cachedLists.get(listName).add(versionRegexList))
                    {
                        LOGGER.warn("Invalid regex in context variable " + listName + ": " + versionRegEx);
                    }
                }
            }
        }
        return false;
    }

    private List<String> getContextValueList(final VirtualHost<?> virtualHost, final String variableName)
    {
        if (virtualHost.getContextKeys(false).contains(variableName))
        {
            return (List<String>) virtualHost.getContextValue(List.class,
                                                              ParameterizedTypes.LIST_OF_STRINGS,
                                                              variableName);
        }
        else
        {
            return Collections.emptyList();
        }
    }

    @Override
    public String getType()
    {
        return "ConnectionVersionValidator";
    }
}
