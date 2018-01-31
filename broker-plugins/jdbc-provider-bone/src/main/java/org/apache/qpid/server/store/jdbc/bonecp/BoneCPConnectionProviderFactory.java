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
package org.apache.qpid.server.store.jdbc.bonecp;

import static java.util.Collections.unmodifiableSet;

import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.jolbox.bonecp.BoneCPConfig;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.store.jdbc.ConnectionProvider;
import org.apache.qpid.server.store.jdbc.JDBCConnectionProviderFactory;

@PluggableService
public class BoneCPConnectionProviderFactory implements JDBCConnectionProviderFactory
{
    static final String JDBCSTORE_PREFIX = "qpid.jdbcstore.";
    static final String BONECP_SETTING_PREFIX = JDBCSTORE_PREFIX + "bonecp.";
    static final String PARTITION_COUNT = BONECP_SETTING_PREFIX + "partitionCount";
    static final String MAX_CONNECTIONS_PER_PARTITION = BONECP_SETTING_PREFIX + "maxConnectionsPerPartition";
    static final String MIN_CONNECTIONS_PER_PARTITION = BONECP_SETTING_PREFIX + "minConnectionsPerPartition";

    private final Set<String> _supportedAttributes;

    public BoneCPConnectionProviderFactory()
    {
        Set<String> names = Arrays.stream(BoneCPConfig.class.getMethods())
                                  .filter(m -> m.getName().startsWith("set")
                                               && m.getName().length() > 3
                                               && Modifier.isPublic(m.getModifiers())
                                               && m.getParameterCount() == 1
                                               && (m.getParameterTypes()[0].isPrimitive()
                                                   || m.getParameterTypes()[0] == String.class))
                                  .map(m -> {
                                      String n = m.getName().substring(3);
                                      n = BONECP_SETTING_PREFIX + Character.toLowerCase(n.charAt(0)) + n.substring(1);
                                      return n;
                                  }).collect(Collectors.toSet());
        _supportedAttributes = unmodifiableSet(names);
    }

    @Override
    public String getType()
    {
        return "BONECP";
    }

    @Override
    public ConnectionProvider getConnectionProvider(String connectionUrl, String username, String password, Map<String, String> providerAttributes)
            throws SQLException
    {
        return new BoneCPConnectionProvider(connectionUrl, username, password, providerAttributes);
    }

    @Override
    public Set<String> getProviderAttributeNames()
    {
        return _supportedAttributes;
    }
}
