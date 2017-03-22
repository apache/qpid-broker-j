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
package org.apache.qpid.server.store.jdbc;

import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.PluggableService;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@PluggableService
public class DefaultConnectionProviderFactory implements JDBCConnectionProviderFactory
{

    public static PluggableFactoryLoader<JDBCConnectionProviderFactory> FACTORY_LOADER =  new PluggableFactoryLoader(JDBCConnectionProviderFactory.class);

    public static final String TYPE = "NONE";

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public ConnectionProvider getConnectionProvider(String connectionUrl, String username, String password, Map<String, String> providerAttributes)
    {
        return new DefaultConnectionProvider(connectionUrl, username, password);
    }

    @Override
    public Set<String> getProviderAttributeNames()
    {
        return Collections.emptySet();
    }

    public static Collection<String> getAllAvailableConnectionProviderTypes()
    {
        return FACTORY_LOADER.getSupportedTypes();
    }
}
