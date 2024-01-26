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
package org.apache.qpid.server.store.jdbc.hikaricp;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;

import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.store.jdbc.ConnectionProvider;
import org.apache.qpid.server.store.jdbc.JDBCConnectionProviderFactory;

@PluggableService
public class HikariCPConnectionProviderFactory implements JDBCConnectionProviderFactory
{
    static final String JDBC_STORE_PREFIX = "qpid.jdbcstore.";
    static final String HIKARICP_SETTING_PREFIX = JDBC_STORE_PREFIX + "hikaricp.";
    static final String MAX_POOLSIZE = HIKARICP_SETTING_PREFIX + "maximumPoolSize";
    static final String MIN_IDLE = HIKARICP_SETTING_PREFIX + "minimumIdle";

    private final Set<String> _supportedAttributes;

    public HikariCPConnectionProviderFactory()
    {
        _supportedAttributes = Arrays.stream(HikariConfig.class.getMethods())
                .filter(method -> isSetter(method) &&
                        Modifier.isPublic(method.getModifiers()) && method.getParameterCount() == 1 &&
                        isPrimitiveOrString(method.getParameterTypes()[0]))
                .map(method ->
                {
                    String name = method.getName().substring(3);
                    name = HIKARICP_SETTING_PREFIX + Character.toLowerCase(name.charAt(0)) + name.substring(1);
                    return name;
                }).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public String getType()
    {
        return "HIKARICP";
    }

    @Override
    public ConnectionProvider getConnectionProvider(
            final String connectionUrl,
            final String username,
            final String password,
            final KeyStore<?> keyStore,
            final String keyStorePathPropertyName,
            final String keyStorePasswordPropertyName,
            final TrustStore<?> trustStore,
            final String trustStorePathPropertyName,
            final String trustStorePasswordPropertyName,
            final Map<String, String> providerAttributes)
    {
        return new HikariCPConnectionProvider(connectionUrl, username, password, keyStore, keyStorePathPropertyName,
                keyStorePasswordPropertyName, trustStore, trustStorePathPropertyName, trustStorePasswordPropertyName, providerAttributes);
    }

    @Override
    public Set<String> getProviderAttributeNames()
    {
        return _supportedAttributes;
    }

    private boolean isSetter(Method method)
    {
        return method.getName().startsWith("set") && method.getName().length() > 3;
    }

    private boolean isPrimitiveOrString(Class<?> parameterType)
    {
        return parameterType.isPrimitive() || parameterType == String.class;
    }
}
