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

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.plugin.Pluggable;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public interface JDBCConnectionProviderFactory extends Pluggable
{
    @Override
    String getType();

    ConnectionProvider getConnectionProvider(
            String connectionUrl,
            String username,
            String password,
            KeyStore<?> keyStore,
            String keyStorePathPropertyName,
            String keyStorePasswordPropertyName,
            TrustStore<?> trustStore,
            String trustStorePathPropertyName,
            String trustStorePasswordPropertyName,
            Map<String, String> providerAttributes)
            throws SQLException;

    Set<String> getProviderAttributeNames();

    final class FACTORIES
    {
        private FACTORIES()
        {
        }

        public static JDBCConnectionProviderFactory get(String type)
        {
            QpidServiceLoader qpidServiceLoader = new QpidServiceLoader();
            Iterable<JDBCConnectionProviderFactory> factories = qpidServiceLoader.atLeastOneInstanceOf(JDBCConnectionProviderFactory.class);
            for(JDBCConnectionProviderFactory factory : factories)
            {
                if(factory.getType().equals(type))
                {
                    return factory;
                }
            }
            return null;
        }
    }
}
