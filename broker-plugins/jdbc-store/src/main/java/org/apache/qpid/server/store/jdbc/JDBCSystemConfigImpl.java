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

import java.security.Principal;
import java.util.Map;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AbstractSystemConfig;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.SystemConfigFactoryConstructor;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;

@ManagedObject( category = false, type = JDBCSystemConfigImpl.SYSTEM_CONFIG_TYPE)
public class JDBCSystemConfigImpl extends AbstractSystemConfig<JDBCSystemConfigImpl> implements JDBCSystemConfig<JDBCSystemConfigImpl>
{
    public static final String SYSTEM_CONFIG_TYPE = "JDBC";

    @ManagedAttributeField
    private String _connectionUrl;
    @ManagedAttributeField
    private String _connectionPoolType;
    @ManagedAttributeField
    private String _username;
    @ManagedAttributeField
    private String _password;
    @ManagedAttributeField
    private String _tableNamePrefix;
    @ManagedAttributeField
    private FileKeyStore<?> _keyStore;
    @ManagedAttributeField
    private String _keyStorePathPropertyName;
    @ManagedAttributeField
    private String _keyStorePasswordPropertyName;
    @ManagedAttributeField
    private FileTrustStore<?> _trustStore;
    @ManagedAttributeField
    private String _trustStorePathPropertyName;
    @ManagedAttributeField
    private String _trustStorePasswordPropertyName;

    @SystemConfigFactoryConstructor
    public JDBCSystemConfigImpl(final TaskExecutor taskExecutor,
                                final EventLogger eventLogger,
                                final Principal systemPrincipal,
                                final Map<String, Object> attributes)
    {
        super(taskExecutor, eventLogger, systemPrincipal, attributes);
    }

    @Override
    protected DurableConfigurationStore createStoreObject()
    {
        return new GenericJDBCConfigurationStore(Broker.class);
    }

    @Override
    public String getConnectionUrl()
    {
        return _connectionUrl;
    }

    @Override
    public String getConnectionPoolType()
    {
        return _connectionPoolType;
    }

    @Override
    public String getUsername()
    {
        return _username;
    }

    @Override
    public String getPassword()
    {
        return _password;
    }

    @Override
    public String getTableNamePrefix()
    {
        return _tableNamePrefix;
    }

    @Override
    public FileKeyStore<?> getKeyStore()
    {
        return _keyStore;
    }

    @Override
    public String getKeyStorePathPropertyName()
    {
        return _keyStorePathPropertyName;
    }

    @Override
    public String getKeyStorePasswordPropertyName()
    {
        return _keyStorePasswordPropertyName;
    }

    @Override
    public FileTrustStore<?> getTrustStore()
    {
        return _trustStore;
    }

    @Override
    public String getTrustStorePathPropertyName()
    {
        return _trustStorePathPropertyName;
    }

    @Override
    public String getTrustStorePasswordPropertyName()
    {
        return _trustStorePasswordPropertyName;
    }

    @Override
    public PreferenceStore getPreferenceStore()
    {
        return ((GenericJDBCConfigurationStore) getConfigurationStore()).getPreferenceStore();
    }
}
