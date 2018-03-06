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
package org.apache.qpid.server.virtualhostnode.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.jdbc.GenericJDBCConfigurationStore;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.JDBCDetails;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhostnode.AbstractStandardVirtualHostNode;

@ManagedObject(type = JDBCVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE, category = false ,
               validChildTypes = "org.apache.qpid.server.virtualhostnode.jdbc.JDBCVirtualHostNodeImpl#getSupportedChildTypes()")
public class JDBCVirtualHostNodeImpl extends AbstractStandardVirtualHostNode<JDBCVirtualHostNodeImpl>
        implements JDBCVirtualHostNode<JDBCVirtualHostNodeImpl>, JDBCContainer
{
    public static final String VIRTUAL_HOST_NODE_TYPE = "JDBC";

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

    @ManagedObjectFactoryConstructor
    public JDBCVirtualHostNodeImpl(Map<String, Object> attributes, Broker<?> parent)
    {
        super(attributes, parent);
    }

    @Override
    protected void writeLocationEventLog()
    {
    }

    @Override
    protected DurableConfigurationStore createConfigurationStore()
    {
        return new GenericJDBCConfigurationStore(VirtualHost.class);
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
    public JDBCDetails getJDBCDetails()
    {
        return JDBCDetails.getDetailsForJdbcUrl(getConnectionUrl(), this);
    }

    @Override
    public Connection getConnection()
    {
        try
        {
            return getStore().getConnection();
        }
        catch (SQLException e)
        {
            throw new ConnectionScopedRuntimeException(String.format(
                    "Error opening connection to database for VirtualHostNode '%s'",
                    getName()));
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [id=" + getId() + ", name=" + getName() +
                                        ", connectionUrl=" + getConnectionUrl() +
                                        ", connectionPoolType=" + getConnectionPoolType() +
                                        ", username=" + getUsername() + "]";
    }


    public static Map<String, Collection<String>> getSupportedChildTypes()
    {
        return Collections.singletonMap(VirtualHost.class.getSimpleName(), getSupportedVirtualHostTypes(true));
    }

    @Override
    public PreferenceStore getPreferenceStore()
    {
        return getStore().getPreferenceStore();
    }

    @Override
    public void addDeleteAction(final Action<Connection> action)
    {
        getStore().addDeleteAction(action);
    }

    @Override
    public void removeDeleteAction(final Action<Connection> action)
    {
        getStore().removeDeleteAction(action);
    }

    private GenericJDBCConfigurationStore getStore()
    {
        return (GenericJDBCConfigurationStore) getConfigurationStore();
    }
}
