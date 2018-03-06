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
package org.apache.qpid.server.virtualhost.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.jdbc.AbstractJDBCMessageStore;
import org.apache.qpid.server.store.jdbc.GenericJDBCMessageStore;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.JDBCDetails;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;

@ManagedObject(category = false, type = JDBCVirtualHostImpl.VIRTUAL_HOST_TYPE)
public class JDBCVirtualHostImpl extends AbstractVirtualHost<JDBCVirtualHostImpl>
        implements JDBCVirtualHost<JDBCVirtualHostImpl>, JDBCContainer
{
    public static final String VIRTUAL_HOST_TYPE = "JDBC";

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
    public JDBCVirtualHostImpl(final Map<String, Object> attributes,
                               final VirtualHostNode<?> virtualHostNode)
    {
        super(attributes, virtualHostNode);
    }

    @Override
    protected MessageStore createMessageStore()
    {
        return new GenericJDBCMessageStore();
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
                    "Error opening connection to database for VirtualHost '%s'",
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

    private AbstractJDBCMessageStore getStore()
    {
        return (AbstractJDBCMessageStore) getMessageStore();
    }
}
