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
package org.apache.qpid.server.virtualhost.derby;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.derby.DerbyMessageStore;
import org.apache.qpid.server.store.derby.DerbyUtils;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.JDBCDetails;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.FileHelper;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;

public class DerbyVirtualHostImpl extends AbstractVirtualHost<DerbyVirtualHostImpl>
        implements DerbyVirtualHost<DerbyVirtualHostImpl>, JDBCContainer
{
    public static final String VIRTUAL_HOST_TYPE = "DERBY";

    static
    {
        DerbyUtils.configureDerbyLogging();
    }

    @ManagedAttributeField
    private String _storePath;

    @ManagedAttributeField
    private Long _storeUnderfullSize;

    @ManagedAttributeField
    private Long _storeOverfullSize;

    @ManagedObjectFactoryConstructor(conditionallyAvailable = true, condition = "org.apache.qpid.server.store.derby.DerbyUtils#isAvailable()")
    public DerbyVirtualHostImpl(final Map<String, Object> attributes,
                                final VirtualHostNode<?> virtualHostNode)
    {
        super(attributes, virtualHostNode);
    }


    @Override
    protected MessageStore createMessageStore()
    {
        return new DerbyMessageStore();
    }

    @Override
    public String getStorePath()
    {
        return _storePath;
    }

    @Override
    public Long getStoreUnderfullSize()
    {
        return _storeUnderfullSize;
    }

    @Override
    public Long getStoreOverfullSize()
    {
        return _storeOverfullSize;
    }

    @Override
    protected void validateMessageStoreCreation()
    {
        if (!new FileHelper().isWritableDirectory(getStorePath()))
        {
            throw new IllegalConfigurationException("The store path is not writable directory");
        }
    }

    @Override
    public JDBCDetails getJDBCDetails()
    {
        return JDBCDetails.getJdbcDetails("derby", this);
    }

    @Override
    public Connection getConnection()
    {
        try
        {
            return ((DerbyMessageStore) getMessageStore()).getConnection();
        }
        catch (SQLException e)
        {
            throw new ConnectionScopedRuntimeException(String.format(
                    "Error opening connection to database for VirtualHost '%s'", getName()));
        }
    }

    @Override
    public String getTableNamePrefix()
    {
        return "";
    }

    @Override
    public void addDeleteAction(final Action<Connection> action)
    {
        ((DerbyMessageStore) getMessageStore()).addDeleteAction(action);
    }

    @Override
    public void removeDeleteAction(final Action<Connection> action)
    {
        ((DerbyMessageStore) getMessageStore()).removeDeleteAction(action);
    }
}
