/*
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

package org.apache.qpid.server.protocol.v1_0.store.jdbc;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreFactory;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.jdbc.JDBCContainer;

@SuppressWarnings("unused")
@PluggableService
public class JDBCLinkStoreFactory implements LinkStoreFactory
{
    public static final String TYPE = "JDBC";

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public LinkStore create(final NamedAddressSpace addressSpace)
    {
        JDBCContainer jdbcContainer = null;
        if (addressSpace instanceof JDBCContainer)
        {
            jdbcContainer = (JDBCContainer) addressSpace;
        }
        else if (addressSpace instanceof VirtualHost
                 && ((VirtualHost) addressSpace).getParent() instanceof JDBCContainer)
        {
            jdbcContainer = (JDBCContainer) ((VirtualHost) addressSpace).getParent();
        }
        else
        {
            throw new StoreException(String.format("Named address space '%s' is not support by link store of type '%s'",
                                                   addressSpace.getName(),
                                                   TYPE));
        }

        return new JDBCLinkStore(jdbcContainer);
    }

    @Override
    public boolean supports(final NamedAddressSpace addressSpace)
    {
        return (addressSpace instanceof JDBCContainer
                || (addressSpace instanceof VirtualHost
                    && ((VirtualHost) addressSpace).getParent() instanceof JDBCContainer));
    }

    @Override
    public int getPriority()
    {
        return 100;
    }
}
