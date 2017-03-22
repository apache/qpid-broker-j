/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.server.protocol.v1_0.store.bdb;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreFactory;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.berkeleydb.BDBEnvironmentContainer;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;

@PluggableService
public class BDBLinkStoreFactory implements LinkStoreFactory
{
    private static final String TYPE = "BDB";
    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public LinkStore create(final NamedAddressSpace addressSpace)
    {
        VirtualHost<?> virtualHost = (VirtualHost<?>) addressSpace;
        if (virtualHost instanceof BDBEnvironmentContainer)
        {
            return new BDBLinkStore((BDBEnvironmentContainer<?>) virtualHost);
        }
        else if (virtualHost.getParent()  instanceof BDBEnvironmentContainer)
        {
            return new BDBLinkStore((BDBEnvironmentContainer<?>) virtualHost.getParent());
        }
        else
        {
            throw new StoreException("Cannot create BDB Link Store for " + addressSpace);
        }
    }

    @Override
    public boolean supports(final NamedAddressSpace addressSpace)
    {
        if (addressSpace instanceof VirtualHost)
        {
            if (addressSpace instanceof BDBEnvironmentContainer)
            {
                return true;
            }
            else if (((VirtualHost) addressSpace).getParent()  instanceof BDBEnvironmentContainer)
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public int getPriority()
    {
        return 100;
    }
}
