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

package org.apache.qpid.server.protocol.v1_0;

import java.util.Collection;
import java.util.Collections;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreFactory;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUpdater;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;

@SuppressWarnings("unused")
@PluggableService
public class TestLinkStoreFactory implements LinkStoreFactory
{
    @Override
    public String getType()
    {
        return "test";
    }

    @Override
    public LinkStore create(final NamedAddressSpace addressSpace)
    {
        return new LinkStore()
        {
            @Override
            public Collection<LinkDefinition<Source, Target>> openAndLoad(final LinkStoreUpdater updater) throws StoreException
            {
                return Collections.emptyList();
            }

            @Override
            public void close() throws StoreException
            {

            }

            @Override
            public void saveLink(final LinkDefinition<Source, Target> link) throws StoreException
            {

            }

            @Override
            public void deleteLink(final LinkDefinition<Source, Target> link) throws StoreException
            {

            }

            @Override
            public void delete()
            {

            }

            @Override
            public TerminusDurability getHighestSupportedTerminusDurability()
            {
                return TerminusDurability.CONFIGURATION;
            }
        };
    }

    @Override
    public boolean supports(final NamedAddressSpace addressSpace)
    {
        return (addressSpace instanceof TestMemoryVirtualHost);
    }

    @Override
    public int getPriority()
    {
        return Integer.MIN_VALUE + 1;
    }
}
