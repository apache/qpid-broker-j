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
package org.apache.qpid.server.store;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;

import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNode;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class JsonFileConfigStoreConfigurationTest extends AbstractDurableConfigurationStoreTestCase
{
    @BeforeEach
    @Override
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.JSON));
        super.setUp();
    }

    @Override
    protected VirtualHostNode<?> createVirtualHostNode(String storeLocation, ConfiguredObjectFactory factory)
    {
        final JsonVirtualHostNode<?> parent = BrokerTestHelper.mockWithSystemPrincipal(JsonVirtualHostNode.class, mock(Principal.class));
        when(parent.getStorePath()).thenReturn(storeLocation);
        when(parent.getName()).thenReturn("testName");
        when(parent.getObjectFactory()).thenReturn(factory);
        when(parent.getModel()).thenReturn(factory.getModel());
        return parent;
    }

    @Override
    protected DurableConfigurationStore createConfigStore()
    {
        return new JsonFileConfigStore(VirtualHost.class);
    }
}
