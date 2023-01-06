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
package org.apache.qpid.server.virtualhostnode;

import java.util.Map;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.store.DurableConfigurationStore;

@ManagedObject(type = TestVirtualHostNode.VIRTUAL_HOST_NODE_TYPE, category=false)
public class TestVirtualHostNode extends AbstractStandardVirtualHostNode<TestVirtualHostNode>
{
    public static final String VIRTUAL_HOST_NODE_TYPE = "TestMemory";

    private final DurableConfigurationStore _store;
    private volatile AccessControl<?> _accessControl;

    public TestVirtualHostNode(final Broker<?> parent, final Map<String, Object> attributes)
    {
        this(parent, attributes, null);
    }

    public TestVirtualHostNode(final Broker<?> parent,
                               final Map<String, Object> attributes,
                               final DurableConfigurationStore store)
    {
        super(attributes, parent);
        _store = store;
    }

    @Override
    protected DurableConfigurationStore createConfigurationStore()
    {
        return _store;
    }

    @Override
    public DurableConfigurationStore getConfigurationStore()
    {
        return _store;
    }

    @Override
    protected void writeLocationEventLog()
    {
    }

    public void setAccessControl(final AccessControl<?> accessControl)
    {
        _accessControl = accessControl;
    }

    @Override
    protected AccessControl<?> getAccessControl()
    {
        return _accessControl == null ? super.getAccessControl() : _accessControl;
    }
}
