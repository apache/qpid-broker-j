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
 */

package org.apache.qpid.server.virtualhost.berkeleydb;

import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.virtualhost.AbstractNonConnectionAcceptingVirtualHost;

/**
  Object that represents the VirtualHost whilst the VirtualHostNode is in the replica role.  The
  real virtualhost will be elsewhere in the group.
 */
@ManagedObject( category = false, type = "BDB_HA_REPLICA", register = false )
public class BDBHAReplicaVirtualHostImpl extends AbstractNonConnectionAcceptingVirtualHost<BDBHAReplicaVirtualHostImpl>
        implements BDBHAReplicaVirtualHost<BDBHAReplicaVirtualHostImpl>
{

    @ManagedObjectFactoryConstructor(conditionallyAvailable = true, condition = "org.apache.qpid.server.JECheck#isAvailable()")
    public BDBHAReplicaVirtualHostImpl(final Map<String, Object> attributes, VirtualHostNode<?> virtualHostNode)
    {
        super(virtualHostNode, attributes);

    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        throwUnsupported();
    }

}
