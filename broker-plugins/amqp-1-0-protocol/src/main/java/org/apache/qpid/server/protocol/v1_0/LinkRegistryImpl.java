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
package org.apache.qpid.server.protocol.v1_0;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.virtualhost.LinkRegistry;

public class LinkRegistryImpl implements LinkRegistry
{
    private final Map<String, Map<String, Link_1_0>> _sendingLinkRegistry = new HashMap<>();
    private final Map<String, Map<String, Link_1_0>> _receivingLinkRegistry = new HashMap<>();

    private final NamedAddressSpace _addressSpace;

    LinkRegistryImpl(final NamedAddressSpace addressSpace)
    {
        _addressSpace = addressSpace;
    }

    public Link_1_0 getSendingLink(final String remoteContainerId, final String linkName)
    {
        return getLinkFromRegistry(remoteContainerId, linkName, _sendingLinkRegistry, Role.SENDER);
    }

    public Link_1_0 getReceivingLink(final String remoteContainerId, final String linkName)
    {
        return getLinkFromRegistry(remoteContainerId, linkName, _receivingLinkRegistry, Role.RECEIVER);
    }

    private Link_1_0 getLinkFromRegistry(final String remoteContainerId,
                                         final String linkName,
                                         final Map<String, Map<String, Link_1_0>> linkRegistry,
                                         final Role role)
    {
        Map<String, Link_1_0> containerRegistry = linkRegistry.get(remoteContainerId);
        if (containerRegistry == null)
        {
            containerRegistry = new HashMap<>();
            linkRegistry.put(remoteContainerId, containerRegistry);
        }
        Link_1_0 link = containerRegistry.get(linkName);
        if (link == null)
        {
            link = new LinkImpl(linkName, role);
            containerRegistry.put(linkName, link);
        }
        return link;
    }
}
