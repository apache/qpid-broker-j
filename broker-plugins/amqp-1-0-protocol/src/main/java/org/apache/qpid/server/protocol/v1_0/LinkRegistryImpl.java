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
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.LinkRegistry;

public class LinkRegistryImpl implements LinkRegistry
{
    private final Map<String, Map<String, SendingLink_1_0>> _sendingLinkRegistry = new HashMap<>();
    private final Map<String, Map<String, StandardReceivingLink_1_0>> _receivingLinkRegistry = new HashMap<>();
    private final Map<String, Map<String, TxnCoordinatorReceivingLink_1_0>> _coordinatorLinkRegistry = new HashMap<>();
    private final NamedAddressSpace _addressSpace;

    LinkRegistryImpl(final NamedAddressSpace addressSpace)
    {
        _addressSpace = addressSpace;
    }

    @Override
    public synchronized <T extends LinkModel> T getLink(final String remoteContainerId, final String linkName, final Class<T> type)
    {
        if (SendingLink_1_0.class.equals(type))
        {
            return (T) getSendingLink(remoteContainerId, linkName);
        }
        else if (StandardReceivingLink_1_0.class.equals(type))
        {
            return (T) getReceivingLink(remoteContainerId, linkName);
        }
        else if (TxnCoordinatorReceivingLink_1_0.class.equals(type))
        {
            return (T) getCoordinatorLink(remoteContainerId, linkName);
        }
        else
        {
            throw new ConnectionScopedRuntimeException(String.format("Unsupported link type: '%s'", type.getSimpleName()));
        }
    }

    private TxnCoordinatorReceivingLink_1_0 getCoordinatorLink(final String remoteContainerId, final String linkName)
    {
        Map<String, TxnCoordinatorReceivingLink_1_0> containerRegistry = _coordinatorLinkRegistry.get(remoteContainerId);
        if (containerRegistry == null)
        {
            containerRegistry = new HashMap<>();
            _coordinatorLinkRegistry.put(remoteContainerId, containerRegistry);
        }
        TxnCoordinatorReceivingLink_1_0 link = containerRegistry.get(linkName);
        if (link == null)
        {
            link = new TxnCoordinatorReceivingLink_1_0(linkName);
            containerRegistry.put(linkName, link);
        }
        return link;
    }

    private SendingLink_1_0 getSendingLink(final String remoteContainerId, final String linkName)
    {
        Map<String, SendingLink_1_0> containerRegistry = _sendingLinkRegistry.get(remoteContainerId);
        if (containerRegistry == null)
        {
            containerRegistry = new HashMap<>();
            _sendingLinkRegistry.put(remoteContainerId, containerRegistry);
        }
        SendingLink_1_0 link = containerRegistry.get(linkName);
        if (link == null)
        {
            link = new SendingLink_1_0(linkName);
            containerRegistry.put(linkName, link);
        }
        return link;
    }

    private StandardReceivingLink_1_0 getReceivingLink(final String remoteContainerId, final String linkName)
    {
        Map<String, StandardReceivingLink_1_0> containerRegistry = _receivingLinkRegistry.get(remoteContainerId);
        if (containerRegistry == null)
        {
            containerRegistry = new HashMap<>();
            _receivingLinkRegistry.put(remoteContainerId, containerRegistry);
        }
        StandardReceivingLink_1_0 link = containerRegistry.get(linkName);
        if (link == null)
        {
            link = new StandardReceivingLink_1_0(linkName);
            containerRegistry.put(linkName, link);
        }
        return link;
    }
}
