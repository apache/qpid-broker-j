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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreFactory;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUpdaterImpl;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class LinkRegistryImpl implements LinkRegistry
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkRegistryImpl.class);
    private final ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> _sendingLinkRegistry = new ConcurrentHashMap<>();
    private final ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> _receivingLinkRegistry = new ConcurrentHashMap<>();

    private final LinkStore _linkStore;

    LinkRegistryImpl(final NamedAddressSpace addressSpace)
    {
        LinkStoreFactory storeFactory = null;
        Iterable<LinkStoreFactory> linkStoreFactories = new QpidServiceLoader().instancesOf(LinkStoreFactory.class);
        for (LinkStoreFactory linkStoreFactory : linkStoreFactories)
        {
            if (linkStoreFactory.supports(addressSpace)
                && (storeFactory == null || storeFactory.getPriority() < linkStoreFactory.getPriority()))
            {
                storeFactory = linkStoreFactory;
            }
        }
        if (storeFactory == null)
        {
            throw new ServerScopedRuntimeException("Cannot find suitable link store");
        }
        _linkStore = storeFactory.create(addressSpace);

    }

    @Override
    public Link_1_0<? extends BaseSource, ? extends BaseTarget> getSendingLink(final String remoteContainerId, final String linkName)
    {
        return getLinkFromRegistry(remoteContainerId, linkName, _sendingLinkRegistry, Role.SENDER);
    }

    @Override
    public Link_1_0<? extends BaseSource, ? extends BaseTarget> getReceivingLink(final String remoteContainerId, final String linkName)
    {
        return getLinkFromRegistry(remoteContainerId, linkName, _receivingLinkRegistry, Role.RECEIVER);
    }

    @Override
    public void linkClosed(final Link_1_0<? extends BaseSource, ? extends BaseTarget>  link)
    {
        ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> linkRegistry =
                getLinkRegistry(link.getRole());
        linkRegistry.remove(new LinkKey(link));
        if (isDurableLink(link))
        {
            _linkStore.deleteLink((Link_1_0<Source, Target>) link);
        }
    }

    @Override
    public void linkChanged(final Link_1_0<? extends BaseSource, ? extends BaseTarget> link)
    {
        getLinkRegistry(link.getRole()).putIfAbsent(new LinkKey(link), link);
        if (isDurableLink(link))
        {
            _linkStore.saveLink((Link_1_0<Source, Target>) link);
        }
    }

    @Override
    public TerminusDurability getHighestSupportedTerminusDurability()
    {
        TerminusDurability supportedTerminusDurability = _linkStore.getHighestSupportedTerminusDurability();
        return supportedTerminusDurability == TerminusDurability.UNSETTLED_STATE ? TerminusDurability.CONFIGURATION : supportedTerminusDurability;
    }

    @Override
    public Collection<Link_1_0<? extends BaseSource, ? extends BaseTarget>> findSendingLinks(final String linkName)
    {
        return _sendingLinkRegistry.entrySet()
                                   .stream()
                                   .filter(e -> e.getKey().getLinkName().equals(linkName))
                                   .map(Map.Entry::getValue)
                                   .collect(Collectors.toList());
    }

    @Override
    public void open()
    {
        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        for(LinkDefinition<? extends BaseSource, ? extends BaseTarget> link: links)
        {
            ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> linkRegistry = getLinkRegistry(link.getRole());
            linkRegistry.put(new LinkKey(link), new LinkImpl<>(link, this));
        }
    }

    @Override
    public void close()
    {
        _linkStore.close();
    }

    @Override
    public void delete()
    {
        _linkStore.delete();
    }

    private boolean isDurableLink(final Link_1_0<? extends BaseSource, ? extends BaseTarget> link)
    {
        return (link.getRole() == Role.SENDER && link.getSource() instanceof Source
                && ((Source) link.getSource()).getDurable() != TerminusDurability.NONE)
               || (link.getRole() == Role.RECEIVER && link.getTarget() instanceof Target
                   && ((Target) link.getTarget()).getDurable() != TerminusDurability.NONE);
    }

    private Link_1_0<? extends BaseSource, ? extends BaseTarget> getLinkFromRegistry(final String remoteContainerId,
                                                                                     final String linkName,
                                                                                     final ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> linkRegistry,
                                                                                     final Role role)
    {
        LinkKey linkKey = new LinkKey(remoteContainerId, linkName, role);
        Link_1_0<? extends BaseSource, ? extends BaseTarget> newLink = new LinkImpl(remoteContainerId, linkName, role, this);
        Link_1_0<? extends BaseSource, ? extends BaseTarget> link = linkRegistry.putIfAbsent(linkKey, newLink);
        if (link == null)
        {
            link = newLink;
        }
        return link;
    }

    private ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> getLinkRegistry(final Role role)
    {
        ConcurrentMap<LinkKey, Link_1_0<? extends BaseSource, ? extends BaseTarget>> linkRegistry;
        if (Role.SENDER == role)
        {
            linkRegistry = _sendingLinkRegistry;

        }
        else if (Role.RECEIVER == role)
        {
            linkRegistry = _receivingLinkRegistry;
        }
        else
        {
            throw new ServerScopedRuntimeException(String.format("Unsupported link role %s", role));
        }

        return linkRegistry;
    }
}
