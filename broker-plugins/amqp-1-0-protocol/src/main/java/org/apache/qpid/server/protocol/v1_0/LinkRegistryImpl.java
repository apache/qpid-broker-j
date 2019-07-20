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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
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

public class LinkRegistryImpl<S extends BaseSource, T extends BaseTarget> implements LinkRegistry<S, T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkRegistryImpl.class);
    private final ConcurrentMap<LinkKey, Link_1_0<S, T>> _sendingLinkRegistry = new ConcurrentHashMap<>();
    private final ConcurrentMap<LinkKey, Link_1_0<S, T>> _receivingLinkRegistry = new ConcurrentHashMap<>();

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
    public Link_1_0<S, T> getSendingLink(final String remoteContainerId, final String linkName)
    {
        return getLinkFromRegistry(remoteContainerId, linkName, _sendingLinkRegistry, Role.SENDER);
    }

    @Override
    public Link_1_0<S, T> getReceivingLink(final String remoteContainerId, final String linkName)
    {
        return getLinkFromRegistry(remoteContainerId, linkName, _receivingLinkRegistry, Role.RECEIVER);
    }

    @Override
    public void linkClosed(final Link_1_0<S, T> link)
    {
        ConcurrentMap<LinkKey, Link_1_0<S, T>> linkRegistry = getLinkRegistry(link.getRole());
        linkRegistry.remove(new LinkKey(link));
        if (isDurableLink(link))
        {
            _linkStore.deleteLink((Link_1_0<Source, Target>) link);
        }
    }

    @Override
    public void linkChanged(final Link_1_0<S,T> link)
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
    public Collection<Link_1_0<S,T>> findSendingLinks(final Pattern containerIdPattern,
                                                                                             final Pattern linkNamePattern)
    {
        return _sendingLinkRegistry.entrySet()
                                   .stream()
                                   .filter(e -> containerIdPattern.matcher(e.getKey().getRemoteContainerId()).matches()
                                                && linkNamePattern.matcher(e.getKey().getLinkName()).matches())
                                   .map(Map.Entry::getValue)
                                   .collect(Collectors.toList());
    }


    @Override
    public void visitSendingLinks(final LinkVisitor<Link_1_0<S,T>> visitor)
    {
        visitLinks(_sendingLinkRegistry.values(), visitor);
    }

    private void visitLinks(final Collection<Link_1_0<S, T>> links,
                            final LinkVisitor<Link_1_0<S, T>> visitor)
    {
        for (Link_1_0<S, T> link : links)
        {
            if (visitor.visit(link))
            {
                break;
            }
        }
    }

    @Override
    public void purgeSendingLinks(final Pattern containerIdPattern, final Pattern linkNamePattern)
    {
        purgeLinks(_sendingLinkRegistry, containerIdPattern, linkNamePattern);
    }

    @Override
    public void purgeReceivingLinks(final Pattern containerIdPattern, final Pattern linkNamePattern)
    {
        purgeLinks(_receivingLinkRegistry, containerIdPattern, linkNamePattern);
    }

    @Override
    public void open()
    {
        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        for(LinkDefinition<Source, Target> link: links)
        {
            ConcurrentMap<LinkKey, Link_1_0<S,T>> linkRegistry = getLinkRegistry(link.getRole());
            LinkDefinition<S, T> definition = (LinkDefinition<S, T>) link;
            linkRegistry.put(new LinkKey(link), new LinkImpl<>(definition, this));
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

    private Link_1_0<S, T> getLinkFromRegistry(final String remoteContainerId,
                                               final String linkName,
                                               final ConcurrentMap<LinkKey, Link_1_0<S, T>> linkRegistry,
                                               final Role role)
    {
        LinkKey linkKey = new LinkKey(remoteContainerId, linkName, role);
        Link_1_0<S, T> newLink = new LinkImpl<>(remoteContainerId, linkName, role, this);
        Link_1_0<S, T> link = linkRegistry.putIfAbsent(linkKey, newLink);
        if (link == null)
        {
            link = newLink;
        }
        return link;
    }

    private void purgeLinks(final ConcurrentMap<LinkKey, Link_1_0<S,T>> linkRegistry,
                            final Pattern containerIdPattern, final Pattern linkNamePattern)
    {
        linkRegistry.entrySet()
                    .stream()
                    .filter(e -> containerIdPattern.matcher(e.getKey().getRemoteContainerId()).matches()
                                 && linkNamePattern.matcher(e.getKey().getLinkName()).matches())
                    .forEach(e -> e.getValue().linkClosed());
    }

    private ConcurrentMap<LinkKey, Link_1_0<S,T>> getLinkRegistry(final Role role)
    {
        ConcurrentMap<LinkKey, Link_1_0<S,T>> linkRegistry;
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

    @Override
    public LinkRegistryDump dump()
    {
        LinkRegistryDump dump = new LinkRegistryDump();
        dumpRegistry(_sendingLinkRegistry, dump);
        dumpRegistry(_receivingLinkRegistry, dump);
        return dump;
    }

    private void dumpRegistry(ConcurrentMap<LinkKey, Link_1_0<S, T>> registry,
                              LinkRegistryDump dump)
    {
        for (Map.Entry<LinkKey, Link_1_0<S,T>> entry : registry.entrySet())
        {
            LinkKey linkKey = entry.getKey();
            LinkRegistryDump.ContainerDump containerLinks =
                    dump._containers.computeIfAbsent(linkKey.getRemoteContainerId(), k -> new LinkRegistryDump.ContainerDump());

            LinkRegistryDump.ContainerDump.LinkDump linkDump = new LinkRegistryDump.ContainerDump.LinkDump();
            linkDump._source = String.valueOf(entry.getValue().getSource());
            linkDump._target = String.valueOf(entry.getValue().getTarget());
            if (linkKey.getRole().equals(Role.SENDER))
            {
                containerLinks._sendingLinks.put(linkKey.getLinkName(), linkDump);
            }
            else
            {
                containerLinks._receivingLinks.put(linkKey.getLinkName(), linkDump);
            }
        }
    }

    static class LinkRegistryDump
    {
        static class ContainerDump
        {
            public static class LinkDump
            {
                private String _source;
                private String _target;

                public String getSource()
                {
                    return _source;
                }

                public String getTarget()
                {
                    return _target;
                }
            }

            private Map<String, LinkDump> _sendingLinks = new LinkedHashMap<>();
            private Map<String, LinkDump> _receivingLinks = new LinkedHashMap<>();


            public Map<String, LinkDump> getSendingLinks()
            {
                return Collections.unmodifiableMap(_sendingLinks);
            }

            public Map<String, LinkDump> getReceivingLinks()
            {
                return Collections.unmodifiableMap(_receivingLinks);
            }
        }

        private Map<String, ContainerDump> _containers = new LinkedHashMap<>();

        public Map<String, ContainerDump> getContainers()
        {
            return Collections.unmodifiableMap(_containers);
        }
    }
}
