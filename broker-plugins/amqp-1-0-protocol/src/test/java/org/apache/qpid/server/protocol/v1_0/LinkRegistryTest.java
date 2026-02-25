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

package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class LinkRegistryTest extends UnitTestBase
{
    static final Pattern ANY = Pattern.compile(".*");

    private QueueManagingVirtualHost<?> _virtualHost;
    private LinkRegistryImpl<?, ?> _linkRegistry;

    @BeforeAll
    void beforeAll()
    {
        _virtualHost = mock(QueueManagingVirtualHost.class);
    }

    @BeforeEach
    void beforeEach()
    {
        _linkRegistry = new LinkRegistryImpl<>(_virtualHost);
    }

    @Test
    void getSendingLink()
    {
        final String remoteContainerId = "testRemoteContainerId";
        final String linkName = "testLinkName";
        final LinkModel link = _linkRegistry.getSendingLink(remoteContainerId, linkName);
        assertNotNull(link, "LinkRegistryModel#getSendingLink should always return an object");
        final LinkModel link2 = _linkRegistry.getSendingLink(remoteContainerId, linkName);
        assertNotNull(link2, "LinkRegistryModel#getSendingLink should always return an object");
        assertSame(link, link2, "Two calls to LinkRegistryModel#getSendingLink should return the same object");
    }

    @Test
    void getReceivingLink()
    {
        final String remoteContainerId = "testRemoteContainerId";
        final String linkName = "testLinkName";
        final LinkModel link = _linkRegistry.getReceivingLink(remoteContainerId, linkName);
        assertNotNull(link, "LinkRegistryModel#getReceivingLink should always return an object");
        final LinkModel link2 = _linkRegistry.getReceivingLink(remoteContainerId, linkName);
        assertNotNull(link2, "LinkRegistryModel#getReceivingLink should always return an object");
        assertSame(link, link2, "Two calls to LinkRegistryModel#getReceivingLink should return the same object");
    }

    @Test
    void purgeSendingLinksFromRegistryWithEmptyRegistry()
    {
        _linkRegistry.purgeSendingLinks(ANY, ANY);
    }

    @Test
    void purgeSendingLinksExactMatch()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    void purgeSendingLinksRegEx()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("test.*Id"), Pattern.compile("testLink.*"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    void purgeSendingLinksNotMatchingRegEx()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("Foo.*"), Pattern.compile(".*bar"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
    }

    @Test
    void purgeSendingLinksDoesNotRemoveReceivingLink()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
        assertEquals(dump.getContainers().get("testContainerId").getReceivingLinks().size(), (long) 1);
        assertEquals(dump.getContainers().get("testContainerId").getSendingLinks().size(), (long) 0);
    }

    @Test
    void purgeReceivingLinksFromRegistryWithEmptyRegistry()
    {
        _linkRegistry.purgeReceivingLinks(ANY, ANY);
    }

    @Test
    void purgeReceivingLinksExactMatch()
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    void purgeReceivingLinksRegEx()
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("test.*Id"), Pattern.compile("testLink.*"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    void purgeReceivingLinksNotMatchingRegEx()
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("Foo.*"), Pattern.compile(".*bar"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
    }

    @Test
    void purgeReceivingLinksDoesNotRemoveSendingLink()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
        assertEquals(dump.getContainers().get("testContainerId").getReceivingLinks().size(), (long) 0);
        assertEquals(dump.getContainers().get("testContainerId").getSendingLinks().size(), (long) 1);
    }

    @Test
    void dump()
    {
        _linkRegistry.getSendingLink("testContainerId1", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId2", "testLinkName");
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();

        assertNotNull(dump);
        final Map<String, LinkRegistryImpl.LinkRegistryDump.ContainerDump> containers = dump.getContainers();

        assertNotNull(containers);
        assertEquals(2, containers.size());

        final LinkRegistryImpl.LinkRegistryDump.ContainerDump container1 = containers.get("testContainerId1");
        final LinkRegistryImpl.LinkRegistryDump.ContainerDump container2 = containers.get("testContainerId2");

        assertNotNull(container1);
        assertNotNull(container2);

        assertEquals(0, container1.getReceivingLinks().size());
        assertEquals(1, container1.getSendingLinks().size());
        assertEquals(1, container2.getReceivingLinks().size());
        assertEquals(0, container2.getSendingLinks().size());

        final LinkRegistryImpl.LinkRegistryDump.ContainerDump.LinkDump link1 =
                container1.getSendingLinks().get("testLinkName");

        final LinkRegistryImpl.LinkRegistryDump.ContainerDump.LinkDump link2 =
                container2.getReceivingLinks().get("testLinkName");

        assertNotNull(link1);
        assertNotNull(link2);
    }

    @Test
    void dumpIsSerializable() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId1", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId2", "testLinkName");
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        final ObjectMapper objectMapper = new ObjectMapper();
        final String data = objectMapper.writeValueAsString(dump);

        assertNotNull(data);
    }
}
