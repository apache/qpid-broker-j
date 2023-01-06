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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class LinkRegistryTest extends UnitTestBase
{
    public static final Pattern ANY = Pattern.compile(".*");
    private QueueManagingVirtualHost _virtualHost;
    private LinkRegistryImpl _linkRegistry;

    @BeforeAll
    public void beforeAll()
    {
        _virtualHost = mock(QueueManagingVirtualHost.class);
    }

    @BeforeEach
    public void beforeEach()
    {
        _linkRegistry = new LinkRegistryImpl(_virtualHost);
    }

    @Test
    public void testGetSendingLink()
    {
        String remoteContainerId = "testRemoteContainerId";
        String linkName = "testLinkName";
        LinkModel link = _linkRegistry.getSendingLink(remoteContainerId, linkName);
        assertNotNull(link, "LinkRegistryModel#getSendingLink should always return an object");
        LinkModel link2 = _linkRegistry.getSendingLink(remoteContainerId, linkName);
        assertNotNull(link2, "LinkRegistryModel#getSendingLink should always return an object");
        assertSame(link, link2, "Two calls to LinkRegistryModel#getSendingLink should return the same object");
    }

    @Test
    public void testGetReceivingLink()
    {
        String remoteContainerId = "testRemoteContainerId";
        String linkName = "testLinkName";
        LinkModel link = _linkRegistry.getReceivingLink(remoteContainerId, linkName);
        assertNotNull(link, "LinkRegistryModel#getReceivingLink should always return an object");
        LinkModel link2 = _linkRegistry.getReceivingLink(remoteContainerId, linkName);
        assertNotNull(link2, "LinkRegistryModel#getReceivingLink should always return an object");
        assertSame(link,
                              link2,
                              "Two calls to LinkRegistryModel#getReceivingLink should return the same object");
    }

    @Test
    public void testPurgeSendingLinksFromRegistryWithEmptyRegistry()
    {
        _linkRegistry.purgeSendingLinks(ANY, ANY);
    }

    @Test
    public void testPurgeSendingLinksExactMatch()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeSendingLinksRegEx()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("test.*Id"), Pattern.compile("testLink.*"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeSendingLinksNotMatchingRegEx()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("Foo.*"), Pattern.compile(".*bar"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
    }

    @Test
    public void testPurgeSendingLinksDoesNotRemoveReceivingLink()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
        assertEquals(dump.getContainers().get("testContainerId").getReceivingLinks().size(), (long) 1);
        assertEquals(dump.getContainers().get("testContainerId").getSendingLinks().size(), (long) 0);
    }

    @Test
    public void testPurgeReceivingLinksFromRegistryWithEmptyRegistry()
    {
        _linkRegistry.purgeReceivingLinks(ANY, ANY);
    }

    @Test
    public void testPurgeReceivingLinksExactMatch()
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeReceivingLinksRegEx()
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("test.*Id"), Pattern.compile("testLink.*"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeReceivingLinksNotMatchingRegEx()
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("Foo.*"), Pattern.compile(".*bar"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
    }

    @Test
    public void testPurgeReceivingLinksDoesNotRemoveSendingLink()
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals(dump.getContainers().size(), (long) 1);
        assertEquals(dump.getContainers().get("testContainerId").getReceivingLinks().size(), (long) 0);
        assertEquals(dump.getContainers().get("testContainerId").getSendingLinks().size(), (long) 1);
    }

    @Test
    public void testDump()
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
    public void testDumpIsSerializable() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId1", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId2", "testLinkName");
        final LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        final ObjectMapper objectMapper = new ObjectMapper();
        final String data = objectMapper.writeValueAsString(dump);

        assertNotNull(data);
    }

}
