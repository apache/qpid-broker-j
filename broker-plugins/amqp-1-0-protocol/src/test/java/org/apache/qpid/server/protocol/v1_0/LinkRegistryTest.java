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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class LinkRegistryTest extends UnitTestBase
{
    public static final Pattern ANY = Pattern.compile(".*");
    private LinkRegistryImpl _linkRegistry;

    @Before
    public void setUp() throws Exception
    {
        final QueueManagingVirtualHost virtualHost = mock(QueueManagingVirtualHost.class);
        _linkRegistry = new LinkRegistryImpl(virtualHost);
    }

    @Test
    public void testGetSendingLink() throws Exception
    {
        String remoteContainerId = "testRemoteContainerId";
        String linkName = "testLinkName";
        LinkModel link = _linkRegistry.getSendingLink(remoteContainerId, linkName);
        assertNotNull("LinkRegistryModel#getSendingLink should always return an object", link);
        LinkModel link2 = _linkRegistry.getSendingLink(remoteContainerId, linkName);
        assertNotNull("LinkRegistryModel#getSendingLink should always return an object", link2);
        assertSame("Two calls to LinkRegistryModel#getSendingLink should return the same object", link, link2);
    }

    @Test
    public void testGetReceivingLink() throws Exception
    {
        String remoteContainerId = "testRemoteContainerId";
        String linkName = "testLinkName";
        LinkModel link = _linkRegistry.getReceivingLink(remoteContainerId, linkName);
        assertNotNull("LinkRegistryModel#getReceivingLink should always return an object", link);
        LinkModel link2 = _linkRegistry.getReceivingLink(remoteContainerId, linkName);
        assertNotNull("LinkRegistryModel#getReceivingLink should always return an object", link2);
        assertSame("Two calls to LinkRegistryModel#getReceivingLink should return the same object", link, link2);
    }

    @Test
    public void testPurgeSendingLinksFromRegistryWithEmptyRegistry() throws Exception
    {
        _linkRegistry.purgeSendingLinks(ANY, ANY);
    }

    @Test
    public void testPurgeSendingLinksExactMatch() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeSendingLinksRegEx() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("test.*Id"), Pattern.compile("testLink.*"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeSendingLinksNotMatchingRegEx() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("Foo.*"), Pattern.compile(".*bar"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 1);
    }

    @Test
    public void testPurgeSendingLinksDoesNotRemoveReceivingLink() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeSendingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 1);
        assertEquals((long) dump.getContainers().get("testContainerId").getReceivingLinks().size(), (long) 1);
        assertEquals((long) dump.getContainers().get("testContainerId").getSendingLinks().size(), (long) 0);
    }

    @Test
    public void testPurgeReceivingLinksFromRegistryWithEmptyRegistry() throws Exception
    {
        _linkRegistry.purgeReceivingLinks(ANY, ANY);
    }

    @Test
    public void testPurgeReceivingLinksExactMatch() throws Exception
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeReceivingLinksRegEx() throws Exception
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("test.*Id"), Pattern.compile("testLink.*"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 0);
    }

    @Test
    public void testPurgeReceivingLinksNotMatchingRegEx() throws Exception
    {
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("Foo.*"), Pattern.compile(".*bar"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 1);
    }

    @Test
    public void testPurgeReceivingLinksDoesNotRemoveSendingLink() throws Exception
    {
        _linkRegistry.getSendingLink("testContainerId", "testLinkName");
        _linkRegistry.getReceivingLink("testContainerId", "testLinkName");
        _linkRegistry.purgeReceivingLinks(Pattern.compile("testContainerId"), Pattern.compile("testLinkName"));
        LinkRegistryImpl.LinkRegistryDump dump = _linkRegistry.dump();
        assertEquals((long) dump.getContainers().size(), (long) 1);
        assertEquals((long) dump.getContainers().get("testContainerId").getReceivingLinks().size(), (long) 0);
        assertEquals((long) dump.getContainers().get("testContainerId").getSendingLinks().size(), (long) 1);
    }
}
