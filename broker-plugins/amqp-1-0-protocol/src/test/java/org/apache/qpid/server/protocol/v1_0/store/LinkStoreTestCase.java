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
 *
 */

package org.apache.qpid.server.protocol.v1_0.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.LinkDefinition;
import org.apache.qpid.server.protocol.v1_0.LinkDefinitionImpl;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NoLocalFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

public abstract class LinkStoreTestCase extends UnitTestBase
{
    private static final String ADDRESS = "amqp.direct/test";
    private static final String CAPABILITY = "test.capability";

    private LinkStore _linkStore;
    private Source _source;
    private Target _target;

    @Before
    public void setUp() throws Exception
    {
        _linkStore = createLinkStore();

        _source = new Source();
        _target = new Target();

        _source.setAddress(ADDRESS);
        _source.setCapabilities(new Symbol[]{Symbol.getSymbol(CAPABILITY)});
        _source.setDefaultOutcome(new Rejected());
        _source.setDistributionMode(StdDistMode.COPY);
        _source.setDurable(TerminusDurability.UNSETTLED_STATE);
        _source.setDynamic(Boolean.TRUE);
        _source.setExpiryPolicy(TerminusExpiryPolicy.CONNECTION_CLOSE);
        _source.setFilter(Collections.singletonMap(Symbol.valueOf("foo"), NoLocalFilter.INSTANCE));
        _source.setOutcomes(new Accepted().getSymbol());
        _source.setDynamicNodeProperties(Collections.singletonMap(Symbol.valueOf("dynamicProperty"),
                                                                  "dynamicPropertyValue"));
        _source.setTimeout(new UnsignedInteger(1));

        _target.setTimeout(new UnsignedInteger(2));
        _target.setDynamicNodeProperties(Collections.singletonMap(Symbol.valueOf("targetDynamicProperty"),
                                                                  "targetDynamicPropertyValue"));
        _target.setDynamic(Boolean.TRUE);
        _target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        _target.setAddress("bar");
        _target.setCapabilities(new Symbol[]{Symbol.getSymbol(CAPABILITY)});
        _target.setDurable(TerminusDurability.CONFIGURATION);
    }

    @After
    public void tearDown() throws Exception
    {
        deleteLinkStore();
    }

    @Test
    public void testOpenAndLoad() throws Exception
    {
        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertTrue("Unexpected links", links.isEmpty());

        try
        {
            _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
            fail("Repeated open of already opened store should fail");
        }
        catch (StoreException e)
        {
            // pass
        }

        LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.saveLink(linkDefinition);
        _linkStore.close();

        links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals("Unexpected link number", 1, links.size());
    }


    @Test
    public void testClose() throws Exception
    {
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());

        _linkStore.close();
        try
        {
            LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
            _linkStore.saveLink(linkDefinition);
            fail("Saving link with close store should fail");
        }
        catch (StoreException e)
        {
            // pass
        }
    }

    @Test
    public void testSaveLink() throws Exception
    {

        LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        _linkStore.saveLink(linkDefinition);
        _linkStore.close();

        try
        {
            _linkStore.saveLink(createLinkDefinition("2", "test2"));
            fail("Save on unopened database should fail");
        }
        catch (StoreException e)
        {
            // pass
        }

        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals("Unexpected link number", 1, links.size());

        LinkDefinition<Source, Target> recoveredLink = links.iterator().next();

        assertEquals("Unexpected link name", linkDefinition.getName(), recoveredLink.getName());
        assertEquals("Unexpected container id", linkDefinition.getRemoteContainerId(), recoveredLink.getRemoteContainerId());
        assertEquals("Unexpected role", linkDefinition.getRole(), recoveredLink.getRole());
        assertEquals("Unexpected source", linkDefinition.getSource(), recoveredLink.getSource());
        assertEquals("Unexpected target", linkDefinition.getTarget(), recoveredLink.getTarget());
    }

    @Test
    public void testDeleteLink() throws Exception
    {
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());

        LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.saveLink(linkDefinition);

        LinkDefinition<Source, Target> linkDefinition2 = createLinkDefinition("2", "test2");
        _linkStore.saveLink(linkDefinition2);

        _linkStore.deleteLink(linkDefinition2);
        _linkStore.close();

        try
        {
            _linkStore.deleteLink(linkDefinition);
            fail("Delete on unopened database should fail");
        }
        catch (StoreException e)
        {
            // pass
        }

        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals("Unexpected link number", 1, links.size());

        LinkDefinition<Source, Target> recoveredLink = links.iterator().next();

        assertEquals("Unexpected link name", linkDefinition.getName(), recoveredLink.getName());
        assertEquals("Unexpected container id", linkDefinition.getRemoteContainerId(), recoveredLink.getRemoteContainerId());
        assertEquals("Unexpected role", linkDefinition.getRole(), recoveredLink.getRole());
        assertEquals("Unexpected source", linkDefinition.getSource(), recoveredLink.getSource());
        assertEquals("Unexpected target", linkDefinition.getTarget(), recoveredLink.getTarget());
    }

    @Test
    public void testDelete() throws Exception
    {
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());

        LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.saveLink(linkDefinition);

        LinkDefinition<Source, Target> linkDefinition2 = createLinkDefinition("2", "test2");
        _linkStore.saveLink(linkDefinition2);

        _linkStore.close();
        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals("Unexpected link number", 2, links.size());

        _linkStore.delete();

        links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals("Unexpected link number", 0, links.size());
    }

    protected abstract LinkStore createLinkStore();

    protected abstract void deleteLinkStore();

    private LinkDefinitionImpl<Source, Target> createLinkDefinition(final String remoteContainerId, final String linkName)
    {
        return new LinkDefinitionImpl(remoteContainerId, linkName, Role.RECEIVER, _source, _target);
    }
}
