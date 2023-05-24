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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
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
        _source.setFilter(Map.of(Symbol.valueOf("foo"), NoLocalFilter.INSTANCE));
        _source.setOutcomes(new Accepted().getSymbol());
        _source.setDynamicNodeProperties(Map.of(Symbol.valueOf("dynamicProperty"), "dynamicPropertyValue"));
        _source.setTimeout(new UnsignedInteger(1));

        _target.setTimeout(new UnsignedInteger(2));
        _target.setDynamicNodeProperties(Map.of(Symbol.valueOf("targetDynamicProperty"), "targetDynamicPropertyValue"));
        _target.setDynamic(Boolean.TRUE);
        _target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        _target.setAddress("bar");
        _target.setCapabilities(new Symbol[]{Symbol.getSymbol(CAPABILITY)});
        _target.setDurable(TerminusDurability.CONFIGURATION);
    }

    @AfterEach
    public void tearDown()
    {
        deleteLinkStore();
    }

    @Test
    public void openAndLoad()
    {
        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertTrue(links.isEmpty(), "Unexpected links");

        assertThrows(StoreException.class,
                () -> _linkStore.openAndLoad(new LinkStoreUpdaterImpl()),
                "Repeated open of already opened store should fail");

        final LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.saveLink(linkDefinition);
        _linkStore.close();

        links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals(1, links.size(), "Unexpected link number");
    }

    @Test
    public void close()
    {
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        _linkStore.close();

        assertThrows(StoreException.class, () ->
        {
            LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
            _linkStore.saveLink(linkDefinition);
        }, "Saving link with close store should fail");
    }

    @Test
    public void saveLink()
    {
        final LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        _linkStore.saveLink(linkDefinition);
        _linkStore.close();

        assertThrows(StoreException.class,
                () -> _linkStore.saveLink(createLinkDefinition("2", "test2")),
                "Save on unopened database should fail");

        final Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals(1, links.size(), "Unexpected link number");

        final LinkDefinition<Source, Target> recoveredLink = links.iterator().next();

        assertEquals(linkDefinition.getName(), recoveredLink.getName(), "Unexpected link name");
        assertEquals(linkDefinition.getRemoteContainerId(), recoveredLink.getRemoteContainerId(),
                "Unexpected container id");
        assertEquals(linkDefinition.getRole(), recoveredLink.getRole(), "Unexpected role");
        assertEquals(linkDefinition.getSource(), recoveredLink.getSource(), "Unexpected source");
        assertEquals(linkDefinition.getTarget(), recoveredLink.getTarget(), "Unexpected target");
    }

    @Test
    public void deleteLink()
    {
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());

        final LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.saveLink(linkDefinition);

        final LinkDefinition<Source, Target> linkDefinition2 = createLinkDefinition("2", "test2");
        _linkStore.saveLink(linkDefinition2);

        _linkStore.deleteLink(linkDefinition2);
        _linkStore.close();

        assertThrows(StoreException.class,
                () -> _linkStore.deleteLink(linkDefinition),
                "Delete on unopened database should fail");

        final Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals(1, links.size(), "Unexpected link number");

        final LinkDefinition<Source, Target> recoveredLink = links.iterator().next();

        assertEquals(linkDefinition.getName(), recoveredLink.getName(), "Unexpected link name");
        assertEquals(linkDefinition.getRemoteContainerId(), recoveredLink.getRemoteContainerId(),
                "Unexpected container id");
        assertEquals(linkDefinition.getRole(), recoveredLink.getRole(), "Unexpected role");
        assertEquals(linkDefinition.getSource(), recoveredLink.getSource(), "Unexpected source");
        assertEquals(linkDefinition.getTarget(), recoveredLink.getTarget(), "Unexpected target");
    }

    @Test
    public void delete()
    {
        _linkStore.openAndLoad(new LinkStoreUpdaterImpl());

        final LinkDefinition<Source, Target> linkDefinition = createLinkDefinition("1", "test");
        _linkStore.saveLink(linkDefinition);

        final LinkDefinition<Source, Target> linkDefinition2 = createLinkDefinition("2", "test2");
        _linkStore.saveLink(linkDefinition2);

        _linkStore.close();
        Collection<LinkDefinition<Source, Target>> links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals(2, links.size(), "Unexpected link number");

        _linkStore.delete();

        links = _linkStore.openAndLoad(new LinkStoreUpdaterImpl());
        assertEquals(0, links.size(), "Unexpected link number");
    }

    protected abstract LinkStore createLinkStore();

    protected abstract void deleteLinkStore();

    private LinkDefinitionImpl<Source, Target> createLinkDefinition(final String remoteContainerId, final String linkName)
    {
        return new LinkDefinitionImpl<>(remoteContainerId, linkName, Role.RECEIVER, _source, _target);
    }
}
