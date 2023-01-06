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
package org.apache.qpid.server.virtualhostalias;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PatternMatchingAlias;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostAliasTest extends UnitTestBase
{
    private final Map<String, VirtualHost<?>> _vhosts = new HashMap<>();
    private AmqpPort<?> _port;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception
    {
        final Broker<?> broker = BrokerTestHelper.createNewBrokerMock();
        final AuthenticationProvider<?> dummyAuthProvider = mock(AuthenticationProvider.class);
        when(dummyAuthProvider.getName()).thenReturn("dummy");
        when(dummyAuthProvider.getId()).thenReturn(randomUUID());
        when(dummyAuthProvider.getMechanisms()).thenReturn(Collections.singletonList("PLAIN"));
        when(broker.getChildren(eq(AuthenticationProvider.class))).thenReturn(Set.of(dummyAuthProvider));
        for (final String name : new String[] { "red", "blue", "purple", "black" })
        {
            final boolean defaultVHN = "black".equals(name);
            final VirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(name, broker, defaultVHN, this);
            final VirtualHostNode<?> vhn = (VirtualHostNode<?>) virtualHost.getParent();
            assertNotSame(vhn.getName(), virtualHost.getName());
            _vhosts.put(name, virtualHost);

            if (defaultVHN)
            {
                when(broker.findDefautVirtualHostNode()).thenReturn(vhn);
            }
        }
        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();

        final Map<String, Object> attributes = Map.of(Port.NAME, getTestName(),
                Port.PORT, 0,
                Port.AUTHENTICATION_PROVIDER, "dummy",
                Port.TYPE, "AMQP");
        _port = (AmqpPort<?>) objectFactory.create(Port.class, attributes, broker);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _port.close();
    }

    @Test
    public void testDefaultAliases_VirtualHostNameAlias()
    {
        NamedAddressSpace addressSpace = _port.getAddressSpace("red");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("red"), addressSpace);

        addressSpace = _port.getAddressSpace("blue");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("blue"), addressSpace);

        addressSpace = _port.getAddressSpace("orange!");

        assertNull(addressSpace);
    }

    @Test
    public void testDefaultAliases_DefaultVirtualHostAlias()
    {
        // test the default vhost resolution
        final NamedAddressSpace addressSpace = _port.getAddressSpace("");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("black"), addressSpace);
    }

    @Test
    public void testDefaultAliases_HostNameAlias()
    {
        // 127.0.0.1 should always resolve and thus return the default vhost
        final NamedAddressSpace addressSpace = _port.getAddressSpace("127.0.0.1");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("black"), addressSpace);
    }

    @Test
    public void testPatternMatching()
    {
        final Map<String, Object> attributes = Map.of(VirtualHostAlias.NAME, "matcher",
                VirtualHostAlias.TYPE, PatternMatchingAlias.TYPE_NAME,
                PatternMatchingAlias.PATTERN, "orange|pink.*",
                PatternMatchingAlias.VIRTUAL_HOST_NODE, _vhosts.get("purple").getParent());
        _port.createChild(VirtualHostAlias.class, attributes);

        NamedAddressSpace addressSpace = _port.getAddressSpace("orange");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("purple"), addressSpace);

        addressSpace = _port.getAddressSpace("pink");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("purple"), addressSpace);

        addressSpace = _port.getAddressSpace("pinker");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("purple"), addressSpace);

        addressSpace = _port.getAddressSpace("o.*");

        assertNull(addressSpace);
    }

    @Test
    public void testPriority()
    {
        NamedAddressSpace addressSpace = _port.getAddressSpace("blue");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("blue"), addressSpace);

        addressSpace = _port.getAddressSpace("black");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("black"), addressSpace);

        Map<String, Object> attributes = Map.of(VirtualHostAlias.NAME, "matcher10",
                VirtualHostAlias.TYPE, PatternMatchingAlias.TYPE_NAME,
                VirtualHostAlias.PRIORITY, 10,
                PatternMatchingAlias.PATTERN, "bl.*",
                PatternMatchingAlias.VIRTUAL_HOST_NODE, _vhosts.get("purple").getParent());
        _port.createChild(VirtualHostAlias.class, attributes);

        addressSpace = _port.getAddressSpace("blue");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("purple"), addressSpace);

        addressSpace = _port.getAddressSpace("black");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("purple"), addressSpace);

        attributes = Map.of(VirtualHostAlias.NAME, "matcher5",
                VirtualHostAlias.TYPE, PatternMatchingAlias.TYPE_NAME,
                VirtualHostAlias.PRIORITY, 5,
                PatternMatchingAlias.PATTERN, ".*u.*",
                PatternMatchingAlias.VIRTUAL_HOST_NODE, _vhosts.get("red").getParent());
        _port.createChild(VirtualHostAlias.class, attributes);

        addressSpace = _port.getAddressSpace("blue");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("red"), addressSpace);

        addressSpace = _port.getAddressSpace("black");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("purple"), addressSpace);

        addressSpace = _port.getAddressSpace("purple");

        assertNotNull(addressSpace);
        assertEquals(_vhosts.get("red"), addressSpace);
    }
}
