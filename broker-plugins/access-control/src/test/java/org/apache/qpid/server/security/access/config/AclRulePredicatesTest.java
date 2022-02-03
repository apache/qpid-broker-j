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
package org.apache.qpid.server.security.access.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.security.access.config.predicates.TestFirewallRule;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.test.utils.UnitTestBase;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.collections.Sets;

import static org.apache.qpid.server.security.access.config.Property.ATTRIBUTES;
import static org.apache.qpid.server.security.access.config.Property.CLASS;
import static org.apache.qpid.server.security.access.config.Property.DURABLE;
import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.apache.qpid.server.security.access.config.Property.FROM_NETWORK;
import static org.apache.qpid.server.security.access.config.Property.NAME;
import static org.apache.qpid.server.security.access.config.Property.QUEUE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AclRulePredicatesTest extends UnitTestBase
{
    private final FirewallRuleFactory _firewallRuleFactory = mock(FirewallRuleFactory.class);
    private final AclRulePredicatesBuilder _builder = new AclRulePredicatesBuilder();

    @Before
    public void setUp() throws Exception
    {
        final FirewallRule firewallRule = new TestFirewallRule();
        when(_firewallRuleFactory.createForHostname(any())).thenReturn(firewallRule);
        when(_firewallRuleFactory.createForNetwork(any())).thenReturn(firewallRule);
    }

    @Test
    public void testParse()
    {
        final String name = "name";
        final String className = "class";

        _builder.parse(NAME.name(), name);
        _builder.parse(CLASS.name(), className);

        final AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertEquals(Collections.singleton(name), predicates.get(NAME));
        assertEquals(Collections.singleton(className), predicates.get(CLASS));
    }

    @Test
    public void testParseHostnameFirewallRule()
    {
        final String hostname = "hostname1,hostname2";
        _builder.parse(FROM_HOSTNAME.name(), hostname).build(_firewallRuleFactory);

        final ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(_firewallRuleFactory).createForHostname(captor.capture());
        assertEquals(Sets.newSet("hostname1", "hostname2"), captor.getValue());
    }

    @Test
    public void testParseNetworkFirewallRule()
    {
        String networks = "network1,network2";
        _builder.parse(FROM_NETWORK.name(), networks).build(_firewallRuleFactory);

        final ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(_firewallRuleFactory).createForNetwork(captor.capture());
        assertEquals(Sets.newSet("network1", "network2"), captor.getValue());
    }

    @Test
    public void testParseThrowsExceptionIfBothHostnameAndNetworkSpecified()
    {
        _builder.parse(FROM_NETWORK.name(), "network1,network2");
        try
        {
            _builder.parse(FROM_HOSTNAME.name(), "hostname1,hostname2");
            fail("Exception not thrown");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testParseAttributesRule()
    {
        final String attributes = "attribute1,attribute2";
        final AclRulePredicates predicates = _builder.parse(ATTRIBUTES.name(), attributes).build(_firewallRuleFactory);

        final Set<String> attributesSet = Sets.newSet(attributes.split(","));
        assertEquals("Unexpected attributes",
                attributesSet,
                predicates.get(ATTRIBUTES));
    }

    @Test
    public void testParseAttributeNamesRule()
    {
        final String attributes = "attribute1,attribute2";
        final AclRulePredicates predicates = _builder.parse("attribute_names", attributes).build(_firewallRuleFactory);

        final Set<String> attributesSet = Sets.newSet(attributes.split(","));
        assertEquals("Unexpected attributes",
                attributesSet,
                predicates.get(ATTRIBUTES));
    }

    @Test
    public void testGetParsedProperties()
    {
        final AclRulePredicates predicates = _builder
                .parse(NAME.name(), "name")
                .parse(ATTRIBUTES.name(), "attribute1,attribute2")
                .parse(FROM_NETWORK.name(), "network")
                .build(_firewallRuleFactory);

        final Map<Property, Object> properties = predicates.getParsedProperties();

        assertEquals(3, properties.size());
        assertEquals(properties.get(NAME), "name");
        assertEquals(properties.get(ATTRIBUTES), "attribute1,attribute2");
        assertEquals(properties.get(FROM_NETWORK), "network");
    }

    @Test
    public void testAttributeNames()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(ATTRIBUTES).isEmpty());

        predicates = _builder.put(ATTRIBUTES, "name,host").build(_firewallRuleFactory);
        final Set<Object> attributeNames = predicates.get(ATTRIBUTES);
        assertEquals(2, attributeNames.size());
        assertTrue(attributeNames.contains("name"));
        assertTrue(attributeNames.contains("host"));
    }

    @Test
    public void testPut_NullInput()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.ROUTING_KEY).isEmpty());

        predicates = _builder.put(Property.ROUTING_KEY, null).build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.ROUTING_KEY).size());
        assertEquals("*", Iterables.getOnlyElement(predicates.get(Property.ROUTING_KEY)));
    }

    @Test
    public void testPut_emptyString()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.QUEUE_NAME).isEmpty());

        predicates = _builder.put(Property.QUEUE_NAME, " ").build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.QUEUE_NAME).size());
        assertEquals("*", Iterables.getOnlyElement(predicates.get(Property.QUEUE_NAME)));
    }

    @Test
    public void testPut_Boolean()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.DURABLE).isEmpty());

        predicates = _builder
                .put(Property.DURABLE, "TRUE")
                .put(Property.TEMPORARY, "False")
                .put(Property.EXCLUSIVE, "T*")
                .put(Property.AUTO_DELETE, "fal*")
                .build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.DURABLE).size());
        assertEquals(1, predicates.get(Property.TEMPORARY).size());
        assertEquals(1, predicates.get(Property.EXCLUSIVE).size());
        assertEquals(1, predicates.get(Property.AUTO_DELETE).size());
        assertEquals(Boolean.TRUE, Iterables.getOnlyElement(predicates.get(Property.DURABLE)));
        assertEquals(Boolean.FALSE, Iterables.getOnlyElement(predicates.get(Property.TEMPORARY)));
        assertEquals(Boolean.TRUE, Iterables.getOnlyElement(predicates.get(Property.EXCLUSIVE)));
        assertEquals(Boolean.FALSE, Iterables.getOnlyElement(predicates.get(Property.AUTO_DELETE)));
    }

    @Test
    public void testPut_Boolean_SpecialCharacters()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.DURABLE).isEmpty());

        predicates = _builder
                .put(Property.DURABLE, "*")
                .build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.DURABLE).size());
        assertEquals("*", Iterables.getOnlyElement(predicates.get(Property.DURABLE)));

        try
        {
            _builder.put(Property.DURABLE, "X*").build(_firewallRuleFactory);
        }
        catch (IllegalArgumentException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testPut()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.TYPE).isEmpty());

        predicates = _builder.put(Property.TYPE, "EX ").build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.TYPE).size());
        assertEquals("EX", Iterables.getOnlyElement(predicates.get(Property.TYPE)));
    }

    @Test
    public void testParse_MultiValue()
    {
        final String nameA = "name.A";
        AclRulePredicates predicates = _builder.parse(NAME.name(), nameA).build(_firewallRuleFactory);
        assertEquals(Collections.singleton(nameA), predicates.get(NAME));

        final String nameB = "name.B";
        predicates = _builder.parse(NAME.name(), nameB).build(_firewallRuleFactory);
        assertEquals(new HashSet<>(Arrays.asList(nameA, nameB)), predicates.get(NAME));

        final String nameX = "name.*";
        predicates = _builder.parse(NAME.name(), nameX).build(_firewallRuleFactory);
        assertEquals(Collections.singleton(nameX), predicates.get(NAME));

        final String nameC = "name.C";
        predicates = _builder.parse(NAME.name(), nameC).build(_firewallRuleFactory);
        assertEquals(Collections.singleton(nameX), predicates.get(NAME));

        predicates = _builder.parse(NAME.name(), "*").build(_firewallRuleFactory);
        assertEquals(Collections.singleton("*"), predicates.get(NAME));
    }

    @Test
    public void testEqualsHashCode()
    {
        _builder.parse(NAME.name(), "name");
        _builder.parse(QUEUE_NAME.name(), "queue");
        _builder.parse(DURABLE.name(), "true");
        _builder.parse(ATTRIBUTES.name(), "name,virtualhost");
        _builder.parse(FROM_HOSTNAME.name(), "localhost");
        final AclRulePredicates first = _builder.build(_firewallRuleFactory);

        final AclRulePredicatesBuilder builder = new AclRulePredicatesBuilder();
        builder.parse(NAME.name(), "name");
        builder.parse(QUEUE_NAME.name(), "queue");
        builder.parse(DURABLE.name(), "true");
        builder.parse(ATTRIBUTES.name(), "virtualhost,name");
        builder.parse(FROM_HOSTNAME.name(), "localhost");
        final AclRulePredicates second = builder.build(_firewallRuleFactory);

        assertEquals(first, first);
        assertEquals(first, second);
        assertEquals(second, first);
        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void testNotEquals()
    {
        _builder.parse(NAME.name(), "name");
        _builder.parse(QUEUE_NAME.name(), "queue");
        final AclRulePredicates first = _builder.build(_firewallRuleFactory);

        AclRulePredicatesBuilder builder = new AclRulePredicatesBuilder();
        builder.parse(NAME.name(), "x");
        builder.parse(QUEUE_NAME.name(), "queue");
        AclRulePredicates another = builder.build(_firewallRuleFactory);

        assertNotEquals(another, first);
        assertNotEquals(first, another);

        builder = new AclRulePredicatesBuilder();
        builder.parse(NAME.name(), "name");
        builder.parse(QUEUE_NAME.name(), "q1");
        another = builder.build(_firewallRuleFactory);

        assertNotEquals(another, first);
        assertNotEquals(first, another);

        assertFalse(first.equals("X"));
        assertFalse(first.equals(null));
    }

    @Test
    public void testToString()
    {
        final String name = "xName";
        final String className = "class";
        final String queueName = "qName";

        _builder.parse(NAME.name(), name);
        _builder.parse(CLASS.name(), className);
        _builder.parse(QUEUE_NAME.name(), queueName);
        _builder.parse(ATTRIBUTES.name(), "name,queue");

        final String str = _builder.build(_firewallRuleFactory).toString();
        assertTrue(str.contains(name));
        assertTrue(str.contains(className));
        assertTrue(str.contains(queueName));
        assertTrue(str.contains("name"));
        assertTrue(str.contains("queue"));
    }
}
