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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.security.access.config.predicates.TestFirewallRule;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.util.CollectionUtils;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.collections.Sets;

import static org.apache.qpid.server.security.access.config.Property.ATTRIBUTES;
import static org.apache.qpid.server.security.access.config.Property.CLASS;
import static org.apache.qpid.server.security.access.config.Property.DURABLE;
import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.apache.qpid.server.security.access.config.Property.FROM_NETWORK;
import static org.apache.qpid.server.security.access.config.Property.NAME;
import static org.apache.qpid.server.security.access.config.Property.QUEUE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class AclRulePredicatesTest extends UnitTestBase
{
    private AclRulePredicatesBuilder _builder;
    private FirewallRuleFactory _firewallRuleFactory;

    @BeforeEach
    public void setUp() throws Exception
    {
        _builder = new AclRulePredicatesBuilder();
        final FirewallRule firewallRule = new TestFirewallRule();
        _firewallRuleFactory = mock(FirewallRuleFactory.class);
        when(_firewallRuleFactory.createForHostname(any())).thenReturn(firewallRule);
        when(_firewallRuleFactory.createForNetwork(any())).thenReturn(firewallRule);
    }

    @Test
    void parse()
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
    void parseHostnameFirewallRule()
    {
        final String hostname = "hostname1,hostname2";
        _builder.parse(FROM_HOSTNAME.name(), hostname).build(_firewallRuleFactory);

        final ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(_firewallRuleFactory).createForHostname(captor.capture());
        assertEquals(Sets.newSet("hostname1", "hostname2"), captor.getValue());
    }

    @Test
    void parseNetworkFirewallRule()
    {
        final String networks = "network1,network2";
        _builder.parse(FROM_NETWORK.name(), networks).build(_firewallRuleFactory);

        final ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(_firewallRuleFactory).createForNetwork(captor.capture());
        assertEquals(Sets.newSet("network1", "network2"), captor.getValue());
    }

    @Test
    void parseThrowsExceptionIfBothHostnameAndNetworkSpecified()
    {
        final String fromHostnameName = FROM_HOSTNAME.name();
        _builder.parse(FROM_NETWORK.name(), "network1,network2");
        assertThrows(IllegalStateException.class,
                () -> _builder.parse(fromHostnameName, "hostname1,hostname2"),
                "Exception not thrown");
    }

    @Test
    void parseAttributesRule()
    {
        final String attributes = "attribute1,attribute2";
        final AclRulePredicates predicates = _builder.parse(ATTRIBUTES.name(), attributes).build(_firewallRuleFactory);

        final Set<String> attributesSet = Sets.newSet(attributes.split(","));
        assertEquals(attributesSet, predicates.get(ATTRIBUTES), "Unexpected attributes");
    }

    @Test
    void parseAttributeNamesRule()
    {
        final String attributes = "attribute1,attribute2";
        final AclRulePredicates predicates = _builder.parse("attribute_names", attributes).build(_firewallRuleFactory);

        final Set<String> attributesSet = Sets.newSet(attributes.split(","));
        assertEquals(attributesSet, predicates.get(ATTRIBUTES), "Unexpected attributes");
    }

    @Test
    void getParsedProperties()
    {
        final AclRulePredicates predicates = _builder
                .parse(NAME.name(), "name")
                .parse(ATTRIBUTES.name(), "attribute1,attribute2")
                .parse(FROM_NETWORK.name(), "network")
                .build(_firewallRuleFactory);

        final Map<Property, Object> properties = predicates.getParsedProperties();

        assertEquals(3, properties.size());
        assertEquals("name", properties.get(NAME));
        assertEquals("attribute1,attribute2", properties.get(ATTRIBUTES));
        assertEquals("network", properties.get(FROM_NETWORK));
    }

    @Test
    void attributeNames()
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
    void put_NullInput()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.ROUTING_KEY).isEmpty());

        predicates = _builder.put(Property.ROUTING_KEY, null).build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.ROUTING_KEY).size());
        assertEquals("*", CollectionUtils.getOnlyElement(predicates.get(Property.ROUTING_KEY)));
    }

    @Test
    void put_emptyString()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(QUEUE_NAME).isEmpty());

        predicates = _builder.put(QUEUE_NAME, " ").build(_firewallRuleFactory);
        assertEquals(1, predicates.get(QUEUE_NAME).size());
        assertEquals("*", CollectionUtils.getOnlyElement(predicates.get(QUEUE_NAME)));
    }

    @Test
    void put_Boolean()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(DURABLE).isEmpty());

        predicates = _builder
                .put(DURABLE, "TRUE")
                .put(Property.TEMPORARY, "False")
                .put(Property.EXCLUSIVE, "T*")
                .put(Property.AUTO_DELETE, "fal*")
                .build(_firewallRuleFactory);
        assertEquals(1, predicates.get(DURABLE).size());
        assertEquals(1, predicates.get(Property.TEMPORARY).size());
        assertEquals(1, predicates.get(Property.EXCLUSIVE).size());
        assertEquals(1, predicates.get(Property.AUTO_DELETE).size());
        assertEquals(Boolean.TRUE, CollectionUtils.getOnlyElement(predicates.get(DURABLE)));
        assertEquals(Boolean.FALSE, CollectionUtils.getOnlyElement(predicates.get(Property.TEMPORARY)));
        assertEquals(Boolean.TRUE, CollectionUtils.getOnlyElement(predicates.get(Property.EXCLUSIVE)));
        assertEquals(Boolean.FALSE, CollectionUtils.getOnlyElement(predicates.get(Property.AUTO_DELETE)));
    }

    @Test
    void put_Boolean_SpecialCharacters()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(DURABLE).isEmpty());

        predicates = _builder
                .put(DURABLE, "*")
                .build(_firewallRuleFactory);
        assertEquals(1, predicates.get(DURABLE).size());
        assertEquals("*", CollectionUtils.getOnlyElement(predicates.get(DURABLE)));

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> _builder.put(DURABLE, "X*"),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void put()
    {
        AclRulePredicates predicates = _builder.build(_firewallRuleFactory);
        assertTrue(predicates.get(Property.TYPE).isEmpty());

        predicates = _builder.put(Property.TYPE, "EX ").build(_firewallRuleFactory);
        assertEquals(1, predicates.get(Property.TYPE).size());
        assertEquals("EX", CollectionUtils.getOnlyElement(predicates.get(Property.TYPE)));
    }

    @Test
    void parse_MultiValue()
    {
        final String nameA = "name.A";
        AclRulePredicates predicates = _builder.parse(NAME.name(), nameA).build(_firewallRuleFactory);
        assertEquals(Set.of(nameA), predicates.get(NAME));

        final String nameB = "name.B";
        predicates = _builder.parse(NAME.name(), nameB).build(_firewallRuleFactory);
        assertEquals(Set.of(nameA, nameB), predicates.get(NAME));

        final String nameX = "name.*";
        predicates = _builder.parse(NAME.name(), nameX).build(_firewallRuleFactory);
        assertEquals(Set.of(nameX), predicates.get(NAME));

        final String nameC = "name.C";
        predicates = _builder.parse(NAME.name(), nameC).build(_firewallRuleFactory);
        assertEquals(Set.of(nameX), predicates.get(NAME));

        predicates = _builder.parse(NAME.name(), "*").build(_firewallRuleFactory);
        assertEquals(Set.of("*"), predicates.get(NAME));
    }

    @Test
    void equalsHashCode()
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
    void notEquals()
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

        assertNotEquals("X", first);
        assertNotEquals(null, first);
    }

    @Test
    void string()
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
