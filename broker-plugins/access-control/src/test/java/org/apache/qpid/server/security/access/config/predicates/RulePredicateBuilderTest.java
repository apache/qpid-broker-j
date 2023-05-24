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
package org.apache.qpid.server.security.access.config.predicates;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.Property;
import org.apache.qpid.server.security.access.config.RulePredicate;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.access.util.PrefixTreeSet;
import org.apache.qpid.server.security.access.util.WildCardSet;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.qpid.server.security.access.config.Property.ATTRIBUTES;
import static org.apache.qpid.server.security.access.config.Property.COMPONENT;
import static org.apache.qpid.server.security.access.config.Property.DURABLE;
import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.apache.qpid.server.security.access.config.Property.FROM_NETWORK;
import static org.apache.qpid.server.security.access.config.Property.METHOD_NAME;
import static org.apache.qpid.server.security.access.config.Property.NAME;
import static org.apache.qpid.server.security.access.config.Property.ROUTING_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RulePredicateBuilderTest extends UnitTestBase
{
    private final FirewallRuleFactory _firewallRuleFactory = mock(FirewallRuleFactory.class);
    private final Subject _subject = TestPrincipalUtils.createTestSubject("TEST_USER");

    private TestFirewallRule _firewallRule;
    private RulePredicateBuilder _builder;

    @BeforeEach
    public void setUp() throws Exception
    {
        _firewallRule = new TestFirewallRule();
        when(_firewallRuleFactory.createForHostname(any())).thenReturn(_firewallRule);
        when(_firewallRuleFactory.createForNetwork(any())).thenReturn(_firewallRule);
        _builder = new RulePredicateBuilder(_firewallRuleFactory);
    }

    @Test
    void match_Attributes()
    {
        final RulePredicate predicate = _builder.build(Map.of(ATTRIBUTES, List.of("name", "port", "host", "active")));

        final ObjectProperties action = new ObjectProperties();
        action.addAttributeNames("name", "host", "port");

        assertTrue(predicate.matches(LegacyOperation.UPDATE, action, _subject));
        assertTrue(predicate.matches(LegacyOperation.ACCESS, action, _subject));
    }

    @Test
    void match_Attributes_empty()
    {
        final RulePredicate predicate = _builder.build(Map.of(ATTRIBUTES, Set.of()));

        final ObjectProperties action = new ObjectProperties();
        action.addAttributeNames("name", "host", "port");

        assertTrue(predicate.matches(LegacyOperation.UPDATE, action, _subject));
        assertTrue(predicate.matches(LegacyOperation.ACCESS, action, _subject));
    }

    @Test
    void doesNotMatch_Attributes()
    {
        final RulePredicate predicate = _builder.build(Map.of(ATTRIBUTES, List.of("name", "port")));

        final ObjectProperties action = new ObjectProperties();
        action.addAttributeNames("name", "host", "port");

        assertFalse(predicate.matches(LegacyOperation.UPDATE, action, _subject));
        assertTrue(predicate.matches(LegacyOperation.ACCESS, action, _subject));
    }

    @Test
    void match_Hostname()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(Map.of(FROM_HOSTNAME, Set.of("localhost")));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void match_Hostname_empty()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(Map.of(FROM_HOSTNAME, Set.of()));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void doesNotMatch_Hostname()
    {
        final RulePredicate predicate = _builder.build(Map.of(FROM_HOSTNAME, Set.of("localhost")));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void match_Network()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(Map.of(FROM_NETWORK, Set.of("localhost")));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void match_Network_empty()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(Map.of(FROM_NETWORK, Set.of()));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void doesNotMatch_Network()
    {
        final RulePredicate predicate = _builder.build(Map.of(FROM_NETWORK, Set.of("localhost")));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void match_Boolean()
    {
        final RulePredicate predicate = _builder.build(Map.of(DURABLE, Set.of(Boolean.TRUE)));
        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, true);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void doesNotMatch_Boolean()
    {
        final RulePredicate predicate = _builder.build(Map.of(DURABLE, Set.of(Boolean.TRUE)));
        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, false);
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Exchange.private.A", "Exchange.public.ABC"})
    void match_String(final String value)
    {
        final RulePredicate predicate = _builder.build(Map.of(NAME, List.of("Exchange.public.*", "Exchange.private.A")));
        final ObjectProperties op = new ObjectProperties(NAME, value);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void doesNotMatch_String()
    {
        final RulePredicate predicate = _builder.build(Map.of(NAME, List.of("Exchange.public.*", "Exchange.private.A")));

        final ObjectProperties op = new ObjectProperties(NAME, "Exchange.private.B");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void match_AnyString()
    {
        final RulePredicate predicate = _builder.build(Map.of(NAME, List.of("*", "Exchange.private.ABC")));

        final ObjectProperties op = new ObjectProperties(NAME, "ABC");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void doesNotMatch_AnyString()
    {
        final RulePredicate predicate = _builder.build(Map.of(NAME, List.of("*", "Exchange.private.ABC")));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    void match_Mixing()
    {
        final RulePredicate predicate = _builder.build(Map.of(DURABLE, List.of(Boolean.TRUE, "Yes")));

        ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, true);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties();
        op.put(DURABLE, "Yes");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void doesNotMatch_Mixing()
    {
        final RulePredicate predicate = _builder.build(Map.of(DURABLE, List.of(Boolean.TRUE, "Y*")));

        ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, false);
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties();
        op.put(DURABLE, "Not");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void match_Multiple()
    {
        final Map<Property, List<?>> properties = new HashMap<>();
        properties.put(DURABLE, List.of(Boolean.TRUE));
        properties.put(NAME, List.of("Exchange.public.*", "Exchange.private.A", "Exchange.private.B"));
        properties.put(COMPONENT, List.of("*"));
        properties.put(METHOD_NAME, List.of());
        properties.put(ROUTING_KEY, null);
        final RulePredicate predicate = _builder.build(properties);

        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, true);
        op.put(COMPONENT, "queue");
        op.setName("Exchange.public.A");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @ParameterizedTest
    @CsvSource({"false,exchange,Exchange.public.A", "true,,Exchange.public.A", "true,exchange,Exchange.private.C"})
    void doesNotMatch_Multiple(final boolean durable, final String component, final String name)
    {
        final Map<Property, List<?>> properties = new HashMap<>();
        properties.put(DURABLE, List.of(Boolean.TRUE));
        properties.put(NAME, List.of("Exchange.public.*", "Exchange.private.A", "Exchange.private.B"));
        properties.put(COMPONENT, List.of("*"));
        properties.put(METHOD_NAME, List.of());
        properties.put(ROUTING_KEY, null);
        final RulePredicate predicate = _builder.build(properties);

        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, durable);
        if (component != null)
        {
            op.put(COMPONENT, component);
        }
        if (name != null)
        {
            op.setName(name);
        }
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Exchange.private.A", "Exchange.private.B", "Exchange.public.ABC"})
    void match_PrefixTree(final String value)
    {
        final PrefixTreeSet tree = new PrefixTreeSet();
        tree.addAll(List.of("Exchange.public.*", "Exchange.private.A", "Exchange.private.B"));
        final RulePredicate predicate = _builder.build(Map.of(NAME, tree));

        final ObjectProperties op = new ObjectProperties(NAME, value);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void doesNotMatch_PrefixTree()
    {
        final PrefixTreeSet tree = new PrefixTreeSet();
        tree.addAll(List.of("Exchange.public.*", "Exchange.private.A", "Exchange.private.B"));
        final RulePredicate predicate = _builder.build(Map.of(NAME, tree));

        final ObjectProperties op = new ObjectProperties(NAME, "Exchange.private.xyz");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void match_PrefixTree_single()
    {
        final PrefixTreeSet tree = new PrefixTreeSet();
        tree.add("Exchange.public.*");
        final RulePredicate predicate = _builder.build(Map.of(NAME, tree));

        final ObjectProperties op = new ObjectProperties(NAME, "Exchange.public.A");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    void doesNotMatch_PrefixTree_single()
    {
        final PrefixTreeSet tree = new PrefixTreeSet();
        tree.add("Exchange.public.*");
        final RulePredicate predicate = _builder.build(Map.of(NAME, tree));

        final ObjectProperties op = new ObjectProperties(NAME, "Exchange.private.xyz");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Exchange.private.A", "Exchange.private.B", "Exchange.public.ABC"})
    void match_WildcardSet(final String value)
    {
        final RulePredicate predicate = _builder.build(Map.of(NAME, WildCardSet.newSet()));
        final ObjectProperties op = new ObjectProperties(NAME, value);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }
}
