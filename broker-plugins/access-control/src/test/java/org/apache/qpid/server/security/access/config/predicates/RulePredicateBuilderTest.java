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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.Property;
import org.apache.qpid.server.security.access.config.RulePredicate;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.Before;
import org.junit.Test;

import static org.apache.qpid.server.security.access.config.Property.ATTRIBUTES;
import static org.apache.qpid.server.security.access.config.Property.COMPONENT;
import static org.apache.qpid.server.security.access.config.Property.DURABLE;
import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.apache.qpid.server.security.access.config.Property.FROM_NETWORK;
import static org.apache.qpid.server.security.access.config.Property.METHOD_NAME;
import static org.apache.qpid.server.security.access.config.Property.NAME;
import static org.apache.qpid.server.security.access.config.Property.ROUTING_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RulePredicateBuilderTest extends UnitTestBase
{
    private final FirewallRuleFactory _firewallRuleFactory = mock(FirewallRuleFactory.class);
    private final Subject _subject = TestPrincipalUtils.createTestSubject("TEST_USER");

    private TestFirewallRule _firewallRule;
    private RulePredicateBuilder _builder;

    @Before
    public void setUp() throws Exception
    {
        _firewallRule = new TestFirewallRule();
        when(_firewallRuleFactory.createForHostname(any())).thenReturn(_firewallRule);
        when(_firewallRuleFactory.createForNetwork(any())).thenReturn(_firewallRule);
        _builder = new RulePredicateBuilder(_firewallRuleFactory);
    }

    @Test
    public void testMatch_Attributes()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(ATTRIBUTES, Arrays.asList("name", "port", "host", "active")));

        final ObjectProperties action = new ObjectProperties();
        action.addAttributeNames("name", "host", "port");

        assertTrue(predicate.matches(LegacyOperation.UPDATE, action, _subject));
        assertTrue(predicate.matches(LegacyOperation.ACCESS, action, _subject));
    }

    @Test
    public void testMatch_Attributes_empty()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(ATTRIBUTES, Collections.emptySet()));

        final ObjectProperties action = new ObjectProperties();
        action.addAttributeNames("name", "host", "port");

        assertTrue(predicate.matches(LegacyOperation.UPDATE, action, _subject));
        assertTrue(predicate.matches(LegacyOperation.ACCESS, action, _subject));
    }

    @Test
    public void testDoesNotMatch_Attributes()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(ATTRIBUTES, Arrays.asList("name", "port")));

        final ObjectProperties action = new ObjectProperties();
        action.addAttributeNames("name", "host", "port");

        assertFalse(predicate.matches(LegacyOperation.UPDATE, action, _subject));
        assertTrue(predicate.matches(LegacyOperation.ACCESS, action, _subject));
    }

    @Test
    public void testMatch_Hostname()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(FROM_HOSTNAME, Collections.singleton("localhost")));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testMatch_Hostname_empty()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(FROM_HOSTNAME, Collections.emptySet()));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testDoesNotMatch_Hostname()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(FROM_HOSTNAME, Collections.singleton("localhost")));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testMatch_Network()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(FROM_NETWORK, Collections.singleton("localhost")));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testMatch_Network_empty()
    {
        _firewallRule.setSubject(_subject);
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(FROM_NETWORK, Collections.emptySet()));

        assertTrue(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testDoesNotMatch_Network()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(FROM_NETWORK, Collections.singleton("localhost")));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testMatch_Boolean()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(DURABLE, Collections.singleton(Boolean.TRUE)));
        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, true);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testDoesNotMatch_Boolean()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(DURABLE, Collections.singleton(Boolean.TRUE)));
        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, false);
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testMatch_String()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(NAME, Arrays.asList("Exchange.public.*", "Exchange.private.A")));

        ObjectProperties op = new ObjectProperties(NAME, "Exchange.private.A");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties(NAME, "Exchange.public.ABC");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testDoesNotMatch_String()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(NAME, Arrays.asList("Exchange.public.*", "Exchange.private.A")));

        final ObjectProperties op = new ObjectProperties(NAME, "Exchange.private.B");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testMatch_AnyString()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(NAME, Arrays.asList("*", "Exchange.private.ABC")));

        final ObjectProperties op = new ObjectProperties(NAME, "ABC");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testDoesNotMatch_AnyString()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(NAME, Arrays.asList("*", "Exchange.private.ABC")));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }

    @Test
    public void testMatch_Mixing()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(DURABLE, Arrays.asList(Boolean.TRUE, "Yes")));

        ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, true);
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties();
        op.put(DURABLE, "Yes");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testDoesNotMatch_Mixing()
    {
        final RulePredicate predicate = _builder.build(
                Collections.singletonMap(DURABLE, Arrays.asList(Boolean.TRUE, "Y*")));

        ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, false);
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties();
        op.put(DURABLE, "Not");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testMatch_Multiple()
    {
        final Map<Property, List<?>> properties = new HashMap<>();
        properties.put(DURABLE, Collections.singletonList(Boolean.TRUE));
        properties.put(NAME, Arrays.asList("Exchange.public.*", "Exchange.private.A", "Exchange.private.B"));
        properties.put(COMPONENT, Collections.singletonList("*"));
        properties.put(METHOD_NAME, Collections.emptyList());
        properties.put(ROUTING_KEY, null);
        final RulePredicate predicate = _builder.build(properties);

        final ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, true);
        op.put(COMPONENT, "queue");
        op.setName("Exchange.public.A");
        assertTrue(predicate.matches(LegacyOperation.PUBLISH, op, _subject));
    }

    @Test
    public void testDoesNotMatch_Multiple()
    {
        final Map<Property, List<?>> properties = new HashMap<>();
        properties.put(DURABLE, Collections.singletonList(Boolean.TRUE));
        properties.put(NAME, Arrays.asList("Exchange.public.*", "Exchange.private.A", "Exchange.private.B"));
        properties.put(COMPONENT, Collections.singletonList("*"));
        properties.put(METHOD_NAME, Collections.emptyList());
        properties.put(ROUTING_KEY, null);
        final RulePredicate predicate = _builder.build(properties);

        ObjectProperties op = new ObjectProperties();
        op.put(DURABLE, false);
        op.put(COMPONENT, "exchange");
        op.setName("Exchange.public.A");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties();
        op.put(DURABLE, true);
        op.setName("Exchange.public.A");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        op = new ObjectProperties();
        op.put(DURABLE, true);
        op.put(COMPONENT, "exchange");
        op.setName("Exchange.private.C");
        assertFalse(predicate.matches(LegacyOperation.PUBLISH, op, _subject));

        assertFalse(predicate.matches(LegacyOperation.PUBLISH, new ObjectProperties(), _subject));
    }
}
