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
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.Test;

import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RuleTest extends UnitTestBase
{
    @Test
    public void testEqualsAndHashCode()
    {
        final String identity = "identity";
        final RuleOutcome allow = RuleOutcome.ALLOW;

        final Rule rule = new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.PURGE)
                .withOutcome(allow)
                .build();
        final Rule equalRule = new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.PURGE)
                .withOutcome(allow)
                .build();

        assertEquals(rule, rule);
        assertEquals(rule, equalRule);
        assertEquals(equalRule, rule);

        assertEquals(rule.hashCode(), equalRule.hashCode());
    }

    @Test
    public void testNotEquals()
    {
        final String identity = "identity";
        final RuleOutcome allow = RuleOutcome.ALLOW;

        final Rule rule = new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build();

        assertNotEquals("Different identity should cause rules to be unequal",
                rule, new Rule.Builder()
                        .withIdentity("identity2")
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(allow)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build());

        assertNotEquals("Different identity should cause rules to be unequal",
                new Rule.Builder()
                        .withIdentity("identity2")
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(allow)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build(), rule);

        assertNotEquals("Different operation should cause rules to be unequal",
                rule, new Rule.Builder()
                        .withIdentity(identity)
                        .withOperation(LegacyOperation.ACCESS)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(allow)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build());

        assertNotEquals("Different action should cause rules to be unequal",
                new Rule.Builder()
                        .withIdentity(identity)
                        .withOperation(LegacyOperation.ACCESS)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(allow)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build(), rule);

        assertNotEquals("Different object type should cause rules to be unequal",
                rule, new Rule.Builder()
                        .withIdentity("identity2")
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.EXCHANGE)
                        .withOutcome(allow)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build());

        assertNotEquals("Different object type should cause rules to be unequal",
                new Rule.Builder()
                        .withIdentity("identity2")
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.EXCHANGE)
                        .withOutcome(allow)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build(), rule);

        assertNotEquals("Different permission should cause rules to be unequal",
                rule, new Rule.Builder()
                        .withIdentity(identity)
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(RuleOutcome.DENY)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build());

        assertNotEquals("Different permission should cause rules to be unequal",
                new Rule.Builder()
                        .withIdentity(identity)
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(RuleOutcome.DENY)
                        .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                        .build(), rule);

        assertNotEquals("Different predicates should cause rules to be unequal",
                rule, new Rule.Builder()
                        .withIdentity(identity)
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(allow)
                        .build());

        assertNotEquals("Different predicates should cause rules to be unequal",
                new Rule.Builder()
                        .withIdentity(identity)
                        .withOperation(LegacyOperation.UPDATE)
                        .withObject(ObjectType.VIRTUALHOST)
                        .withOutcome(allow)
                        .build(), rule);

        assertFalse(rule.equals(null));
        assertFalse(rule.equals("rule"));
    }

    @Test
    public void testAsAclRule()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.NAME, "name")
                .withOutcome(RuleOutcome.ALLOW)
                .build();
        final org.apache.qpid.server.security.access.plugins.AclRule aclRule = rule.asAclRule();

        assertEquals(rule.getAttributes(), aclRule.getAttributes());
        assertEquals(rule.getIdentity(), aclRule.getIdentity());
        assertEquals(rule.getObjectType(), aclRule.getObjectType());
        assertEquals(rule.getOperation(), aclRule.getOperation());
        assertEquals(rule.getOutcome(), aclRule.getOutcome());

        final Rule copy = new Rule(aclRule);

        assertEquals(rule.getAttributes(), copy.getAttributes());
        assertEquals(rule.getIdentity(), copy.getIdentity());
        assertEquals(rule.getObjectType(), copy.getObjectType());
        assertEquals(rule.getOperation(), copy.getOperation());
        assertEquals(rule.getOutcome(), copy.getOutcome());
    }

    @Test
    public void testDoesNotMatch_Operation()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.VIRTUALHOST, new ObjectProperties(), subject));
    }

    @Test
    public void testMatch_AnyOperation()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.VIRTUALHOST, new ObjectProperties(), subject));
    }

    @Test
    public void testDoesNotMatch_ObjectType()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertFalse(rule.matches(LegacyOperation.ACCESS, ObjectType.QUEUE, new ObjectProperties(), subject));
    }

    @Test
    public void testMatch_AnyObjectType()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertTrue(rule.matches(LegacyOperation.ACCESS, ObjectType.QUEUE, new ObjectProperties(), subject));
    }

    @Test
    public void testDoesNotMatch_FirewallRule()
    {
        final FirewallRule firewallRule = subject -> false;

        final FirewallRuleFactory firewallRuleFactory = new FirewallRuleFactory()
        {
            @Override
            public FirewallRule createForHostname(Collection<String> hostnames)
            {
                return firewallRule;
            }

            @Override
            public FirewallRule createForNetwork(Collection<String> networks)
            {
                return firewallRule;
            }
        };
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(FROM_HOSTNAME.name(), "hostname")
                .withOutcome(RuleOutcome.ALLOW)
                .build(firewallRuleFactory);

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertFalse(rule.matches(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(), subject));
    }

    @Test
    public void testMatch_FirewallRule()
    {
        final FirewallRule firewallRule = subject -> true;

        final FirewallRuleFactory firewallRuleFactory = new FirewallRuleFactory()
        {
            @Override
            public FirewallRule createForHostname(Collection<String> hostnames)
            {
                return firewallRule;
            }

            @Override
            public FirewallRule createForNetwork(Collection<String> networks)
            {
                return firewallRule;
            }
        };
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(FROM_HOSTNAME.name(), "hostname")
                .withOutcome(RuleOutcome.ALLOW)
                .build(firewallRuleFactory);

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertTrue(rule.matches(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(), subject));
    }

    @Test
    public void testDoesNotMatch_Attributes()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.ATTRIBUTES.name(), "name")
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        final ObjectProperties objectProperties = new ObjectProperties();
        objectProperties.addAttributeNames("name", "port");
        assertFalse(rule.matches(LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, objectProperties, subject));
    }

    @Test
    public void testMatch_Attributes()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.ATTRIBUTES.name(), "name")
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        assertTrue(rule.matches(LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, new ObjectProperties(), subject));
    }

    @Test
    public void testMatch_Attributes2()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.ATTRIBUTES.name(), "name")
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        final ObjectProperties objectProperties = new ObjectProperties();
        objectProperties.addAttributeNames("name", "port");
        assertTrue(rule.matches(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, objectProperties, subject));
    }

    @Test
    public void testDoesNotMatch_Predicates()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME.name(), "name")
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        final ObjectProperties objectProperties = new ObjectProperties("testName");
        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, objectProperties, subject));
    }

    @Test
    public void testMatch_Predicates()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME.name(), "name")
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        final ObjectProperties objectProperties = new ObjectProperties("name");
        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, objectProperties, subject));
    }

    @Test
    public void testMatch_Any()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Subject subject = TestPrincipalUtils.createTestSubject("TEST_USER");
        final ObjectProperties objectProperties = new ObjectProperties("name");
        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, objectProperties, subject));
    }

    @Test
    public void testGetAttributes()
    {
        final Rule rule = new Rule.Builder()
                .withIdentity("identity")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .withPredicate(FROM_HOSTNAME, "localhost")
                .withPredicate(Property.ATTRIBUTES, "name")
                .withPredicate(Property.NAME, "VH")
                .build();

        final Map<Property, String> expected = new HashMap<>();
        expected.put(FROM_HOSTNAME, "localhost");
        expected.put(Property.ATTRIBUTES, "name");
        expected.put(Property.NAME, "VH");

        assertEquals(rule.getAttributes(), expected);
    }

    @Test
    public void testIsAll()
    {
        assertTrue(Rule.isAll("all"));
        assertTrue(Rule.isAll("All"));
        assertTrue(Rule.isAll("ALL"));
        assertFalse(Rule.isAll("any"));
        assertFalse(Rule.isAll("Any"));
        assertFalse(Rule.isAll("ANY"));
    }

    @Test
    public void testIsOwner()
    {
        assertTrue(Rule.isOwner("owner"));
        assertTrue(Rule.isOwner("Owner"));
        assertTrue(Rule.isOwner("OWNER"));
        assertFalse(Rule.isOwner("any"));
        assertFalse(Rule.isOwner("Any"));
        assertFalse(Rule.isOwner("ANY"));
    }
}
