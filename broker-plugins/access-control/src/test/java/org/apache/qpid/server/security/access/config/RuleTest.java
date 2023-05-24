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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleTest extends UnitTestBase
{
    @Test
    void equalsAndHashCode()
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
    void notEquals()
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

        assertNotEquals(rule, new Rule.Builder()
                .withIdentity("identity2")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), "Different identity should cause rules to be unequal");

        assertNotEquals(new Rule.Builder()
                .withIdentity("identity2")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), rule, "Different identity should cause rules to be unequal");

        assertNotEquals(rule, new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), "Different operation should cause rules to be unequal");

        assertNotEquals(new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), rule, "Different action should cause rules to be unequal");

        assertNotEquals(rule, new Rule.Builder()
                .withIdentity("identity2")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), "Different object type should cause rules to be unequal");

        assertNotEquals(new Rule.Builder()
                .withIdentity("identity2")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(allow)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), rule, "Different object type should cause rules to be unequal");

        assertNotEquals(rule, new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.DENY)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), "Different permission should cause rules to be unequal");

        assertNotEquals(new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.DENY)
                .withPredicate(Property.VIRTUALHOST_NAME, "vhname")
                .build(), rule, "Different permission should cause rules to be unequal");

        assertNotEquals(rule, new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .build(), "Different predicates should cause rules to be unequal");

        assertNotEquals(new Rule.Builder()
                .withIdentity(identity)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(allow)
                .build(), rule, "Different predicates should cause rules to be unequal");

        assertNotEquals(null, rule);
        assertNotEquals("rule", rule);
    }

    @Test
    void asAclRule()
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
    void doesNotMatch_Operation()
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
    void match_AnyOperation()
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
    void doesNotMatch_ObjectType()
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
    void match_AnyObjectType()
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
    void doesNotMatch_FirewallRule()
    {
        final FirewallRule firewallRule = subject -> false;

        final FirewallRuleFactory firewallRuleFactory = new FirewallRuleFactory()
        {
            @Override
            public FirewallRule createForHostname(final Collection<String> hostnames)
            {
                return firewallRule;
            }

            @Override
            public FirewallRule createForNetwork(final Collection<String> networks)
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
    void match_FirewallRule()
    {
        final FirewallRule firewallRule = subject -> true;

        final FirewallRuleFactory firewallRuleFactory = new FirewallRuleFactory()
        {
            @Override
            public FirewallRule createForHostname(final Collection<String> hostnames)
            {
                return firewallRule;
            }

            @Override
            public FirewallRule createForNetwork(final Collection<String> networks)
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
    void doesNotMatch_Attributes()
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
    void match_Attributes()
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
    void match_Attributes2()
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
    void doesNotMatch_Predicates()
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
    void match_Predicates()
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
    void match_Any()
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
    void getAttributes()
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

    @ParameterizedTest
    @CsvSource(
    {
            "all,true,true", "All,true,true", "ALL,true,true", "any,false,false", "Any,false,false", "ANY,false,false"
    })
    void isAll(final String identity, final boolean isForAll, final boolean isForOwnerOrAll)
    {
        final Rule rule = new Rule.Builder().withIdentity(identity).build();
        assertEquals(isForAll, rule.isForAll());
        assertEquals(isForOwnerOrAll, rule.isForOwnerOrAll());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "owner,true,true", "Owner,true,true", "OWNER,true,true",
            "any,false,false", "Any,false,false", "ANY,false,false"
    })
    void isOwner(final String identity, final boolean isForOwner, final boolean isForOwnerOrAll)
    {
        final Rule rule = new Rule.Builder().withIdentity(identity).build();
        assertEquals(isForOwner, rule.isForOwner());
        assertEquals(isForOwnerOrAll, rule.isForOwnerOrAll());
    }

    @Test
    void withOwner()
    {
        final Rule rule = new Rule.Builder().withOwner().build();
        assertTrue(rule.isForOwner());
        assertTrue(rule.isForOwnerOrAll());
    }
}
