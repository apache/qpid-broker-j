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

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.apache.qpid.server.security.access.config.Property;
import org.apache.qpid.server.security.access.config.Rule;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.apache.qpid.server.security.access.config.Property.ATTRIBUTES;
import static org.apache.qpid.server.security.access.config.Property.FROM_HOSTNAME;
import static org.apache.qpid.server.security.access.config.Property.NAME;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RulePredicateTest extends UnitTestBase
{
    private final FirewallRuleFactory _firewallRuleFactory = mock(FirewallRuleFactory.class);
    private final Subject _subject = TestPrincipalUtils.createTestSubject("TEST_USER");

    private Rule.Builder _builder = new Rule.Builder();
    private TestFirewallRule _firewallRule;

    @BeforeEach
    public void setUp() throws Exception
    {
        _builder = new Rule.Builder();
        _firewallRule = new TestFirewallRule();
        when(_firewallRuleFactory.createForHostname(any())).thenReturn(_firewallRule);
        when(_firewallRuleFactory.createForNetwork(any())).thenReturn(_firewallRule);
    }

    @Test
    void matchAny()
    {
        final Rule rule = _builder
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "distribute.public");
        action.setCreatedBy("Josh");
        action.setOwner("Josh");
        action.addAttributeNames("name", "host", "port");

        assertTrue(rule.matches(LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, action, _subject));
        assertFalse(rule.matches(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, action, _subject));
        assertTrue(rule.anyPropertiesMatch());
    }

    @Test
    void match_Attributes()
    {
        final Rule rule = _builder
                .withPredicate(ATTRIBUTES, "name,port,host,active")
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "distribute.public");
        action.setCreatedBy("Josh");
        action.setOwner("Josh");
        action.addAttributeNames("name", "host", "port");

        assertFalse(rule.matches(LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, action, _subject));
        assertTrue(rule.matches(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void doesNotMatch_Attributes()
    {
        final Rule rule = _builder
                .withPredicate(ATTRIBUTES, "name,port")
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "distribute.public");
        action.setCreatedBy("Josh");
        action.setOwner("Josh");
        action.addAttributeNames("name", "host", "port");

        assertFalse(rule.matches(LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, action, _subject));
        assertFalse(rule.matches(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void match_Properties_WildCard()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "*")
                .withPredicate(Property.NAME, "")
                .withPredicate(Property.METHOD_NAME, null)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.put(Property.NAME, "broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void doesNotMatch_Properties_WildCard()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "*")
                .withPredicate(Property.NAME, "")
                .withPredicate(Property.METHOD_NAME, null)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.NAME, "broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties(), _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void match_Properties_EndWithWildCard()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withPredicate(Property.NAME, "broad*")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.put(Property.NAME, "broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void doesNotMatch_Properties_EndWithWildCard()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withPredicate(Property.NAME, "broad*")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "generic.public");
        action.put(Property.NAME, "generic");
        action.put(Property.METHOD_NAME, "publish");

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties(), _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void match_Properties_SpecificValue()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.public")
                .withPredicate(Property.NAME, "broadcast")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.put(Property.NAME, "broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void doesNotMatch_Properties_SpecificValue()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "generic.public")
                .withPredicate(NAME, "generic")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.setName("broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties(), _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void match_Properties_Combination()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withPredicate(NAME, "broadcast")
                .withPredicate(Property.METHOD_NAME, "*")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.setName("broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @ParameterizedTest
    @CsvSource({"generic.public,broadcast,publish", "generic.public,generic,publish", "generic.public,generic,", ",,"})
    void doesMatch_Properties_Combination(final String routingKey, final String name, final String methodName)
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withPredicate(NAME, "broadcast")
                .withPredicate(Property.METHOD_NAME, "*")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        assertFalse(rule.anyPropertiesMatch());

        final ObjectProperties action = new ObjectProperties();
        if (routingKey != null)
        {
            action.put(Property.ROUTING_KEY, routingKey);
        }
        if (name != null)
        {
            action.setName(name);
        }
        if (methodName != null)
        {
            action.put(Property.METHOD_NAME, methodName);
        }

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
    }

    @Test
    void match_Properties()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withPredicate(NAME, "broadcast")
                .withPredicate(Property.METHOD_NAME, "*")
                .withPredicate(Property.CREATED_BY, "JJ")
                .withPredicate(Property.DURABLE, String.valueOf(true))
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.setName("broadcast");
        action.put(Property.METHOD_NAME, "publish");
        action.setCreatedBy("JJ");
        action.put(Property.DURABLE, true);

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "broadcast.public,broadcast,",
            "broadcast.public,,publish",
            "broadcast.public,generic,publish",
            ",broadcast,publish",
            "generic.public,broadcast,publish"
    })
    void doesNotMatch_Properties(final String routingKey, final String name, final String methodName)
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withPredicate(NAME, "broadcast")
                .withPredicate(Property.METHOD_NAME, "*")
                .withPredicate(Property.CREATED_BY, "JJ")
                .withPredicate(Property.DURABLE, String.valueOf(true))
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        assertFalse(rule.anyPropertiesMatch());

        final ObjectProperties action = new ObjectProperties();
        if (routingKey != null)
        {
            action.put(Property.ROUTING_KEY, routingKey);
        }
        if (name != null)
        {
            action.setName(name);
        }
        if (methodName != null)
        {
            action.put(Property.METHOD_NAME, methodName);
        }
        action.setCreatedBy("JJ");
        action.put(Property.DURABLE, true);

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
    }

    @Test
    void match_FirewallRule()
    {
        _firewallRule.setSubject(_subject);
        final Rule rule = _builder
                .withPredicate(FROM_HOSTNAME, "localhost")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties(), _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @Test
    void doesNotMatch_FirewallRule()
    {
        _firewallRule.setSubject(TestPrincipalUtils.createTestSubject("X"));
        final Rule rule = _builder
                .withPredicate(FROM_HOSTNAME, "localhost")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties(), _subject));
        assertFalse(rule.anyPropertiesMatch());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "broadcast.public,broadcast.A,publish",
            "broadcast.public,broadcast.B,publish",
            "broadcast.public,broadcast.X.new,publish"
    })
    void match_Properties_MultiValue(final String routingKey, final String name, final String methodName)
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "broadcast.public")
                .withPredicate(Property.NAME, "broadcast.A")
                .withPredicate(Property.NAME, "broadcast.B")
                .withPredicate(Property.NAME, "broadcast.X.*")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, routingKey);
        action.put(Property.NAME, name);
        action.put(Property.METHOD_NAME, methodName);

        assertTrue(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
    }

    @Test
    void doesNotMatch_Properties_MultiValue()
    {
        final Rule rule = _builder
                .withPredicate(Property.ROUTING_KEY, "generic.public")
                .withPredicate(Property.NAME, "broadcast.A")
                .withPredicate(Property.NAME, "broadcast.B")
                .withPredicate(Property.NAME, "broadcast.X.*")
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withOutcome(RuleOutcome.ALLOW)
                .build(_firewallRuleFactory);

        final ObjectProperties action = new ObjectProperties();
        action.put(Property.ROUTING_KEY, "broadcast.public");
        action.setName("broadcast");
        action.put(Property.METHOD_NAME, "publish");

        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, action, _subject));
        assertFalse(rule.matches(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties(), _subject));
    }
}