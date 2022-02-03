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

package org.apache.qpid.server.security.access.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ListIterator;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.Rule.Builder;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * This test checks that the {@link RuleSet} object which forms the core of the access control plugin performs
 * correctly.
 * <p>
 * The ruleset is configured directly rather than using an external file by adding rules individually, calling the
 * {@link RuleCollector#addRule(Integer, Rule)} method
 * . Then, the
 * access control mechanism is validated by checking whether operations would be authorised by calling the
 * {@link RuleSet#check(Subject, LegacyOperation, ObjectType, ObjectProperties)} method.
 * <p>
 * It ensure that permissions can be granted correctly on users directly and on groups.
 */
public class RuleSetTest extends UnitTestBase
{
    private static final String DENIED_VH = "deniedVH";
    private static final String ALLOWED_VH = "allowedVH";

    private static final ObjectProperties EMPTY = new ObjectProperties();

    private RuleCollector _ruleCollector = new RuleCollector();

    private static final String TEST_USER = "user";

    // Common things that are passed to frame constructors
    private final String _queueName = this.getClass().getName() + "queue";
    private final String _exchangeName = "amq.direct";
    private final String _exchangeType = "direct";
    private final Subject _testSubject = TestPrincipalUtils.createTestSubject(TEST_USER);

    @Before
    public void setUp() throws Exception
    {
        _ruleCollector = new RuleCollector();
    }

    private RuleSet createRuleSet()
    {
        return _ruleCollector.createRuleSet(mock(EventLoggerProvider.class));
    }

    private void assertDenyGrantAllow(Subject subject, LegacyOperation operation, ObjectType objectType)
    {
        assertDenyGrantAllow(subject, operation, objectType, EMPTY);
    }

    private void assertDenyGrantAllow(Subject subject,
                                      LegacyOperation operation,
                                      ObjectType objectType,
                                      ObjectProperties properties)
    {
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(subject, operation, objectType, properties));
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(operation)
                .withObject(objectType)
                .withPredicates(properties)
                .build());
        ruleSet = createRuleSet();
        assertEquals(1, ruleSet.size());
        assertEquals(Result.ALLOWED, ruleSet.check(subject, operation, objectType, properties));
    }

    @Test
    public void testEmptyRuleSet()
    {
        final RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);
        assertEquals(0, ruleSet.size());
        assertEquals(ruleSet.getDefault(),
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
    }

    @Test
    public void testVirtualHostNodeCreateAllowPermissionWithVirtualHostName()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.VIRTUALHOSTNODE)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.VIRTUALHOSTNODE, EMPTY));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.DELETE, ObjectType.VIRTUALHOSTNODE, EMPTY));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithVirtualHostName()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.NAME, ALLOWED_VH)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithNameSetToWildCard()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.NAME, AclRulePredicatesBuilder.WILD_CARD)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithNoName()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessDenyPermissionWithNoName()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessDenyPermissionWithNameSetToWildCard()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.NAME, AclRulePredicatesBuilder.WILD_CARD)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowDenyPermissions()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.NAME, DENIED_VH)
                .build());
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicate(Property.NAME, ALLOWED_VH)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithVirtualHostNameOtherPredicate()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.put(Property.VIRTUALHOST_NAME, ALLOWED_VH);

        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicates(properties)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties));
        assertEquals(Result.DEFER, ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST,
                                                        new ObjectProperties(DENIED_VH)));
    }


    @Test
    public void testQueueCreateNamed()
    {
        assertDenyGrantAllow(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, new ObjectProperties(_queueName));
    }

    @Test
    public void testQueueCreateNamedVirtualHost()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicate(Property.VIRTUALHOST_NAME, ALLOWED_VH)
                .build());
        final RuleSet ruleSet = createRuleSet();
        final ObjectProperties allowedQueueObjectProperties = new ObjectProperties(_queueName);
        allowedQueueObjectProperties.put(Property.VIRTUALHOST_NAME, ALLOWED_VH);
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, new ObjectProperties(allowedQueueObjectProperties)));

        final ObjectProperties deniedQueueObjectProperties = new ObjectProperties(_queueName);
        deniedQueueObjectProperties.put(Property.VIRTUALHOST_NAME, DENIED_VH);
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, deniedQueueObjectProperties));
    }

    @Test
    public void testQueueCreateNamedNullRoutingKey()
    {
        final ObjectProperties properties = new ObjectProperties(_queueName);
        properties.put(Property.ROUTING_KEY, null);

        assertDenyGrantAllow(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, properties);
    }

    @Test
    public void testExchangeCreateNamedVirtualHost()
    {
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.VIRTUALHOST_NAME, ALLOWED_VH)
                .build());
        final RuleSet ruleSet = createRuleSet();
        final ObjectProperties allowedExchangeProperties = new ObjectProperties(_exchangeName);
        allowedExchangeProperties.put(Property.TYPE, _exchangeType);
        allowedExchangeProperties.put(Property.VIRTUALHOST_NAME, ALLOWED_VH);

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.EXCHANGE, allowedExchangeProperties));

        final ObjectProperties deniedExchangeProperties = new ObjectProperties(_exchangeName);
        deniedExchangeProperties.put(Property.TYPE, _exchangeType);
        deniedExchangeProperties.put(Property.VIRTUALHOST_NAME, DENIED_VH);
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.EXCHANGE, deniedExchangeProperties));
    }

    @Test
    public void testExchangeCreate()
    {
        final ObjectProperties properties = new ObjectProperties(_exchangeName);
        properties.put(Property.TYPE, _exchangeType);

        assertDenyGrantAllow(_testSubject, LegacyOperation.CREATE, ObjectType.EXCHANGE, properties);
    }

    @Test
    public void testConsume()
    {
        assertDenyGrantAllow(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE);
    }

    @Test
    public void testPublish()
    {
        assertDenyGrantAllow(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE);
    }

    /**
    * If the consume permission for temporary queues is for an unnamed queue then it should
    * be global for any temporary queue but not for any non-temporary queue
    */
    @Test
    public void testTemporaryUnnamedQueueConsume()
    {
        final ObjectProperties temporary = new ObjectProperties();
        temporary.put(Property.AUTO_DELETE, Boolean.TRUE);

        final ObjectProperties normal = new ObjectProperties();
        normal.put(Property.AUTO_DELETE, Boolean.FALSE);

        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));
        _ruleCollector.addRule(0, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CONSUME)
                .withObject(ObjectType.QUEUE)
                .withPredicates(temporary)
                .build());
        ruleSet = createRuleSet();
        assertEquals(1, ruleSet.size());
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));

        // defer to global if exists, otherwise default answer - this is handled by the security manager
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, normal));
    }

    /**
     * Test that temporary queue permissions before queue perms in the ACL config work correctly
     */
    @Test
    public void testTemporaryQueueFirstConsume()
    {
        final ObjectProperties temporary = new ObjectProperties(_queueName);
        temporary.put(Property.AUTO_DELETE, Boolean.TRUE);

        final ObjectProperties normal = new ObjectProperties(_queueName);
        normal.put(Property.AUTO_DELETE, Boolean.FALSE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));

        // should not matter if the temporary permission is processed first or last
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CONSUME)
                .withObject(ObjectType.QUEUE)
                .withPredicates(normal)
                .build());

        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CONSUME)
                .withObject(ObjectType.QUEUE)
                .withPredicates(temporary)
                .build());

        ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, normal));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));
    }

    /**
     * Test that temporary queue permissions after queue perms in the ACL config work correctly
     */
    @Test
    public void testTemporaryQueueLastConsume()
    {
        final ObjectProperties temporary = new ObjectProperties(_queueName);
        temporary.put(Property.AUTO_DELETE, Boolean.TRUE);

        final ObjectProperties normal = new ObjectProperties(_queueName);
        normal.put(Property.AUTO_DELETE, Boolean.FALSE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));

        // should not matter if the temporary permission is processed first or last
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CONSUME)
                .withObject(ObjectType.QUEUE)
                .withPredicates(temporary)
                .build());

        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CONSUME)
                .withObject(ObjectType.QUEUE)
                .withPredicates(normal)
                .build());

        ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, normal));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));
    }

    /*
     * Test different rules for temporary queues.
     */

    /**
     * The more generic rule first is used, so both requests are allowed.
     */
    @Test
    public void testFirstNamedSecondTemporaryQueueDenied()
    {
        final ObjectProperties named = new ObjectProperties(_queueName);
        final ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(Property.AUTO_DELETE, Boolean.TRUE);

        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(named)
                .build());

        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(namedTemporary)
                .build());

        ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
    }

    /**
     * The more specific rule is first, so those requests are denied.
     */
    @Test
    public void testFirstTemporarySecondNamedQueueDenied()
    {
        final ObjectProperties named = new ObjectProperties(_queueName);
        final ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(Property.AUTO_DELETE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(namedTemporary)
                .build());

        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(named)
                .build());

        ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
    }

    /**
     * The more specific rules are first, so those requests are denied.
     */
    @Test
    public void testFirstTemporarySecondDurableThirdNamedQueueDenied()
    {
        final ObjectProperties named = new ObjectProperties(_queueName);
        final ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(Property.AUTO_DELETE, Boolean.TRUE);
        final ObjectProperties namedDurable = new ObjectProperties(_queueName);
        namedDurable.put(Property.DURABLE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedDurable));

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(namedTemporary)
                .build());

        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(namedDurable)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(named)
                .build());

        ruleSet = createRuleSet();
        assertEquals(3, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedDurable));
    }

    @Test
    public void testNamedTemporaryQueueAllowed()
    {
        final ObjectProperties named = new ObjectProperties(_queueName);
        final ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(Property.AUTO_DELETE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(namedTemporary)
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(named)
                .build());
        ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
    }

    @Test
    public void testNamedTemporaryQueueDeniedAllowed()
    {
        final ObjectProperties named = new ObjectProperties(_queueName);
        final ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(Property.AUTO_DELETE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(namedTemporary)
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.CREATE)
                .withObject(ObjectType.QUEUE)
                .withPredicates(named)
                .build());
        ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
    }

    /**
     * Tests support for the {@link Rule#ALL} keyword.
     */
    @Test
    public void testAllowToAll()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(1, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("usera"), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("userb"), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
    }

    @Test
    public void testGroupsSupported()
    {
        final String allowGroup = "allowGroup";
        final String deniedGroup = "deniedGroup";

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(allowGroup)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(deniedGroup)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("usera", allowGroup), LegacyOperation
                                    .ACCESS, ObjectType.VIRTUALHOST, EMPTY));
        assertEquals(Result.DENIED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("userb", deniedGroup), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
        assertEquals(Result.DEFER,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("user", "group not mentioned in acl"), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
    }

    /**
     * Rule order in the ACL determines the outcome of the check.  This test ensures that a user who is
     * granted explicit permission on an object, is granted that access even though a group
     * to which the user belongs is later denied the permission.
     */
    @Test
    public void testAllowDeterminedByRuleOrder()
    {
        final String group = "group";
        final String user = "user";

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(user)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(group)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject(user, group), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
    }

    /**
     * Rule order in the ACL determines the outcome of the check.  This tests ensures that a user who is denied
     * access by group, is denied access, despite there being a later rule granting permission to that user.
     */
    @Test
    public void testDenyDeterminedByRuleOrder()
    {
        final String group = "aclgroup";
        final String user = "usera";

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(group)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(user)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(2, ruleSet.size());

        assertEquals(Result.DENIED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject(user, group), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
    }

    @Test
    public void testUserInMultipleGroups()
    {
        final String allowedGroup = "group1";
        final String deniedGroup = "group2";

        _ruleCollector.addRule(1, new Builder()
                .withIdentity(allowedGroup)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(deniedGroup)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        final RuleSet ruleSet = createRuleSet();
        final Subject subjectInBothGroups = TestPrincipalUtils.createTestSubject("user", allowedGroup, deniedGroup);
        final Subject subjectInDeniedGroupAndOneOther = TestPrincipalUtils.createTestSubject("user", deniedGroup, "some other group");
        final Subject subjectInAllowedGroupAndOneOther = TestPrincipalUtils.createTestSubject("user", allowedGroup, "some other group");

        assertEquals(Result.ALLOWED,
                            ruleSet.check(subjectInBothGroups, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));

        assertEquals(Result.DENIED,
                            ruleSet.check(subjectInDeniedGroupAndOneOther, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));

        assertEquals(Result.ALLOWED,
                            ruleSet.check(subjectInAllowedGroupAndOneOther, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY));
    }

    @Test
    public void testUpdatedAllowedAttribute()
    {
        final ObjectProperties ruleProperties = new ObjectProperties();
        ruleProperties.addAttributeNames(Collections.singleton("attribute1"));
        _ruleCollector.addRule(1, new Rule.Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .withPredicates(ruleProperties)
                .build());
        _ruleCollector.addRule(2, new Rule.Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.UPDATE)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        final RuleSet ruleSet = createRuleSet();

        ObjectProperties updateProperties = new ObjectProperties();
        updateProperties.addAttributeNames(Collections.singleton("attribute2"));

        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, updateProperties));

        updateProperties = new ObjectProperties();
        updateProperties.addAttributeNames(Collections.singleton("attribute1"));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, updateProperties));
    }

    @Test
    public void testExistingObjectOwner()
    {
        _ruleCollector.addRule(1, new Rule.Builder()
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.CONSUME)
                .withObject(ObjectType.QUEUE)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(1, ruleSet.size());

        assertEquals(Result.ALLOWED,
                     ruleSet.check(_testSubject,
                                   LegacyOperation.CONSUME,
                                   ObjectType.QUEUE,
                                   new ObjectProperties(Property.CREATED_BY, TEST_USER)));

        assertEquals(Result.DEFER,
                     ruleSet.check(_testSubject,
                                   LegacyOperation.CONSUME,
                                   ObjectType.QUEUE,
                                   new ObjectProperties(Property.CREATED_BY, "anotherUser")));
    }

    @Test
    public void testCreateIgnoresOwnerRule()
    {
        _ruleCollector.addRule(1, new Rule.Builder()
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.QUEUE)
                .build());
        final RuleSet ruleSet = createRuleSet();
        assertEquals(1, ruleSet.size());

        assertEquals(Result.ALLOWED,
                     ruleSet.check(_testSubject,
                                   LegacyOperation.UPDATE,
                                   ObjectType.QUEUE,
                                   new ObjectProperties(Property.CREATED_BY, TEST_USER)));

        assertEquals(Result.DEFER,
                     ruleSet.check(_testSubject,
                                   LegacyOperation.CREATE,
                                   ObjectType.QUEUE,
                                   new ObjectProperties(Property.CREATED_BY, "anotherUser")));

    }

    @Test
    public void testSuppressedRules()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME, "testExchange")
                .build());
        _ruleCollector.addRule(2, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());
        _ruleCollector.addRule(4, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(4, ruleSet.size());

        assertEquals(Result.ALLOWED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties("testExchange")));
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties("exchange")));
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties()));
    }

    @Test
    public void testPublishToExchange()
    {
        _ruleCollector.addRule(1, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "rs.broadcast.*")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(7, new Builder()
                .withPredicate(Property.NAME, "rs.broadcast")
                .withPredicate(Property.ROUTING_KEY, "rs.broadcast.*")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(4, ruleSet.size());

        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties()));

        ObjectProperties object = new ObjectProperties("broadcast");
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "broadcast.public");
        assertEquals(Result.ALLOWED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        assertEquals(Result.ALLOWED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("rs.broadcast");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        assertEquals(Result.ALLOWED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "queue");
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("brs");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        // Another user
        final Subject testSubject = TestPrincipalUtils.createTestSubject("Java");
        object = new ObjectProperties("rs.broadcast");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        assertEquals(Result.DEFER, ruleSet.check(testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));
    }

    @Test
    public void testPublishToExchange_OwnerBased()
    {
        _ruleCollector.addRule(1, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "rs.broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(11, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.QUEUE_NAME, "QQ")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(4, ruleSet.size());

        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, new ObjectProperties()));

        // User = owner
        ObjectProperties object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "brs");
        object.setCreatedBy(TEST_USER);
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        object.setCreatedBy(TEST_USER);
        assertEquals(Result.ALLOWED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));
    }

    @Test
    public void testPublishToExchange_OwnerBased_withoutAuthPrincipal()
    {
        _ruleCollector.addRule(1, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withPredicate(Property.NAME,"broadcast")
                .withPredicate(Property.ROUTING_KEY, "rs.broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(11, new Builder()
                .withPredicate(Property.NAME,"broadcast")
                .withPredicate(Property.QUEUE_NAME, "QQ")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(4, ruleSet.size());

        // User without authentication principal
        final Subject notAuthentificated = new Subject(false,
                Collections.singleton(new UsernamePrincipal(TEST_USER, Mockito.mock(AuthenticationProvider.class))),
                Collections.emptySet(),
                Collections.emptySet());

        ObjectProperties object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        object.setCreatedBy(TEST_USER);
        assertEquals(Result.DENIED, ruleSet.check(notAuthentificated, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.QUEUE_NAME, "QQ");
        assertEquals(Result.ALLOWED, ruleSet.check(notAuthentificated, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));
    }

    @Test
    public void testPublishToExchange_OwnerBased_byAnotherUser()
    {
        _ruleCollector.addRule(1, new Builder()
                .withPredicate(Property.NAME,"broadcast")
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withPredicate(Property.NAME,"broadcast")
                .withPredicate(Property.ROUTING_KEY, "rs.broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(11, new Builder()
                .withPredicate(Property.NAME,"broadcast")
                .withPredicate(Property.QUEUE_NAME, "QQ")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(4, ruleSet.size());

        // Created be other user
        ObjectProperties object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "broadcast.public");
        object.setCreatedBy("another");
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.QUEUE_NAME, "QQ");
        object.setCreatedBy("another");
        assertEquals(Result.ALLOWED, ruleSet.check(_testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        // Action is performed by another user
        final Subject testSubject = TestPrincipalUtils.createTestSubject("Java");
        object = new ObjectProperties("broadcast");
        assertEquals(Result.DEFER, ruleSet.check(testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        // Action is performed by another user == owner
        object = new ObjectProperties("broadcast");
        object.setCreatedBy("Java");
        assertEquals(Result.DEFER, ruleSet.check(testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        object = new ObjectProperties("broadcast");
        object.put(Property.ROUTING_KEY, "rs.broadcast.public");
        object.setCreatedBy("Java");
        assertEquals(Result.ALLOWED, ruleSet.check(testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));
    }

    @Test
    public void testPublishToExchange_OwnerBased_withGenericRule()
    {
        _ruleCollector.addRule(1, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.ROUTING_KEY, "rs.broadcast.*")
                .withOwner()
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(11, new Builder()
                .withPredicate(Property.NAME, "broadcast")
                .withPredicate(Property.QUEUE_NAME, "QQ")
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.ALL)
                .build());

        _ruleCollector.addRule(27, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(5, ruleSet.size());

        // Action is performed by another user
        final Subject testSubject = TestPrincipalUtils.createTestSubject("Java");

        ObjectProperties object = new ObjectProperties("broadcast");
        assertEquals(Result.DENIED, ruleSet.check(testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));

        // Action is performed by another user == owner
        object = new ObjectProperties("broadcast");
        object.setCreatedBy("Java");
        assertEquals(Result.DENIED, ruleSet.check(testSubject, LegacyOperation.PUBLISH, ObjectType.EXCHANGE, object));
    }

    @Test
    public void testList_UnsupportedException()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME, "broadcast")
                .build());
        _ruleCollector.addRule(3, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        _ruleCollector.addRule(17, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertEquals(3, ruleSet.size());

        final Rule rule = new Builder()
                .withIdentity(TEST_USER)
                .withOperation(LegacyOperation.ACCESS)
                .withOutcome(RuleOutcome.ALLOW)
                .build();
        try
        {
            ruleSet.add(rule);
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.remove(ruleSet.get(1));
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.addAll(Collections.singleton(rule));
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.removeAll(new ArrayList<>(ruleSet));
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.retainAll(Collections.singleton(rule));
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.clear();
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.addAll(1, Collections.singleton(rule));
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.set(1, rule);
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.add(1, rule);
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }

        try
        {
            ruleSet.remove(1);
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            // Nothing to do
        }
    }

    @Test
    public void testList()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME, "broadcast")
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);
        assertEquals(3, ruleSet.size());
        assertFalse(ruleSet.isEmpty());

        final Rule rule = new Builder()
                .withIdentity(TEST_USER)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final Rule all = new Builder()
                .withIdentity(Rule.ALL)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .withOutcome(RuleOutcome.DENY)
                .build();

        assertTrue(ruleSet.contains(rule));
        assertTrue(ruleSet.containsAll(Arrays.asList(rule, all)));
        assertEquals(rule, ruleSet.get(1));
        assertEquals(1, ruleSet.indexOf(rule));
        assertEquals(1, ruleSet.lastIndexOf(rule));
    }

    @Test
    public void testList_Arrays()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME, "broadcast")
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);

        Object[] array = ruleSet.toArray();
        Rule[] ruleArray = ruleSet.toArray(new Rule[0]);
        assertEquals(3, array.length);
        assertEquals(3, ruleArray.length);

        for (int i = 0; i < array.length; i++)
        {
            assertEquals(ruleSet.get(i), array[i]);
            assertEquals(ruleSet.get(i), ruleArray[i]);
        }
    }

    @Test
    public void testList_Iterators()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME, "broadcast")
                .build());
        _ruleCollector.addRule(3, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        _ruleCollector.addRule(17, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);

        int j = 0;
        for (Rule r : ruleSet)
        {
            assertEquals(ruleSet.get(j++), r);
        }

        ListIterator<Rule> iterator = ruleSet.listIterator();
        assertNotNull(iterator);
        while (iterator.hasNext())
        {
            assertEquals(ruleSet.get(iterator.nextIndex()), iterator.next());
            try
            {
                iterator.remove();
                fail("An exception is expected!");
            }
            catch (RuntimeException e)
            {
                //
            }
        }

        iterator = ruleSet.listIterator(1);
        assertNotNull(iterator);
        while (iterator.hasNext())
        {
            assertEquals(ruleSet.get(iterator.nextIndex()), iterator.next());
            try
            {
                iterator.remove();
                fail("An exception is expected!");
            }
            catch (RuntimeException e)
            {
                //
            }
        }
    }

    @Test
    public void testList_subList()
    {
        _ruleCollector.addRule(1, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.PUBLISH)
                .withObject(ObjectType.EXCHANGE)
                .withPredicate(Property.NAME, "broadcast")
                .build());

        _ruleCollector.addRule(3, new Builder()
                .withIdentity(TEST_USER)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());

        _ruleCollector.addRule(17, new Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.ALL)
                .build());

        final Rule rule = new Builder()
                .withIdentity(TEST_USER)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);

        assertNotNull(ruleSet.subList(1, 2));
        assertEquals(rule, ruleSet.subList(1, 2).get(0));

        try
        {
            ruleSet.subList(1, 2).add(rule);
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            //
        }
    }

    @Test
    public void testGetEventLogger()
    {
        final Rule rule = new Builder()
                .withIdentity(TEST_USER)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final EventLogger logger = mock(EventLogger.class);
        final RuleSet ruleSet = RuleSet.newInstance(() -> logger, Collections.singletonList(rule), Result.DENIED);
        assertNotNull(ruleSet);
        assertEquals(logger, ruleSet.getEventLogger());
    }

    @Test
    public void testGetDefault()
    {
        final Rule rule = new Builder()
                .withIdentity(TEST_USER)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .withOutcome(RuleOutcome.ALLOW)
                .build();

        final EventLoggerProvider logger = mock(EventLoggerProvider.class);
        final RuleSet ruleSet = RuleSet.newInstance(logger, Collections.singletonList(rule), Result.ALLOWED);
        assertNotNull(ruleSet);
        assertEquals(Result.ALLOWED, ruleSet.getDefault());
    }
}
