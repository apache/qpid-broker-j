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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.Rule.Builder;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
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
        assertEquals(1, ruleSet.getRuleCount());
        assertEquals(Result.ALLOWED, ruleSet.check(subject, operation, objectType, properties));
    }

    @Test
    public void testEmptyRuleSet()
    {
        final RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);
        assertEquals(ruleSet.getRuleCount(), 0);
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
        ObjectProperties properties = new ObjectProperties(_exchangeName);
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
        assertEquals(1, ruleSet.getRuleCount());
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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(3, ruleSet.getRuleCount());

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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(1, ruleSet.getRuleCount());

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
        assertEquals(2, ruleSet.getRuleCount());

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
        String group = "group";
        String user = "user";

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
        assertEquals(2, ruleSet.getRuleCount());

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
        String group = "aclgroup";
        String user = "usera";

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
        assertEquals(2, ruleSet.getRuleCount());

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
        assertEquals(1, ruleSet.getRuleCount());

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
        assertEquals(1, ruleSet.getRuleCount());

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
}
