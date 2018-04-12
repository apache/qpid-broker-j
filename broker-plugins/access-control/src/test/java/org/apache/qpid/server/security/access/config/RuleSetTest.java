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
import org.apache.qpid.server.security.access.config.ObjectProperties.Property;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * This test checks that the {@link RuleSet} object which forms the core of the access control plugin performs
 * correctly.
 * <p>
 * The ruleset is configured directly rather than using an external file by adding rules individually, calling the
 * {@link RuleSetCreator#addRule(Integer, String, RuleOutcome, LegacyOperation, ObjectType, ObjectProperties)} method
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

    private RuleSetCreator _ruleSetCreator = new RuleSetCreator();

    private static final String TEST_USER = "user";

    // Common things that are passed to frame constructors
    private String _queueName = this.getClass().getName() + "queue";
    private String _exchangeName = "amq.direct";
    private String _exchangeType = "direct";
    private Subject _testSubject = TestPrincipalUtils.createTestSubject(TEST_USER);

    @Before
    public void setUp() throws Exception
    {

        _ruleSetCreator = new RuleSetCreator();
    }

    private RuleSet createRuleSet()
    {
        return _ruleSetCreator.createRuleSet(mock(EventLoggerProvider.class));
    }

    private void assertDenyGrantAllow(Subject subject, LegacyOperation operation, ObjectType objectType)
    {
        assertDenyGrantAllow(subject, operation, objectType, ObjectProperties.EMPTY);
    }

    private void assertDenyGrantAllow(Subject subject,
                                      LegacyOperation operation,
                                      ObjectType objectType,
                                      ObjectProperties properties)
    {
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(subject, operation, objectType, properties));
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, operation, objectType, properties);
        ruleSet = createRuleSet();
        assertEquals((long) 1, (long) ruleSet.getRuleCount());
        assertEquals(Result.ALLOWED, ruleSet.check(subject, operation, objectType, properties));
    }

    @Test
    public void testEmptyRuleSet()
    {
        RuleSet ruleSet = createRuleSet();
        assertNotNull(ruleSet);
        assertEquals((long) ruleSet.getRuleCount(), (long) 0);
        assertEquals(ruleSet.getDefault(),
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
    }

    @Test
    public void testVirtualHostNodeCreateAllowPermissionWithVirtualHostName() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.VIRTUALHOSTNODE, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.VIRTUALHOSTNODE, ObjectProperties.EMPTY));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.DELETE, ObjectType.VIRTUALHOSTNODE, ObjectProperties.EMPTY));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithVirtualHostName() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH));
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithNameSetToWildCard() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ObjectProperties.WILD_CARD));

        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithNoName() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessDenyPermissionWithNoName() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessDenyPermissionWithNameSetToWildCard() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ObjectProperties.WILD_CARD));
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowDenyPermissions() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH));
        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH));
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(ALLOWED_VH)));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties(DENIED_VH)));
    }

    @Test
    public void testVirtualHostAccessAllowPermissionWithVirtualHostNameOtherPredicate() throws Exception
    {
        ObjectProperties properties = new ObjectProperties();
        properties.put(Property.VIRTUALHOST_NAME, ALLOWED_VH);

        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties));
        assertEquals(Result.DEFER, ruleSet.check(_testSubject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST,
                                                        new ObjectProperties(DENIED_VH)));
    }


    @Test
    public void testQueueCreateNamed() throws Exception
    {
        assertDenyGrantAllow(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, new ObjectProperties(_queueName));
    }

    @Test
    public void testQueueCreateNamedVirtualHost() throws Exception
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, new ObjectProperties(Property.VIRTUALHOST_NAME, ALLOWED_VH));
        RuleSet ruleSet = createRuleSet();
        ObjectProperties allowedQueueObjectProperties = new ObjectProperties(_queueName);
        allowedQueueObjectProperties.put(Property.VIRTUALHOST_NAME, ALLOWED_VH);
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, new ObjectProperties(allowedQueueObjectProperties)));

        ObjectProperties deniedQueueObjectProperties = new ObjectProperties(_queueName);
        deniedQueueObjectProperties.put(Property.VIRTUALHOST_NAME, DENIED_VH);
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, deniedQueueObjectProperties));
    }

    @Test
    public void testQueueCreateNamedNullRoutingKey()
    {
        ObjectProperties properties = new ObjectProperties(_queueName);
        properties.put(ObjectProperties.Property.ROUTING_KEY, (String) null);

        assertDenyGrantAllow(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, properties);
    }

    @Test
    public void testExchangeCreateNamedVirtualHost()
    {
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.EXCHANGE, new ObjectProperties(Property.VIRTUALHOST_NAME, ALLOWED_VH));
        RuleSet ruleSet = createRuleSet();
        ObjectProperties allowedExchangeProperties = new ObjectProperties(_exchangeName);
        allowedExchangeProperties.put(Property.TYPE, _exchangeType);
        allowedExchangeProperties.put(Property.VIRTUALHOST_NAME, ALLOWED_VH);

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.EXCHANGE, allowedExchangeProperties));

        ObjectProperties deniedExchangeProperties = new ObjectProperties(_exchangeName);
        deniedExchangeProperties.put(Property.TYPE, _exchangeType);
        deniedExchangeProperties.put(Property.VIRTUALHOST_NAME, DENIED_VH);
        assertEquals(Result.DEFER,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.EXCHANGE, deniedExchangeProperties));
    }

    @Test
    public void testExchangeCreate()
    {
        ObjectProperties properties = new ObjectProperties(_exchangeName);
        properties.put(ObjectProperties.Property.TYPE, _exchangeType);

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
        ObjectProperties temporary = new ObjectProperties();
        temporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);

        ObjectProperties normal = new ObjectProperties();
        normal.put(ObjectProperties.Property.AUTO_DELETE, Boolean.FALSE);

        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));
        _ruleSetCreator.addRule(0, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary);
        ruleSet = createRuleSet();
        assertEquals((long) 1, (long) ruleSet.getRuleCount());
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
        ObjectProperties temporary = new ObjectProperties(_queueName);
        temporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);

        ObjectProperties normal = new ObjectProperties(_queueName);
        normal.put(ObjectProperties.Property.AUTO_DELETE, Boolean.FALSE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));

        // should not matter if the temporary permission is processed first or last
        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CONSUME, ObjectType.QUEUE, normal);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary);
        ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

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
        ObjectProperties temporary = new ObjectProperties(_queueName);
        temporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);

        ObjectProperties normal = new ObjectProperties(_queueName);
        normal.put(ObjectProperties.Property.AUTO_DELETE, Boolean.FALSE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary));

        // should not matter if the temporary permission is processed first or last
        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CONSUME, ObjectType.QUEUE, temporary);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CONSUME, ObjectType.QUEUE, normal);
        ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

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
        ObjectProperties named = new ObjectProperties(_queueName);
        ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);

        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, named);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.DENY, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary);
        ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

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
        ObjectProperties named = new ObjectProperties(_queueName);
        ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.DENY, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, named);
        ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

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
        ObjectProperties named = new ObjectProperties(_queueName);
        ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);
        ObjectProperties namedDurable = new ObjectProperties(_queueName);
        namedDurable.put(ObjectProperties.Property.DURABLE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedDurable));

        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.DENY, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.DENY, LegacyOperation.CREATE, ObjectType.QUEUE, namedDurable);
        _ruleSetCreator.addRule(3, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, named);
        ruleSet = createRuleSet();
        assertEquals((long) 3, (long) ruleSet.getRuleCount());

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
        ObjectProperties named = new ObjectProperties(_queueName);
        ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, named);
        ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));
    }

    @Test
    public void testNamedTemporaryQueueDeniedAllowed()
    {
        ObjectProperties named = new ObjectProperties(_queueName);
        ObjectProperties namedTemporary = new ObjectProperties(_queueName);
        namedTemporary.put(ObjectProperties.Property.AUTO_DELETE, Boolean.TRUE);
        RuleSet ruleSet = createRuleSet();
        assertEquals(Result.DENIED, ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, named));
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary));

        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW, LegacyOperation.CREATE, ObjectType.QUEUE, namedTemporary);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.DENY, LegacyOperation.CREATE, ObjectType.QUEUE, named);
        ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

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
        _ruleSetCreator.addRule(1, Rule.ALL, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals((long) 1, (long) ruleSet.getRuleCount());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("usera"), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("userb"), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
    }

    @Test
    public void testGroupsSupported()
    {
        String allowGroup = "allowGroup";
        String deniedGroup = "deniedGroup";

        _ruleSetCreator.addRule(1, allowGroup, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        _ruleSetCreator.addRule(2, deniedGroup, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("usera", allowGroup), LegacyOperation
                                    .ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
        assertEquals(Result.DENIED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("userb", deniedGroup), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
        assertEquals(Result.DEFER,
                            ruleSet.check(TestPrincipalUtils.createTestSubject("user", "group not mentioned in acl"), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
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

        _ruleSetCreator.addRule(1, user, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        _ruleSetCreator.addRule(2, group, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

        assertEquals(Result.ALLOWED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject(user, group), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
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

        _ruleSetCreator.addRule(1, group, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        _ruleSetCreator.addRule(2, user, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);


        RuleSet ruleSet = createRuleSet();
        assertEquals((long) 2, (long) ruleSet.getRuleCount());

        assertEquals(Result.DENIED,
                            ruleSet.check(TestPrincipalUtils.createTestSubject(user, group), LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
    }

    @Test
    public void testUserInMultipleGroups()
    {
        String allowedGroup = "group1";
        String deniedGroup = "group2";

        _ruleSetCreator.addRule(1, allowedGroup, RuleOutcome.ALLOW, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        _ruleSetCreator.addRule(2, deniedGroup, RuleOutcome.DENY, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        Subject subjectInBothGroups = TestPrincipalUtils.createTestSubject("user", allowedGroup, deniedGroup);
        Subject subjectInDeniedGroupAndOneOther = TestPrincipalUtils.createTestSubject("user", deniedGroup, "some other group");
        Subject subjectInAllowedGroupAndOneOther = TestPrincipalUtils.createTestSubject("user", allowedGroup, "some other group");

        assertEquals(Result.ALLOWED,
                            ruleSet.check(subjectInBothGroups, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));

        assertEquals(Result.DENIED,
                            ruleSet.check(subjectInDeniedGroupAndOneOther, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));

        assertEquals(Result.ALLOWED,
                            ruleSet.check(subjectInAllowedGroupAndOneOther, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY));
    }

    @Test
    public void testUpdatedAllowedAttribute()
    {
        final ObjectProperties ruleProperties = new ObjectProperties();
        ruleProperties.setAttributeNames(Collections.singleton("attribute1"));
        _ruleSetCreator.addRule(1, TEST_USER, RuleOutcome.ALLOW,  LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, ruleProperties);
        _ruleSetCreator.addRule(2, TEST_USER, RuleOutcome.DENY,  LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();

        final ObjectProperties updateProperties = new ObjectProperties();
        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, updateProperties));

        updateProperties.setAttributeNames(Collections.singleton("attribute2"));

        assertEquals(Result.DENIED,
                            ruleSet.check(_testSubject, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, updateProperties));

        updateProperties.setAttributeNames(Collections.singleton("attribute1"));
        assertEquals(Result.ALLOWED,
                            ruleSet.check(_testSubject, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, updateProperties));
    }

    @Test
    public void testExistingObjectOwner()
    {
        _ruleSetCreator.addRule(1,
                                Rule.OWNER,
                                RuleOutcome.ALLOW,
                                LegacyOperation.CONSUME,
                                ObjectType.QUEUE,
                                ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals((long) 1, (long) ruleSet.getRuleCount());

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
        _ruleSetCreator.addRule(1,
                                Rule.OWNER,
                                RuleOutcome.ALLOW,
                                LegacyOperation.ALL,
                                ObjectType.QUEUE,
                                ObjectProperties.EMPTY);
        RuleSet ruleSet = createRuleSet();
        assertEquals((long) 1, (long) ruleSet.getRuleCount());

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
