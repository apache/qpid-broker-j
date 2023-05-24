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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.UnitTestMessageLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * In these tests, the ruleset is configured programmatically rather than from an external file.
 *
 * @see RuleSetTest
 */
class RuleBasedAccessControlTest extends UnitTestBase
{
    private static final String ALLOWED_GROUP = "allowed_group";
    private static final String DENIED_GROUP = "denied_group";
    private static final ObjectProperties EMPTY = new ObjectProperties();

    private RuleBasedAccessControl _plugin = null;  // Class under test
    private UnitTestMessageLogger _messageLogger;
    private EventLogger _eventLogger;

    @BeforeEach
    void setUp()
    {
        _messageLogger = new UnitTestMessageLogger();
        _eventLogger = new EventLogger(_messageLogger);
        _plugin = null;
    }

    private void setUpGroupAccessControl()
    {
        configureAccessControl(createGroupRuleSet());
    }

    private void configureAccessControl(final RuleSet rs)
    {
        _plugin = new RuleBasedAccessControl(rs, BrokerModel.getInstance());
    }

    private RuleSet createGroupRuleSet()
    {
        final EventLoggerProvider provider = mock(EventLoggerProvider.class);
        when(provider.getEventLogger()).thenReturn(_eventLogger);
        final RuleCollector rsc = new RuleCollector();

        // Rule expressed with username
        rsc.addRule(0, new Rule.Builder()
                .withIdentity("user1")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        // Rules expressed with groups
        rsc.addRule(1, new Rule.Builder()
                .withIdentity(ALLOWED_GROUP)
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        rsc.addRule(2, new Rule.Builder()
                .withIdentity(DENIED_GROUP)
                .withOutcome(RuleOutcome.DENY)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());
        // Catch all rule
        rsc.addRule(3, new Rule.Builder()
                .withIdentity(Rule.ALL)
                .withOutcome(RuleOutcome.DENY_LOG)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.VIRTUALHOST)
                .build());

        return rsc.createRuleSet(provider);
    }

    /**
     * ACL plugin must always defer if there is no  subject attached to the thread.
     */
    @Test
    void noSubjectAlwaysDefers()
    {
        setUpGroupAccessControl();
        final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);
        assertEquals(Result.DEFER, result);
    }

    /**
     * Tests that an allow rule expressed with a username allows an operation performed by a thread running
     * with the same username.
     */
    @Test
    void usernameAllowsOperation()
    {
        setUpGroupAccessControl();
        Subject.doAs(TestPrincipalUtils.createTestSubject("user1"), (PrivilegedAction<Object>) () ->
        {
            final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);
            assertEquals(Result.ALLOWED, result);
            return null;
        });
    }

    /**
     * Tests that an allow rule expressed with an <b>ACL groupname</b> allows an operation performed by a thread running
     * by a user who belongs to the same group..
     */
    @Test
    void groupMembershipAllowsOperation()
    {
        setUpGroupAccessControl();

        authoriseAndAssertResult(Result.ALLOWED, "member of allowed group", ALLOWED_GROUP);
        authoriseAndAssertResult(Result.DENIED, "member of denied group", DENIED_GROUP);
        authoriseAndAssertResult(Result.ALLOWED, "another member of allowed group", ALLOWED_GROUP);
    }

    /**
     * Tests that a deny rule expressed with a <b>groupname</b> denies an operation performed by a thread running
     * by a user who belongs to the same group.
     */
    @Test
    void groupMembershipDeniesOperation()
    {
        setUpGroupAccessControl();
        authoriseAndAssertResult(Result.DENIED, "user3", DENIED_GROUP);
    }

    /**
     * Tests that the catch all deny denies the operation and logs with the logging actor.
     */
    @Test
    void catchAllRuleDeniesUnrecognisedUsername()
    {
        setUpGroupAccessControl();
        Subject.doAs(TestPrincipalUtils.createTestSubject("unknown", "unkgroup1", "unkgroup2"),
                (PrivilegedAction<Object>) () ->
                {
                    assertEquals(0, (long) _messageLogger.getLogMessages().size(), "Expecting zero messages before test");

                    final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);
                    assertEquals(Result.DENIED, result);

                    assertEquals(1, (long) _messageLogger.getLogMessages().size(), "Expecting one message before test");
                    assertTrue(_messageLogger.messageContains(0, "ACL-1002"),
                            "Logged message does not contain expected string");
                    return null;
                });
    }

    /**
     * Tests that a grant access method rule allows any access operation to be performed on any component
     */
    @Test
    void authoriseAccessMethodWhenAllAccessOperationsAllowedOnAllComponents()
    {
        final RuleCollector rs = new RuleCollector();
        // grant user4 access right on any method in any component
        rs.addRule(1, new Rule.Builder()
                .withPredicate(Property.NAME, AclRulePredicatesBuilder.WILD_CARD)
                .withIdentity("user4")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.METHOD)
                .build());
        configureAccessControl(rs.createRuleSet(mock(EventLoggerProvider.class)));
        Subject.doAs(TestPrincipalUtils.createTestSubject("user4"), (PrivilegedAction<Object>) () ->
        {
            final ObjectProperties actionProperties = new ObjectProperties("getName");
            actionProperties.put(Property.COMPONENT, "Test");

            final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, actionProperties);
            assertEquals(Result.ALLOWED, result);
            return null;
        });
    }

    /**
     * Tests that a grant access method rule allows any access operation to be performed on a specified component
     */
    @Test
    void authoriseAccessMethodWhenAllAccessOperationsAllowedOnSpecifiedComponent()
    {
        final RuleCollector rs = new RuleCollector();

        // grant user5 access right on any methods in "Test" component
        rs.addRule(1, new Rule.Builder()
                .withPredicate(Property.NAME, AclRulePredicatesBuilder.WILD_CARD)
                .withPredicate(Property.COMPONENT, "Test")
                .withIdentity("user5")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.METHOD)
                .build());
        configureAccessControl(rs.createRuleSet(mock(EventLoggerProvider.class)));
        Subject.doAs(TestPrincipalUtils.createTestSubject("user5"), (PrivilegedAction<Object>) () ->
        {
            final ObjectProperties actionProperties = new ObjectProperties("getName");
            actionProperties.put(Property.COMPONENT, "Test");
            Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, actionProperties);
            assertEquals(Result.ALLOWED, result);

            actionProperties.put(Property.COMPONENT, "Test2");
            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, actionProperties);
            assertEquals(Result.DEFER, result);
            return null;
        });
    }

    @Test
    void access() throws Exception
    {
        final Subject subject = TestPrincipalUtils.createTestSubject("user1");
        final String testVirtualHost = getTestName();
        final InetAddress inetAddress = InetAddress.getLocalHost();
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, 1);

        final AMQPConnection<?> connectionModel = mock(AMQPConnection.class);
        when(connectionModel.getRemoteSocketAddress()).thenReturn(inetSocketAddress);

        subject.getPrincipals().add(new ConnectionPrincipal(connectionModel));

        Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () ->
        {
            final RuleSet mockRuleSet = mock(RuleSet.class);
            final RuleBasedAccessControl accessControl =
                    new RuleBasedAccessControl(mockRuleSet, BrokerModel.getInstance());

            final ObjectProperties properties = new ObjectProperties(testVirtualHost);
            accessControl.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);

            verify(mockRuleSet).check(subject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);
            return null;
        });
    }

    @Test
    void accessIsDeniedIfRuleThrowsException() throws Exception
    {
        final Subject subject = TestPrincipalUtils.createTestSubject("user1");
        final InetAddress inetAddress = InetAddress.getLocalHost();
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, 1);

        final AMQPConnection<?> connectionModel = mock(AMQPConnection.class);
        when(connectionModel.getRemoteSocketAddress()).thenReturn(inetSocketAddress);

        subject.getPrincipals().add(new ConnectionPrincipal(connectionModel));

        Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () ->
        {
            final RuleSet mockRuleSet = mock(RuleSet.class);
            when(mockRuleSet.check(subject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY))
                    .thenThrow(new RuntimeException());

            final RuleBasedAccessControl accessControl =
                    new RuleBasedAccessControl(mockRuleSet, BrokerModel.getInstance());
            final Result result = accessControl.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);

            assertEquals(Result.DENIED, result);
            return null;
        });
    }

    /**
     * Tests that a grant access method rule allows any access operation to be performed on a specified component
     */
    @Test
    void authoriseAccessMethodWhenSpecifiedAccessOperationsAllowedOnSpecifiedComponent()
    {
        final RuleCollector rs = new RuleCollector();

        // grant user6 access right on "getAttribute" method in "Test" component
        rs.addRule(1, new Rule.Builder()
                .withPredicate(Property.NAME, "getAttribute")
                .withPredicate(Property.COMPONENT, "Test")
                .withIdentity("user6")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.METHOD)
                .build());
        configureAccessControl(rs.createRuleSet(mock(EventLoggerProvider.class)));
        Subject.doAs(TestPrincipalUtils.createTestSubject("user6"), (PrivilegedAction<Object>) () ->
        {
            ObjectProperties properties = new ObjectProperties("getAttribute");
            properties.put(Property.COMPONENT, "Test");
            Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            properties.put(Property.COMPONENT, "Test2");
            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.DEFER, result);

            properties = new ObjectProperties("getAttribute2");
            properties.put(Property.COMPONENT, "Test");
            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.DEFER, result);

            return null;
        });
    }

    /**
     * Tests that granting of all method rights on a method allows a specified operation to be performed on any component
     */
    @Test
    void authoriseAccessUpdateMethodWhenAllRightsGrantedOnSpecifiedMethodForAllComponents()
    {
        final RuleCollector rs = new RuleCollector();
        // grant user8 all rights on method queryNames in all component
        rs.addRule(1, new Rule.Builder()
                .withPredicate(Property.NAME, "queryNames")
                .withIdentity("user8")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.METHOD)
                .build());
        configureAccessControl(rs.createRuleSet(mock(EventLoggerProvider.class)));
        Subject.doAs(TestPrincipalUtils.createTestSubject("user8"), (PrivilegedAction<Object>) () ->
        {
            ObjectProperties properties = new ObjectProperties();
            properties.put(Property.COMPONENT, "Test");
            properties.put(Property.NAME, "queryNames");

            Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            result = _plugin.authorise(LegacyOperation.UPDATE, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            properties = new ObjectProperties("getAttribute");
            properties.put(Property.COMPONENT, "Test");
            result = _plugin.authorise(LegacyOperation.UPDATE, ObjectType.METHOD, properties);
            assertEquals(Result.DEFER, result);

            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.DEFER, result);
            return null;
        });
    }

    /**
     * Tests that granting of all method rights allows any operation to be performed on any component
     */
    @Test
    void authoriseAccessUpdateMethodWhenAllRightsGrantedOnAllMethodsInAllComponents()
    {
        final RuleCollector rs = new RuleCollector();

        // grant user9 all rights on any method in all component
        rs.addRule(1, new Rule.Builder()
                .withIdentity("user9")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ALL)
                .withObject(ObjectType.METHOD)
                .build());
        configureAccessControl(rs.createRuleSet(mock(EventLoggerProvider.class)));
        Subject.doAs(TestPrincipalUtils.createTestSubject("user9"), (PrivilegedAction<Object>) () ->
        {
            ObjectProperties properties = new ObjectProperties("queryNames");
            properties.put(Property.COMPONENT, "Test");

            Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            result = _plugin.authorise(LegacyOperation.UPDATE, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            properties = new ObjectProperties("getAttribute");
            properties.put(Property.COMPONENT, "Test");
            result = _plugin.authorise(LegacyOperation.UPDATE, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);
            return null;
        });
    }

    /**
     * Tests that granting of access method rights with mask allows matching operations to be performed on the specified component
     */
    @Test
    void authoriseAccessMethodWhenMatchingAccessOperationsAllowedOnSpecifiedComponent()
    {
        final RuleCollector rs = new RuleCollector();

        // grant user9 all rights on "getAttribute*" methods in Test component
        final Rule rule = new Rule.Builder()
                .withPredicate(Property.COMPONENT, "Test")
                .withPredicate(Property.NAME, "getAttribute*")
                .withIdentity("user9")
                .withOutcome(RuleOutcome.ALLOW)
                .withOperation(LegacyOperation.ACCESS)
                .withObject(ObjectType.METHOD)
                .build();

        rs.addRule(1, rule);
        configureAccessControl(rs.createRuleSet(mock(EventLoggerProvider.class)));
        Subject.doAs(TestPrincipalUtils.createTestSubject("user9"), (PrivilegedAction<Object>) () ->
        {
            ObjectProperties properties = new ObjectProperties("getAttributes");
            properties.put(Property.COMPONENT, "Test");
            Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            properties = new ObjectProperties("getAttribute");
            properties.put(Property.COMPONENT, "Test");
            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.ALLOWED, result);

            properties = new ObjectProperties("getAttribut");
            properties.put(Property.COMPONENT, "Test");
            result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, properties);
            assertEquals(Result.DEFER, result);
            return null;
        });
    }

    private void authoriseAndAssertResult(final Result expectedResult, final String userName, final String... groups)
    {
        Subject.doAs(TestPrincipalUtils.createTestSubject(userName, groups), (PrivilegedAction<Object>) () ->
        {
            final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);
            assertEquals(expectedResult, result);
            return null;
        });
    }
}
