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
import org.apache.qpid.server.security.access.config.predicates.AclRulePredicates;
import org.apache.qpid.server.security.access.config.predicates.AclRulePredicatesBuilder;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * In these tests, the ruleset is configured programmatically rather than from an external file.
 *
 * @see RuleSetTest
 */
public class RuleBasedAccessControlTest extends UnitTestBase
{
    private static final String ALLOWED_GROUP = "allowed_group";
    private static final String DENIED_GROUP = "denied_group";

    private static final ObjectProperties EMPTY = new ObjectProperties();

    private RuleBasedAccessControl _plugin = null;  // Class under test
    private UnitTestMessageLogger _messageLogger;
    private EventLogger _eventLogger;

    @Before
    public void setUp() throws Exception
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
        RuleCollector rsc = new RuleCollector();

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
    public void testNoSubjectAlwaysDefers()
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
    public void testUsernameAllowsOperation()
    {
        setUpGroupAccessControl();
        Subject.doAs(TestPrincipalUtils.createTestSubject("user1"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);

                assertEquals(Result.ALLOWED, result);
                return null;
            }
        });
    }

    /**
     * Tests that an allow rule expressed with an <b>ACL groupname</b> allows an operation performed by a thread running
     * by a user who belongs to the same group..
     */
    @Test
    public void testGroupMembershipAllowsOperation()
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
    public void testGroupMembershipDeniesOperation()
    {
        setUpGroupAccessControl();
        authoriseAndAssertResult(Result.DENIED, "user3", DENIED_GROUP);
    }

    /**
     * Tests that the catch all deny denies the operation and logs with the logging actor.
     */
    @Test
    public void testCatchAllRuleDeniesUnrecognisedUsername()
    {
        setUpGroupAccessControl();
        Subject.doAs(TestPrincipalUtils.createTestSubject("unknown", "unkgroup1", "unkgroup2"),
                     new PrivilegedAction<Object>()
                     {
                         @Override
                         public Object run()
                         {
                             assertEquals("Expecting zero messages before test",
                                                 (long) 0,
                                                 (long) _messageLogger.getLogMessages().size());

                             final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST,
                                                                     EMPTY);
                             assertEquals(Result.DENIED, result);

                             assertEquals("Expecting one message before test",
                                                 (long) 1,
                                                 (long) _messageLogger.getLogMessages().size());
                             assertTrue("Logged message does not contain expected string",
                                               _messageLogger.messageContains(0, "ACL-1002"));
                             return null;
                         }
                     });

    }

    /**
     * Tests that a grant access method rule allows any access operation to be performed on any component
     */
    @Test
    public void testAuthoriseAccessMethodWhenAllAccessOperationsAllowedOnAllComponents()
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
        Subject.doAs(TestPrincipalUtils.createTestSubject("user4"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                ObjectProperties actionProperties = new ObjectProperties("getName");
                actionProperties.put(Property.COMPONENT, "Test");

                final Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, actionProperties);
                assertEquals(Result.ALLOWED, result);
                return null;
            }
        });

    }

    /**
     * Tests that a grant access method rule allows any access operation to be performed on a specified component
     */
    @Test
    public void testAuthoriseAccessMethodWhenAllAccessOperationsAllowedOnSpecifiedComponent()
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
        Subject.doAs(TestPrincipalUtils.createTestSubject("user5"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                ObjectProperties actionProperties = new ObjectProperties("getName");
                actionProperties.put(Property.COMPONENT, "Test");
                Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, actionProperties);
                assertEquals(Result.ALLOWED, result);

                actionProperties.put(Property.COMPONENT, "Test2");
                result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.METHOD, actionProperties);
                assertEquals(Result.DEFER, result);
                return null;
            }
        });


    }

    @Test
    public void testAccess() throws Exception
    {
        final Subject subject = TestPrincipalUtils.createTestSubject("user1");
        final String testVirtualHost = getTestName();
        final InetAddress inetAddress = InetAddress.getLocalHost();
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, 1);

        AMQPConnection connectionModel = mock(AMQPConnection.class);
        when(connectionModel.getRemoteSocketAddress()).thenReturn(inetSocketAddress);

        subject.getPrincipals().add(new ConnectionPrincipal(connectionModel));

        Subject.doAs(subject, new PrivilegedExceptionAction<Object>()
        {
            @Override
            public Object run() throws Exception
            {
                RuleSet mockRuleSet = mock(RuleSet.class);

                RuleBasedAccessControl accessControl = new RuleBasedAccessControl(mockRuleSet,
                                                                                  BrokerModel.getInstance());

                ObjectProperties properties = new ObjectProperties(testVirtualHost);
                accessControl.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);

                verify(mockRuleSet).check(subject, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);
                return null;
            }
        });

    }

    @Test
    public void testAccessIsDeniedIfRuleThrowsException() throws Exception
    {
        final Subject subject = TestPrincipalUtils.createTestSubject("user1");
        final InetAddress inetAddress = InetAddress.getLocalHost();
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, 1);

        AMQPConnection connectionModel = mock(AMQPConnection.class);
        when(connectionModel.getRemoteSocketAddress()).thenReturn(inetSocketAddress);

        subject.getPrincipals().add(new ConnectionPrincipal(connectionModel));

        Subject.doAs(subject, new PrivilegedExceptionAction<Object>()
        {
            @Override
            public Object run() throws Exception
            {


                RuleSet mockRuleSet = mock(RuleSet.class);
                when(mockRuleSet.check(
                        subject,
                        LegacyOperation.ACCESS,
                        ObjectType.VIRTUALHOST,
                        EMPTY)).thenThrow(new RuntimeException());

                RuleBasedAccessControl accessControl = new RuleBasedAccessControl(mockRuleSet,
                                                                                  BrokerModel.getInstance());
                Result result = accessControl.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);

                assertEquals(Result.DENIED, result);
                return null;
            }
        });

    }


    /**
     * Tests that a grant access method rule allows any access operation to be performed on a specified component
     */
    @Test
    public void testAuthoriseAccessMethodWhenSpecifiedAccessOperationsAllowedOnSpecifiedComponent()
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
        Subject.doAs(TestPrincipalUtils.createTestSubject("user6"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
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
            }
        });

    }

    /**
     * Tests that granting of all method rights on a method allows a specified operation to be performed on any component
     */
    @Test
    public void testAuthoriseAccessUpdateMethodWhenAllRightsGrantedOnSpecifiedMethodForAllComponents()
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
        Subject.doAs(TestPrincipalUtils.createTestSubject("user8"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
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
            }
        });


    }

    /**
     * Tests that granting of all method rights allows any operation to be performed on any component
     */
    @Test
    public void testAuthoriseAccessUpdateMethodWhenAllRightsGrantedOnAllMethodsInAllComponents()
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
        Subject.doAs(TestPrincipalUtils.createTestSubject("user9"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
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
            }
        });


    }

    /**
     * Tests that granting of access method rights with mask allows matching operations to be performed on the specified component
     */
    @Test
    public void testAuthoriseAccessMethodWhenMatchingAccessOperationsAllowedOnSpecifiedComponent()
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
        Subject.doAs(TestPrincipalUtils.createTestSubject("user9"), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
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
            }
        });
    }

    private void authoriseAndAssertResult(final Result expectedResult, String userName, String... groups)
    {

        Subject.doAs(TestPrincipalUtils.createTestSubject(userName, groups), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                Result result = _plugin.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, EMPTY);
                assertEquals(expectedResult, result);
                return null;
            }
        });

    }
}
