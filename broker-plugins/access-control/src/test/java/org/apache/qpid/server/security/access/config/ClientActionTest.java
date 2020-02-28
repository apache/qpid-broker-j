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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.security.access.firewall.FirewallRule;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class ClientActionTest extends UnitTestBase
{
    private Action _action;
    private AclAction _ruleAction;
    private ClientAction _clientAction;
    private Subject _subject;
    private QueueManagingVirtualHost _addressSpace;
    private AMQPConnection _connection;

    @Before
    public void setUp()
    {
        final InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 5672);

        _addressSpace = mock(QueueManagingVirtualHost.class);
        _connection = mock(AMQPConnection.class);
        when(_connection.getAddressSpace()).thenReturn(_addressSpace);

        final ConnectionPrincipal connectionPrincipal = mock(ConnectionPrincipal.class);
        when(connectionPrincipal.getConnection()).thenReturn(_connection);
        when(_connection.getRemoteSocketAddress()).thenReturn(address);
        when(_connection.getAuthorizedPrincipal()).thenReturn(connectionPrincipal);
        final Set<? extends Principal> principals = Collections.singleton(connectionPrincipal);
        _subject = new Subject(false, principals, Collections.emptySet(), Collections.emptySet());
        _ruleAction = mock(AclAction.class);
        when(_ruleAction.getDynamicRule()).thenReturn(subject -> true);
        _action = mock(Action.class);
        _clientAction = new ClientAction(_action);
    }

    @Test
    public void testMatches_returnsTrueWhenActionsMatchAndNoFirewallRule()
    {
        when(_action.matches(any(Action.class))).thenReturn(true);
        when(_ruleAction.getDynamicRule()).thenReturn(null);
        when(_ruleAction.getAction()).thenReturn(mock(Action.class));

        assertTrue(_clientAction.matches(_ruleAction, _subject));
    }

    @Test
    public void testMatches_returnsFalseWhenActionsDontMatch()
    {
        FirewallRule firewallRule = mock(FirewallRule.class);
        when(firewallRule.matches(_subject)).thenReturn(true);
        when(_ruleAction.getDynamicRule()).thenReturn(firewallRule);

        when(_action.matches(any(Action.class))).thenReturn(false);
        when(_ruleAction.getDynamicRule()).thenReturn(firewallRule);
        when(_ruleAction.getAction()).thenReturn(mock(Action.class));

        assertFalse(_clientAction.matches(_ruleAction, _subject));
    }

    @Test
    public void testMatches_returnsTrueWhenActionsAndFirewallRuleMatch()
    {
        FirewallRule firewallRule = mock(FirewallRule.class);
        when(firewallRule.matches(_subject)).thenReturn(true);
        when(_ruleAction.getDynamicRule()).thenReturn(firewallRule);

        when(_action.matches(any(Action.class))).thenReturn(true);
        when(_ruleAction.getDynamicRule()).thenReturn(firewallRule);
        when(_ruleAction.getAction()).thenReturn(mock(Action.class));

        assertTrue(_clientAction.matches(_ruleAction, _subject));
    }

    @Test
    public void testMatches_ignoresFirewallRuleIfClientAddressIsNull()
    {
        FirewallRule firewallRule = new FirewallRule()
        {
            @Override
            protected boolean matches(final InetAddress addressOfClient)
            {
                return false;
            }
        };

        when(_action.matches(any(Action.class))).thenReturn(true);
        when(_ruleAction.getDynamicRule()).thenReturn(firewallRule);
        when(_ruleAction.getAction()).thenReturn(mock(Action.class));

        assertTrue(_clientAction.matches(_ruleAction, _subject));
    }

    @Test
    public void testMatchesWhenConnectionLimitBreached()
    {
        final ObjectProperties properties = new ObjectProperties("foo");
        final AclRulePredicates predicates = new AclRulePredicates();
        predicates.parse(ObjectProperties.Property.CONNECTION_LIMIT.name(), "1");
        final AclAction ruleAction = new AclAction(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, predicates);
        final ClientAction clientAction = new ClientAction(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);

        when(_connection.getAuthenticatedPrincipalConnectionCount()).thenReturn(2);
        boolean matches = clientAction.matches(ruleAction, _subject);
        assertFalse(matches);
    }
}
