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
package org.apache.qpid.server.security.access.firewall;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.access.config.FirewallRule;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.RulePredicate;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

import javax.security.auth.Subject;

class NetworkFirewallRuleTest extends UnitTestBase
{
    private static final String LOCALHOST_IP = "127.0.0.1";
    private static final String OTHER_IP_1 = "192.168.23.1";
    private static final String OTHER_IP_2 = "192.168.23.2";

    private InetAddress _addressNotInRule;
    private NetworkFirewallRule _networkFirewallRule;

    @BeforeEach
    void setUp() throws Exception
    {
        _addressNotInRule = InetAddress.getByName(LOCALHOST_IP);
    }

    @Test
    void ipRule() throws Exception
    {
        final String ipAddressInRule = OTHER_IP_1;

        _networkFirewallRule = new NetworkFirewallRule(ipAddressInRule);

        assertFalse(_networkFirewallRule.matches(_addressNotInRule));
        assertTrue(_networkFirewallRule.matches(InetAddress.getByName(ipAddressInRule)));
    }

    @Test
    void netMask() throws Exception
    {
        final String ipAddressInRule = "192.168.23.0/24";
        _networkFirewallRule = new NetworkFirewallRule(ipAddressInRule);

        assertFalse(_networkFirewallRule.matches(InetAddress.getByName("192.168.24.1")));
        assertTrue(_networkFirewallRule.matches(InetAddress.getByName("192.168.23.0")));
        assertTrue(_networkFirewallRule.matches(InetAddress.getByName("192.168.23.255")));
    }

    @Test
    void wildcard() throws Exception
    {
        // Test xxx.xxx.*

        assertFalse(new NetworkFirewallRule("192.168.*")
             .matches(InetAddress.getByName("192.169.1.0")));

        assertTrue(new NetworkFirewallRule("192.168.*")
            .matches(InetAddress.getByName("192.168.1.0")));

        assertTrue(new NetworkFirewallRule("192.168.*")
            .matches(InetAddress.getByName("192.168.255.255")));

        // Test xxx.xxx.xxx.*

        assertFalse(new NetworkFirewallRule("192.168.1.*")
             .matches(InetAddress.getByName("192.169.2.0")));

        assertTrue(new NetworkFirewallRule("192.168.1.*")
            .matches(InetAddress.getByName("192.168.1.0")));

        assertTrue(new NetworkFirewallRule("192.168.1.*")
            .matches(InetAddress.getByName("192.168.1.255")));
    }

    @Test
    void multipleNetworks() throws Exception
    {
        final String[] ipAddressesInRule = new String[] {OTHER_IP_1, OTHER_IP_2};

        _networkFirewallRule = new NetworkFirewallRule(ipAddressesInRule);

        assertFalse(_networkFirewallRule.matches(_addressNotInRule));
        for (final String ipAddressInRule : ipAddressesInRule)
        {
            assertTrue(_networkFirewallRule.matches(InetAddress.getByName(ipAddressInRule)));
        }
    }

    @Test
    void equalsAndHashCode()
    {
        final NetworkFirewallRule rule = new NetworkFirewallRule(LOCALHOST_IP, OTHER_IP_1);
        final NetworkFirewallRule equalRule = new NetworkFirewallRule(LOCALHOST_IP, OTHER_IP_1);

        assertEquals(rule, rule);
        assertEquals(rule, equalRule);
        assertEquals(equalRule, rule);

        assertEquals(rule.hashCode(), equalRule.hashCode());

        assertNotEquals(rule, new NetworkFirewallRule(LOCALHOST_IP, OTHER_IP_2),
                "Different networks should cause rules to be unequal");

    }

    @Test
    void managementConnectionPrincipals()
    {
        final ManagementConnectionPrincipal managementConnectionPrincipal = mock(ManagementConnectionPrincipal.class);
        when(managementConnectionPrincipal.getRemoteAddress())
                .thenReturn(new InetSocketAddress("192.168.1.1", 8000));

        final ManagementConnectionPrincipal invalidManagementConnectionPrincipal = mock(ManagementConnectionPrincipal.class);
        when(invalidManagementConnectionPrincipal.getRemoteAddress())
                .thenReturn(new InetSocketAddress("192.168.3.1", 8000));

        final Subject subject = new Subject();
        final FirewallRule rule1 = new NetworkFirewallRule("192.168.1.*");
        final FirewallRule rule2 = new NetworkFirewallRule("192.168.2.*");

        assertTrue(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertTrue(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        assertTrue(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertTrue(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        final Subject anotherSubject = new Subject(false,
                Set.of(new UsernamePrincipal("name", mock(AuthenticationProvider.class)), managementConnectionPrincipal),
                Set.of(),
                Set.of());

        assertTrue(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertTrue(RulePredicate.any().and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        assertFalse(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertFalse(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        assertFalse(rule2.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertFalse(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        final Subject invalidSubject = new Subject(false,
                Set.of(new UsernamePrincipal("name", mock(AuthenticationProvider.class)), invalidManagementConnectionPrincipal),
                Set.of(),
                Set.of());

        assertFalse(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
        assertFalse(RulePredicate.any().and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));

        assertFalse(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
        assertFalse(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));

        assertFalse(rule2.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
        assertFalse(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
    }
}
