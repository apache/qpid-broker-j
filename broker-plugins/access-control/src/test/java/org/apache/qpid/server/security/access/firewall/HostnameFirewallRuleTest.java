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

class HostnameFirewallRuleTest extends UnitTestBase
{
    private InetAddress _addressNotInRule;
    private HostnameFirewallRule _HostnameFirewallRule;

    @BeforeEach
    void setUp() throws Exception
    {
        _addressNotInRule = InetAddress.getByName("127.0.0.1");
    }

    @Test
    void singleHostname()
    {
        final String hostnameInRule = "hostnameInRule";
        final InetAddress addressWithMatchingHostname = mock(InetAddress.class);
        when(addressWithMatchingHostname.getCanonicalHostName()).thenReturn(hostnameInRule);

        _HostnameFirewallRule = new HostnameFirewallRule(hostnameInRule);

        assertFalse(_HostnameFirewallRule.matches(_addressNotInRule));
        assertTrue(_HostnameFirewallRule.matches(addressWithMatchingHostname));
    }

    @Test
    void singleHostnameWildcard()
    {
        final String hostnameInRule = ".*FOO.*";
        final InetAddress addressWithMatchingHostname = mock(InetAddress.class);
        when(addressWithMatchingHostname.getCanonicalHostName()).thenReturn("xxFOOxx");

        _HostnameFirewallRule = new HostnameFirewallRule(hostnameInRule);

        assertFalse(_HostnameFirewallRule.matches(_addressNotInRule));
        assertTrue(_HostnameFirewallRule.matches(addressWithMatchingHostname));
    }

    @Test
    void multipleHostnames()
    {
        final String[] hostnamesInRule = new String[] {"hostnameInRule1", "hostnameInRule2"};

        _HostnameFirewallRule = new HostnameFirewallRule(hostnamesInRule);

        assertFalse(_HostnameFirewallRule.matches(_addressNotInRule));
        for (final String hostnameInRule : hostnamesInRule)
        {
            final InetAddress addressWithMatchingHostname = mock(InetAddress.class);
            when(addressWithMatchingHostname.getCanonicalHostName()).thenReturn(hostnameInRule);

            assertTrue(_HostnameFirewallRule.matches(addressWithMatchingHostname));
        }
    }

    @Test
    void equalsAndHashCode()
    {
        final String hostname1 = "hostname1";
        final String hostname2 = "hostname2";

        final HostnameFirewallRule rule = new HostnameFirewallRule(hostname1, hostname2);
        final HostnameFirewallRule equalRule = new HostnameFirewallRule(hostname1, hostname2);

        assertEquals(rule, rule);
        assertEquals(rule, equalRule);
        assertEquals(equalRule, rule);

        assertEquals(rule.hashCode(), equalRule.hashCode());

        assertNotEquals(rule, new HostnameFirewallRule(hostname1, "different-hostname"),
                "Different hostnames should cause rules to be unequal");
    }

    @Test
    void managementConnectionPrincipals()
    {
        final InetAddress inetAddress = mock(InetAddress.class);
        when(inetAddress.getCanonicalHostName()).thenReturn("127.0.0.1");
        final InetSocketAddress inetSocketAddress = mock(InetSocketAddress.class);
        when(inetSocketAddress.getAddress()).thenReturn(inetAddress);

        final ManagementConnectionPrincipal managementConnectionPrincipal = mock(ManagementConnectionPrincipal.class);
        when(managementConnectionPrincipal.getRemoteAddress()).thenReturn(inetSocketAddress);

        final InetAddress invalidInetAddress = mock(InetAddress.class);
        when(invalidInetAddress.getCanonicalHostName()).thenReturn("127.0.0.3");
        final InetSocketAddress invalidInetSocketAddress = mock(InetSocketAddress.class);
        when(invalidInetSocketAddress.getAddress()).thenReturn(invalidInetAddress);

        final ManagementConnectionPrincipal invalidManagementConnectionPrincipal = mock(ManagementConnectionPrincipal.class);
        when(invalidManagementConnectionPrincipal.getRemoteAddress()).thenReturn(invalidInetSocketAddress);

        final Subject subject = new Subject();
        final FirewallRule rule1 = new HostnameFirewallRule("127.0.0.1", "localhost");
        final FirewallRule rule2 = new HostnameFirewallRule("127.0.0.2");

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
