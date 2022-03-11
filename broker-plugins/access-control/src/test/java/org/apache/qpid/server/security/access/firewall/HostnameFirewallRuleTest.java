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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.access.config.FirewallRule;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.RulePredicate;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

import javax.security.auth.Subject;

public class HostnameFirewallRuleTest extends UnitTestBase
{
    private InetAddress _addressNotInRule;

    private HostnameFirewallRule _HostnameFirewallRule;

    @Before
    public void setUp() throws Exception
    {
        _addressNotInRule = InetAddress.getByName("127.0.0.1");
    }

    @Test
    public void testSingleHostname() throws Exception
    {
        String hostnameInRule = "hostnameInRule";
        InetAddress addressWithMatchingHostname = mock(InetAddress.class);
        when(addressWithMatchingHostname.getCanonicalHostName()).thenReturn(hostnameInRule);

        _HostnameFirewallRule = new HostnameFirewallRule(hostnameInRule);

        assertFalse(_HostnameFirewallRule.matches(_addressNotInRule));
        assertTrue(_HostnameFirewallRule.matches(addressWithMatchingHostname));
    }

    @Test
    public void testSingleHostnameWildcard() throws Exception
    {
        String hostnameInRule = ".*FOO.*";
        InetAddress addressWithMatchingHostname = mock(InetAddress.class);
        when(addressWithMatchingHostname.getCanonicalHostName()).thenReturn("xxFOOxx");

        _HostnameFirewallRule = new HostnameFirewallRule(hostnameInRule);

        assertFalse(_HostnameFirewallRule.matches(_addressNotInRule));
        assertTrue(_HostnameFirewallRule.matches(addressWithMatchingHostname));
    }

    @Test
    public void testMultipleHostnames() throws Exception
    {
        String[] hostnamesInRule = new String[] {"hostnameInRule1", "hostnameInRule2"};

        _HostnameFirewallRule = new HostnameFirewallRule(hostnamesInRule);

        assertFalse(_HostnameFirewallRule.matches(_addressNotInRule));
        for (String hostnameInRule : hostnamesInRule)
        {
            InetAddress addressWithMatchingHostname = mock(InetAddress.class);
            when(addressWithMatchingHostname.getCanonicalHostName()).thenReturn(hostnameInRule);

            assertTrue(_HostnameFirewallRule.matches(addressWithMatchingHostname));
        }
    }

    @Test
    public void testEqualsAndHashCode()
    {
        String hostname1 = "hostname1";
        String hostname2 = "hostname2";

        HostnameFirewallRule rule = new HostnameFirewallRule(hostname1, hostname2);
        HostnameFirewallRule equalRule = new HostnameFirewallRule(hostname1, hostname2);

        assertTrue(rule.equals(rule));
        assertTrue(rule.equals(equalRule));
        assertTrue(equalRule.equals(rule));

        assertTrue(rule.hashCode() == equalRule.hashCode());

        assertFalse("Different hostnames should cause rules to be unequal",
                           rule.equals(new HostnameFirewallRule(hostname1, "different-hostname")));

    }

    @Test
    public void testManagementConnectionPrincipals()
    {
        final ManagementConnectionPrincipal managementConnectionPrincipal = mock(ManagementConnectionPrincipal.class);
        when(managementConnectionPrincipal.getRemoteAddress())
            .thenReturn(new InetSocketAddress("127.0.0.1", 8000));

        final ManagementConnectionPrincipal invalidManagementConnectionPrincipal = mock(ManagementConnectionPrincipal.class);
        when(invalidManagementConnectionPrincipal.getRemoteAddress())
            .thenReturn(new InetSocketAddress("127.0.0.3", 8000));

        final Subject subject = new Subject();
        final FirewallRule rule1 = new HostnameFirewallRule("127.0.0.1", "localhost");
        final FirewallRule rule2 = new HostnameFirewallRule("127.0.0.2");

        assertTrue(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertTrue(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        assertTrue(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertTrue(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        final Subject anotherSubject = new Subject(false,
            ImmutableSet.of(
                new UsernamePrincipal("name", mock(AuthenticationProvider.class)), managementConnectionPrincipal
            ),
            Collections.emptySet(),
            Collections.emptySet());

        assertTrue(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertTrue(RulePredicate.any().and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        assertFalse(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertFalse(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        assertFalse(rule2.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertFalse(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        final Subject invalidSubject = new Subject(false,
            ImmutableSet.of(
                new UsernamePrincipal("name", mock(AuthenticationProvider.class)), invalidManagementConnectionPrincipal
            ),
            Collections.emptySet(),
            Collections.emptySet());

        assertFalse(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
        assertFalse(RulePredicate.any().and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));

        assertFalse(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
        assertFalse(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));

        assertFalse(rule2.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
        assertFalse(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), invalidSubject));
    }
}
