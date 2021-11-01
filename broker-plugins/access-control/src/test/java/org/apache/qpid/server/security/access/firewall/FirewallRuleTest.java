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

import java.util.Collections;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.access.config.FirewallRule;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.RulePredicate;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FirewallRuleTest extends UnitTestBase
{
    @Test
    public void testAnd_Match()
    {
        final Subject subject = new Subject();
        final FirewallRule rule1 = s -> s.equals(subject);
        final FirewallRule rule2 = s -> s.equals(subject);

        assertTrue(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertTrue(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        assertTrue(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertTrue(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        final Subject anotherSubject = new Subject(false,
                Collections.singleton(new UsernamePrincipal("name", Mockito.mock(AuthenticationProvider.class))),
                Collections.emptySet(),
                Collections.emptySet());
        assertFalse(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertFalse(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));

        assertFalse(rule1.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
        assertFalse(RulePredicate.any().and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), anotherSubject));
    }

    @Test
    public void testAnd_DoesNotMatch()
    {
        final Subject subject = new Subject();
        final FirewallRule rule1 = s -> s.equals(subject);
        final FirewallRule rule2 = s -> !s.equals(subject);

        assertFalse(rule1.and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertFalse(rule2.and(rule1).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));

        assertFalse(RulePredicate.any().and(rule2).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
        assertFalse(rule2.and(RulePredicate.any()).matches(LegacyOperation.ACCESS, new ObjectProperties(), subject));
    }

}
