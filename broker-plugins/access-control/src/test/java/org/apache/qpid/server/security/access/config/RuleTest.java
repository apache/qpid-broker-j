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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.test.utils.UnitTestBase;

public class RuleTest extends UnitTestBase
{
    @Test
    public void testEqualsAndHashCode()
    {
        AclAction aclAction = mock(AclAction.class);
        String identity = "identity";
        RuleOutcome allow = RuleOutcome.ALLOW;

        Rule rule = new Rule(identity, aclAction, allow);
        Rule equalRule = new Rule(identity, aclAction, allow);

        assertTrue(rule.equals(rule));
        assertTrue(rule.equals(equalRule));
        assertTrue(equalRule.equals(rule));

        assertTrue(rule.hashCode() == equalRule.hashCode());

        assertFalse("Different identity should cause rules to be unequal",
                           rule.equals(new Rule("identity2", aclAction, allow)));


        final AclAction differentAclAction = mock(AclAction.class);
        Action action = new Action(LegacyOperation.PURGE);
        Action differentAction = new Action(LegacyOperation.ACCESS);
        when(aclAction.getAction()).thenReturn(action);
        when(differentAclAction.getAction()).thenReturn(differentAction);
        assertFalse("Different action should cause rules to be unequal",
                           rule.equals(new Rule(identity, differentAclAction, allow)));

        assertFalse("Different permission should cause rules to be unequal",
                           rule.equals(new Rule(identity, aclAction, RuleOutcome.DENY)));
    }
}
