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

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.test.utils.UnitTestBase;

public class RuleCollectorTest extends UnitTestBase
{
    @Test
    public void testAddRule()
    {
        final RuleCollector creator = new RuleCollector();
        creator.addRule(4, newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS));
        creator.addRule(3, newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE));
        creator.addRule(6, newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS));
        creator.addRule(7, newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE));

        RuleSet ruleSet = creator.createRuleSet(Mockito.mock(EventLoggerProvider.class));
        assertNotNull(ruleSet);
        assertEquals(2, ruleSet.size());
        assertEquals(newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS), ruleSet.get(1));
        assertEquals(newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE), ruleSet.get(0));
    }

    @Test
    public void testAddRule_Reorder()
    {
        final RuleCollector creator = new RuleCollector();
        creator.addRule(4, newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS));
        creator.addRule(7, newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE));
        creator.addRule(6, newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS));
        creator.addRule(1, newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE));

        RuleSet ruleSet = creator.createRuleSet(Mockito.mock(EventLoggerProvider.class));
        assertNotNull(ruleSet);
        assertEquals(2, ruleSet.size());
        assertEquals(newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS), ruleSet.get(1));
        assertEquals(newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE), ruleSet.get(0));
    }

    @Test
    public void testIsValid()
    {
        final RuleCollector creator = new RuleCollector();
        try
        {
            creator.addRule(3, newRule(RuleOutcome.DENY, LegacyOperation.DELETE, ObjectType.MANAGEMENT));
            fail("An exception is required");
        }
        catch (IllegalArgumentException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testIsValidNumber()
    {
        final RuleCollector creator = new RuleCollector();
        creator.addRule(4, newRule(RuleOutcome.ALLOW, LegacyOperation.ACCESS));
        creator.addRule(3, newRule(RuleOutcome.DENY, LegacyOperation.PUBLISH, ObjectType.EXCHANGE));
        assertTrue(creator.isValidNumber(5));
        assertFalse(creator.isValidNumber(4));
    }

    private Rule newRule(RuleOutcome outcome, LegacyOperation operation)
    {
        return new Rule.Builder().withOutcome(outcome).withOperation(operation).build();
    }

    private Rule newRule(RuleOutcome outcome, LegacyOperation operation, ObjectType objectType)
    {
        return new Rule.Builder().withOutcome(outcome).withOperation(operation).withObject(objectType).build();
    }
}