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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.mockito.Mockito;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;
import org.apache.qpid.test.utils.UnitTestBase;

class RuleCollectorTest extends UnitTestBase
{
    @Test
    void addRule()
    {
        final RuleCollector creator = new RuleCollector();
        creator.addRule(4, newRule());
        creator.addRule(3, newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE));
        creator.addRule(6, newRule());
        creator.addRule(7, newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE));

        final RuleSet ruleSet = creator.createRuleSet(Mockito.mock(EventLoggerProvider.class));
        assertNotNull(ruleSet);
        assertEquals(2, ruleSet.size());
        assertEquals(newRule(), ruleSet.get(1));
        assertEquals(newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE), ruleSet.get(0));
    }

    @Test
    void addRule_Reorder()
    {
        final RuleCollector creator = new RuleCollector();
        creator.addRule(4, newRule());
        creator.addRule(7, newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE));
        creator.addRule(6, newRule());
        creator.addRule(1, newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE));

        final RuleSet ruleSet = creator.createRuleSet(Mockito.mock(EventLoggerProvider.class));
        assertNotNull(ruleSet);
        assertEquals(2, ruleSet.size());
        assertEquals(newRule(), ruleSet.get(1));
        assertEquals(newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE), ruleSet.get(0));
    }

    @Test
    void isValid()
    {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> newRule(LegacyOperation.DELETE, ObjectType.MANAGEMENT),
                "An exception is required");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void isValidNumber()
    {
        final RuleCollector creator = new RuleCollector();
        creator.addRule(4, newRule());
        creator.addRule(3, newRule(LegacyOperation.PUBLISH, ObjectType.EXCHANGE));
        assertTrue(creator.isValidNumber(5));
        assertFalse(creator.isValidNumber(4));
    }

    private Rule newRule()
    {
        return new Rule.Builder().withOutcome(RuleOutcome.ALLOW).withOperation(LegacyOperation.ACCESS).build();
    }

    private Rule newRule(final LegacyOperation operation, final ObjectType objectType)
    {
        return new Rule.Builder().withOutcome(RuleOutcome.DENY).withOperation(operation).withObject(objectType).build();
    }
}