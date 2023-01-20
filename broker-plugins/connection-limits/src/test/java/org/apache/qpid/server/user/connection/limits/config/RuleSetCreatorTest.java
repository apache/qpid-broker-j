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
package org.apache.qpid.server.user.connection.limits.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class RuleSetCreatorTest extends UnitTestBase
{
    @Test
    public void testAddRule()
    {
        final Rule rule1 = Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final Rule rule2 = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 60, Duration.ofMinutes(1L));
        final RuleSetCreator creator = new RuleSetCreator();

        assertTrue(creator.add(rule1));
        assertTrue(creator.addAll(Collections.singleton(rule2)));
        assertFalse(creator.isEmpty());
        assertEquals(2, creator.size());
        assertTrue(creator.contains(rule1));
        assertTrue(creator.contains(rule2));
        assertTrue(creator.containsAll(Arrays.asList(rule1, rule2)));
    }

    @Test
    public void testAddRule_null()
    {
        final RuleSetCreator creator = new RuleSetCreator();

        assertFalse(creator.add(null));
        assertFalse(creator.addAll(null));
        assertTrue(creator.isEmpty());
    }

    @Test
    public void testRemoveRule()
    {
        final Rule rule1 = Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final Rule rule2 = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 60, Duration.ofMinutes(3L));
        final RuleSetCreator creator = new RuleSetCreator();

        assertTrue(creator.add(rule1));
        assertTrue(creator.add(rule2));
        assertFalse(creator.isEmpty());
        assertEquals(2, creator.size());

        assertTrue(creator.remove(rule1));
        assertEquals(1, creator.size());
        for (final Rule r : creator)
        {
            assertSame(rule2, r);
        }
        assertTrue(creator.removeAll(Collections.singleton(rule2)));
        assertTrue(creator.isEmpty());
    }

    @Test
    public void testClear()
    {
        final Rule rule1 = Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final Rule rule2 = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 60, Duration.ofMinutes(2L));
        final RuleSetCreator creator = new RuleSetCreator();

        assertTrue(creator.add(rule1));
        assertTrue(creator.add(rule2));
        assertEquals(2, creator.size());

        creator.clear();
        assertTrue(creator.isEmpty());
    }

    @Test
    public void testRetainAll()
    {
        final Rule rule1 = Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final Rule rule2 = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 60, Duration.ofMinutes(2L));
        final RuleSetCreator creator = new RuleSetCreator();

        assertTrue(creator.add(rule1));
        assertTrue(creator.add(rule2));
        assertFalse(creator.isEmpty());
        assertEquals(2, creator.size());

        assertTrue(creator.retainAll(Collections.singleton(rule2)));
        assertEquals(1, creator.size());
        for (final Rule r : creator)
        {
            assertSame(rule2, r);
        }
    }

    @Test
    public void testDefaultFrequencyPeriod()
    {
        final RuleSetCreator creator = new RuleSetCreator();
        assertNull(creator.getDefaultFrequencyPeriod());
        assertFalse(creator.isDefaultFrequencyPeriodSet());

        creator.setDefaultFrequencyPeriod(30L);
        assertEquals(Long.valueOf(30L), creator.getDefaultFrequencyPeriod());
        assertTrue(creator.isDefaultFrequencyPeriodSet());
    }

    @Test
    public void testLogAllMessages()
    {
        final RuleSetCreator creator = new RuleSetCreator();
        assertFalse(creator.isLogAllMessages());

        creator.setLogAllMessages(true);
        assertTrue(creator.isLogAllMessages());

        creator.setLogAllMessages(false);
        assertFalse(creator.isLogAllMessages());
    }

    @Test
    public void testGetLimiter()
    {
        final RuleSetCreator creator = new RuleSetCreator();
        final Rule rule = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 60, null);
        creator.add(rule);

        creator.setDefaultFrequencyPeriod(-1L);
        assertNotNull(creator.getLimiter("Limiter"));
        assertNull(rule.getFrequencyPeriod());

        creator.setDefaultFrequencyPeriod(0L);
        assertNotNull(creator.getLimiter("Limiter"));
        assertNull(rule.getFrequencyPeriod());

        creator.setDefaultFrequencyPeriod(400000L);
        assertNotNull(creator.getLimiter("Limiter"));
        assertEquals(Duration.ofMillis(400000L), rule.getFrequencyPeriod());
    }

    @Test
    public void testUpdateRulesWithDefaultFrequencyPeriod()
    {
        final RuleSetCreator creator = new RuleSetCreator();
        final Rule rule = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 60, null);
        creator.add(rule);

        creator.updateRulesWithDefaultFrequencyPeriod();
        assertNull(rule.getFrequencyPeriod());

        creator.setDefaultFrequencyPeriod(-1L);
        creator.updateRulesWithDefaultFrequencyPeriod();
        assertNull(rule.getFrequencyPeriod());

        creator.setDefaultFrequencyPeriod(400000L);
        creator.updateRulesWithDefaultFrequencyPeriod();
        assertEquals(Duration.ofMillis(400000L), rule.getFrequencyPeriod());
    }
}