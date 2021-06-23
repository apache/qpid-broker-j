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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectionLimitsTest extends UnitTestBase
{
    @Test
    public void testMergeWith_NonBlocking()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, null, Duration.ofMinutes(1L));
        final ConnectionLimits second = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, null, 300, Duration.ofMinutes(2L));

        ConnectionLimits limits = first.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(2L), 300), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.mergeWith(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(2L), 300), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testMergeWith_Smaller()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 50, 200, Duration.ofMinutes(1L));
        final ConnectionLimits second = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 300, Duration.ofMinutes(1L));

        ConnectionLimits limits = first.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(1L), 200), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.mergeWith(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(1L), 200), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testMergeWith_Blocking()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, 20, Duration.ofMinutes(7L));
        final ConnectionLimits second = new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);

        ConnectionLimits limits = first.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(ConnectionLimits.noLimits());
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());
    }

    @Test
    public void testMergeWith_Empty()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, 20, Duration.ofMinutes(3L));
        final ConnectionLimits second = new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final ConnectionLimits empty = ConnectionLimits.noLimits();

        assertTrue(empty.isEmpty());
        assertFalse(empty.isUserBlocked());
        assertNull(empty.getCountLimit());
        assertTrue(empty.getFrequencyLimits().isEmpty());


        ConnectionLimits limits = empty.mergeWith(first);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertEquals(first.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = first.mergeWith(empty);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertEquals(first.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());


        limits = empty.mergeWith(second);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertEquals(second.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(empty);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertEquals(second.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());


        limits = empty.mergeWith(empty);
        assertTrue(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
        assertNull(limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
    }

    @Test
    public void testMergeWith_Null()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, 20, Duration.ofMinutes(1L));
        final ConnectionLimits second = new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final ConnectionLimits empty = ConnectionLimits.noLimits();

        assertEquals(first, first.mergeWith(null));
        assertEquals(second, second.mergeWith(null));
        assertEquals(empty, empty.mergeWith(null));
    }

    @Test
    public void testThen_NonBlocking()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, null, Duration.ofMinutes(2L));
        final ConnectionLimits second = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, null, 300, Duration.ofMinutes(2L));

        ConnectionLimits limits = first.then(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.then(first);
        assertNotNull(limits);
        assertNull(limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(2L), 300), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testThen_Smaller()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 50, 200, Duration.ofMinutes(2L));
        final ConnectionLimits second = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 20, 30, Duration.ofMinutes(3L));

        ConnectionLimits limits = first.then(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(50), limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(2L), 200), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.then(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(3L), 30), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testThen_Blocking()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, null, null);
        final ConnectionLimits second = new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);

        ConnectionLimits limits = first.then(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.then(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());
    }

    @Test
    public void testThen_Empty()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, 20, Duration.ofHours(1L));
        final ConnectionLimits second = new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final ConnectionLimits empty = ConnectionLimits.noLimits();
        final ConnectionLimits empty2 = new ConnectionLimits()
        {
            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Map<Duration, Integer> getFrequencyLimits()
            {
                return Collections.emptyMap();
            }

            @Override
            public boolean isUserBlocked()
            {
                return false;
            }

            @Override
            public boolean isEmpty()
            {
                return true;
            }
        };

        assertTrue(empty.isEmpty());
        assertFalse(empty.isUserBlocked());
        assertNull(empty.getCountLimit());
        assertTrue(empty.getFrequencyLimits().isEmpty());


        ConnectionLimits limits = empty.then(first);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertEquals(first.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = empty2.then(first);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertEquals(first.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());


        limits = empty.then(second);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertEquals(second.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = empty2.then(second);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertEquals(second.getFrequencyLimits(), limits.getFrequencyLimits());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());


        limits = empty.then(empty);
        assertTrue(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
        assertNull(limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());

        limits = empty2.then(empty);
        assertTrue(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
        assertNull(limits.getCountLimit());
        assertTrue(limits.getFrequencyLimits().isEmpty());
    }

    @Test
    public void testThen_Null()
    {
        final ConnectionLimits first = new NonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10, 20, Duration.ofMinutes(17L));
        final ConnectionLimits second = new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS);
        final ConnectionLimits empty = ConnectionLimits.noLimits();

        assertEquals(first, first.then(null));
        assertEquals(second, second.then(null));
        assertEquals(empty, empty.then(null));
    }
}