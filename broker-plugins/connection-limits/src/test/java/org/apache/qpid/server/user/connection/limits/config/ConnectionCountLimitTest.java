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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ConnectionCountLimitTest extends UnitTestBase
{
    @Test
    public void testMergeWith_NonBlocking()
    {
        final ConnectionCountLimit first = () -> null;
        final ConnectionCountLimit second = () -> 20;

        ConnectionCountLimit limits = first.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.mergeWith(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testMergeWith_Smaller()
    {
        final ConnectionCountLimit first = () -> 10;
        final ConnectionCountLimit second = () -> 20;

        ConnectionCountLimit limits = first.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.mergeWith(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testMergeWith_Blocking()
    {
        final ConnectionCountLimit first = () -> 10;
        final ConnectionCountLimit second = ConnectionCountLimit.blockedUser();

        ConnectionCountLimit limits = first.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());
    }

    @Test
    public void testMergeWith_Empty()
    {
        final ConnectionCountLimit first = () -> 10;
        final ConnectionCountLimit second = ConnectionCountLimit.blockedUser();
        final ConnectionCountLimit empty = ConnectionCountLimit.noLimits();

        assertTrue(empty.isEmpty());
        assertFalse(empty.isUserBlocked());
        assertNull(empty.getCountLimit());


        ConnectionCountLimit limits = empty.mergeWith(first);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = first.mergeWith(empty);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());


        limits = empty.mergeWith(second);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(empty);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());


        limits = empty.mergeWith(empty);
        assertTrue(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
        assertNull(limits.getCountLimit());
    }

    @Test
    public void testMergeWith_Null()
    {
        final ConnectionCountLimit first = () -> 10;
        final ConnectionCountLimit second = ConnectionCountLimit.blockedUser();
        final ConnectionCountLimit empty = ConnectionCountLimit.noLimits();

        assertEquals(first, first.mergeWith(null));
        assertEquals(second, second.mergeWith(null));
        assertEquals(empty, empty.mergeWith(null));
    }

    @Test
    public void testThen_NonBlocking()
    {
        final ConnectionCountLimit first = () -> null;
        final ConnectionCountLimit second = () -> 10;

        ConnectionCountLimit limits = first.then(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.then(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testThen_Smaller()
    {
        final ConnectionCountLimit first = () -> 20;
        final ConnectionCountLimit second = () -> 10;

        ConnectionCountLimit limits = first.then(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.then(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(10), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
    }

    @Test
    public void testThen_Blocking()
    {
        final ConnectionCountLimit first = () -> 20;
        final ConnectionCountLimit second = ConnectionCountLimit.blockedUser();

        ConnectionCountLimit limits = first.then(second);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(20), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = second.then(first);
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = second.mergeWith(ConnectionCountLimit.noLimits());
        assertNotNull(limits);
        assertEquals(Integer.valueOf(0), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());
    }

    @Test
    public void testThen_Empty()
    {
        final ConnectionCountLimit first = () -> 20;
        final ConnectionCountLimit second = ConnectionCountLimit.blockedUser();
        final ConnectionCountLimit empty = ConnectionCountLimit.noLimits();
        final ConnectionCountLimit empty2 = () -> null;

        assertTrue(empty.isEmpty());
        assertFalse(empty.isUserBlocked());
        assertNull(empty.getCountLimit());

        assertTrue(empty2.isEmpty());
        assertFalse(empty2.isUserBlocked());
        assertNull(empty2.getCountLimit());


        ConnectionCountLimit limits = empty.then(first);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());

        limits = empty2.then(first);
        assertNotNull(limits);
        assertEquals(first.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertFalse(limits.isUserBlocked());


        limits = empty.then(second);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());

        limits = empty2.then(second);
        assertNotNull(limits);
        assertEquals(second.getCountLimit(), limits.getCountLimit());
        assertFalse(limits.isEmpty());
        assertTrue(limits.isUserBlocked());


        limits = empty.then(empty);
        assertTrue(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
        assertNull(limits.getCountLimit());

        limits = empty2.then(empty);
        assertTrue(limits.isEmpty());
        assertFalse(limits.isUserBlocked());
        assertNull(limits.getCountLimit());
    }

    @Test
    public void testThen_Null()
    {
        final ConnectionCountLimit first = () -> 20;
        final ConnectionCountLimit second = ConnectionCountLimit.blockedUser();
        final ConnectionCountLimit empty = ConnectionCountLimit.noLimits();

        assertEquals(first, first.then(null));
        assertEquals(second, second.then(null));
        assertEquals(empty, empty.then(null));
    }

    @Test
    public void testNewInstance()
    {
        final ConnectionCountLimit first = ConnectionCountLimit.newInstance(
                new NonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 50, 200, null));
        assertNotNull(first);
        assertEquals(Integer.valueOf(50), first.getCountLimit());
        assertFalse(first.isUserBlocked());
        assertFalse(first.isEmpty());

        final ConnectionCountLimit second = ConnectionCountLimit.newInstance(
                new NonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, null, 200, null));
        assertNotNull(second);
        assertNull(second.getCountLimit());
        assertFalse(second.isUserBlocked());
        assertTrue(second.isEmpty());

        final ConnectionCountLimit third = ConnectionCountLimit.newInstance(new BlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));
        assertNotNull(third);
        assertEquals(Integer.valueOf(0), third.getCountLimit());
        assertTrue(third.isUserBlocked());
        assertFalse(third.isEmpty());
    }
}