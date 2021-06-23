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
package org.apache.qpid.server.security.limit;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.qpid.server.security.limit.ConnectionLimiter.CachedLimiter;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectionLimiterTest extends UnitTestBase
{
    private static final String CONNECTION_BREAKS_LIMIT = "Connection breaks limit";

    @Test
    public void testRegister_NoLimits()
    {
        final ConnectionLimiter limiter = ConnectionLimiter.noLimits();
        final AMQPConnection<?> connection = newConnection();
        final ConnectionSlot slot = limiter.register(connection);

        for (int i = 0; i < 127; i++)
        {
            assertEquals(slot, limiter.register(connection));
        }
    }

    @Test
    public void testDeregister_NoLimits()
    {
        final CachedLimiter limiter = ConnectionLimiter.noLimits();
        final AMQPConnection<?> connection = newConnection();
        limiter.register(connection);

        for (int i = 0; i < 127; i++)
        {
            assertTrue(limiter.deregister(connection));
        }
    }

    @Test
    public void testRegister_BlockedUser()
    {
        final ConnectionLimiter limiter = ConnectionLimiter.blockEveryone();
        for (int i = 0; i < 7; i++)
        {
            try
            {
                limiter.register(newConnection());
                fail("A connection limit exception is expected");
            }
            catch (ConnectionLimitException e)
            {
                assertNotNull(e.getMessage());
            }
        }
    }

    @Test
    public void testDeregister_BlockedUser()
    {
        final CachedLimiter limiter = ConnectionLimiter.blockEveryone();
        final AMQPConnection<?> connection = newConnection();
        try
        {
            limiter.register(connection);
            fail("A connection limit exception is expected");
        }
        catch (ConnectionLimitException e)
        {
            assertNotNull(e.getMessage());
        }
        for (int i = 0; i < 7; i++)
        {
            assertFalse(limiter.deregister(connection));
        }
    }

    @Test
    public void testRegister_CachedLimiter()
    {
        final ConnectionLimiterImpl underlyingLimiter = new ConnectionLimiterImpl(1);
        final CachedLimiter limiter = new CachedConnectionLimiterImpl(underlyingLimiter);

        final AMQPConnection<?> connection = newConnection();
        final ConnectionSlot slot1 = limiter.register(connection);
        assertEquals(slot1, limiter.register(connection));
        assertEquals(slot1, limiter.register(connection));
        slot1.free();

        final ConnectionSlot slot2 = limiter.register(connection);
        assertNotEquals(slot1, slot2);
        assertEquals(slot2, limiter.register(connection));
        assertEquals(slot2, limiter.register(connection));

        slot1.free();
        try
        {
            underlyingLimiter.register(connection).free();
            fail("A connection limit exception is expected");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals(CONNECTION_BREAKS_LIMIT, e.getMessage());
        }

        slot2.free();
        underlyingLimiter.register(connection).free();
    }

    @Test
    public void testDeregister_CachedLimiter()
    {
        final CachedLimiter limiter = new CachedConnectionLimiterImpl(new ConnectionLimiterImpl(1));
        final AMQPConnection<?> connection = newConnection();
        limiter.register(connection);

        assertTrue(limiter.deregister(connection));
        assertFalse(limiter.deregister(connection));
        assertFalse(limiter.deregister(connection));
    }

    @Test
    public void testAppend_noLimits()
    {
        final ConnectionLimiter secondary = new ConnectionLimiterImpl(1);
        final ConnectionLimiter noLimits = ConnectionLimiter.noLimits();

        final ConnectionLimiter limiter = noLimits.append(secondary);
        final AMQPConnection<?> connection = newConnection();
        final ConnectionSlot slot = limiter.register(connection);

        try
        {
            limiter.register(connection);
            fail("A connection limit exception is expected here");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals(CONNECTION_BREAKS_LIMIT, e.getMessage());
        }
        slot.free();
        limiter.register(connection).free();
    }

    @Test
    public void testAppend_blockEveryone()
    {
        final ConnectionLimiter secondary = new ConnectionLimiterImpl(1);
        final ConnectionLimiter blocked = ConnectionLimiter.blockEveryone();

        final ConnectionLimiter limiter = blocked.append(secondary);

        for (int i = 0; i < 3; i++)
        {
            try
            {
                limiter.register(newConnection());
                fail("A connection limit exception is expected here");
            }
            catch (ConnectionLimitException e)
            {
                assertNotNull(e.getMessage());
            }
        }
    }

    @Test
    public void testAppend_CachedLimiter()
    {
        final ConnectionLimiter secondary = new ConnectionLimiterImpl(1);
        final ConnectionLimiter cachedLimiter = new CachedConnectionLimiterImpl(new ConnectionLimiterImpl(10));

        final ConnectionLimiter limiter = cachedLimiter.append(secondary);
        final AMQPConnection<?> connection = newConnection();
        final ConnectionSlot slot = limiter.register(connection);

        try
        {
            limiter.register(connection);
            fail("A connection limit exception is expected here");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals(CONNECTION_BREAKS_LIMIT, e.getMessage());
        }
        slot.free();
        limiter.register(connection).free();
    }

    @Test
    public void testFreeSlot_NoLimits()
    {
        final ConnectionLimiter limiter = ConnectionLimiter.noLimits();

        for (int i = 0; i < 127; i++)
        {
            limiter.register(newConnection()).free();
        }
    }

    private AMQPConnection<?> newConnection()
    {
        return Mockito.mock(AMQPConnection.class);
    }

    private static final class ConnectionLimiterImpl implements ConnectionLimiter
    {
        private final Map<AMQPConnection<?>, Integer> _counters;
        private final int _limit;
        private final ConnectionLimiter _subLimiter;

        public ConnectionLimiterImpl(int limit)
        {
            _counters = new HashMap<>();
            _limit = limit;
            _subLimiter = ConnectionLimiter.noLimits();
        }

        private ConnectionLimiterImpl(ConnectionLimiterImpl limiter, ConnectionLimiter subLimiter)
        {
            _counters = limiter._counters;
            _limit = limiter._limit;
            _subLimiter = subLimiter;
        }

        @Override
        public ConnectionSlot register(final AMQPConnection<?> connection)
        {
            int counter = _counters.computeIfAbsent(connection, con -> 0);
            if (counter >= _limit)
            {
                throw new ConnectionLimitException(CONNECTION_BREAKS_LIMIT);
            }
            final ConnectionSlot subSlot = _subLimiter.register(connection);
            _counters.put(connection, counter + 1);
            final ConnectionSlot slot = () -> _counters.put(connection, _counters.get(connection) - 1);
            return slot.chainTo(subSlot);
        }

        @Override
        public ConnectionLimiter append(ConnectionLimiter limiter)
        {
            return new ConnectionLimiterImpl(this, _subLimiter.append(limiter));
        }
    }
}