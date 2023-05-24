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
package org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.qpid.server.protocol.v1_0.AMQPConnection_1_0;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.mockito.Mockito;

class StrongConnectionEstablishmentLimiterTest extends UnitTestBase
{
    private StrongConnectionEstablishmentLimiter _limiter;

    private Registry _registry;

    @BeforeAll
    void setUp()
    {
        _registry = new Registry();
        _limiter = (StrongConnectionEstablishmentLimiter) new StrongConnectionEstablishmentLimiter().append(_registry);
    }

    @Test
    void type()
    {
        assertEquals("EstablishmentPolicy.strong", _limiter.getType());
    }

    @Test
    void noPolicy()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", null);
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        final AMQPConnection_1_0<?> connection2 = newConnection("C", null);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        final AMQPConnection_1_0<?> connection3 = newConnection("C", null);
        final ConnectionSlot slot3 = _limiter.register(connection3);
        assertTrue(_registry.isRegistered(connection3));

        slot3.free();
        assertFalse(_registry.isRegistered(connection3));
        assertTrue(_registry.hasBeenRegistered(connection3));

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    @Test
    void newConnectionWithPolicy()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", null);
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        final AMQPConnection_1_0<?> connection2 = newConnection("C", null);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        final AMQPConnection_1_0<?> connection3 = newConnection("C", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);

        final SoleConnectionEnforcementPolicyException thrown = assertThrows(SoleConnectionEnforcementPolicyException.class,
                () -> _limiter.register(connection3),
                "A sole connection enforcement policy exception is expected");

        assertEquals("Single connection with container ID 'C' is required due to sole connection enforcement policy 'refuse-connection'",
                thrown.getMessage());

        assertEquals(2, thrown.getExistingConnections().size());
        assertTrue(thrown.getExistingConnections().contains(connection1));
        assertTrue(thrown.getExistingConnections().contains(connection2));
        assertEquals(SoleConnectionEnforcementPolicy.REFUSE_CONNECTION, thrown.getPolicy());

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    @Test
    void existingConnectionWithPolicy()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        final AMQPConnection_1_0<?> connection2 = newConnection("C", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);

        final SoleConnectionEnforcementPolicyException thrown = assertThrows(SoleConnectionEnforcementPolicyException.class,
                () -> _limiter.register(connection2),
                "A sole connection enforcement policy exception is expected");

        assertEquals("Single connection with container ID 'C' is required due to sole connection enforcement policy 'close-existing'",
                thrown.getMessage());

        assertEquals(1, thrown.getExistingConnections().size());
        assertTrue(thrown.getExistingConnections().contains(connection1));
        assertEquals(SoleConnectionEnforcementPolicy.CLOSE_EXISTING, thrown.getPolicy());

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    @Test
    void existingClosedConnectionWithPolicy()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);
        doReturn(false).when(connection1).isClosing();
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        doReturn(true).when(connection1).isClosing();
        final AMQPConnection_1_0<?> connection2 = newConnection("C", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));
    }

    @Test
    void closedConnection()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        final AMQPConnection_1_0<?> connection2 = newConnection("C", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);
        doReturn(true).when(connection1).isClosing();
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));
    }

    @Test
    void newConnectionWithPolicy_ClosedExisting()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);
        doReturn(false).when(connection1).isClosing();
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        doReturn(true).when(connection1).isClosing();
        final AMQPConnection_1_0<?> connection2 = newConnection("C", null);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        final AMQPConnection_1_0<?> connection3 = newConnection("C", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);

        final SoleConnectionEnforcementPolicyException thrown = assertThrows(SoleConnectionEnforcementPolicyException.class,
                () -> _limiter.register(connection3),
                "A sole connection enforcement policy exception is expected");

        assertEquals("Single connection with container ID 'C' is required due to sole connection enforcement policy 'close-existing'",
                thrown.getMessage());

        assertEquals(1, thrown.getExistingConnections().size());
        assertTrue(thrown.getExistingConnections().contains(connection2));
        assertEquals(SoleConnectionEnforcementPolicy.CLOSE_EXISTING, thrown.getPolicy());

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    @Test
    void newConnectionWithPolicy2_ClosedExisting()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);
        doReturn(false).when(connection1).isClosing();
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        doReturn(true).when(connection1).isClosing();
        final AMQPConnection_1_0<?> connection2 = newConnection("C", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        final AMQPConnection_1_0<?> connection3 = newConnection("C", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);

        final SoleConnectionEnforcementPolicyException thrown = assertThrows(SoleConnectionEnforcementPolicyException.class,
                () -> _limiter.register(connection3),
                "A sole connection enforcement policy exception is expected");

        assertEquals("Single connection with container ID 'C' is required due to sole connection enforcement policy 'refuse-connection'",
                thrown.getMessage());

        assertEquals(1, thrown.getExistingConnections().size());
        assertTrue(thrown.getExistingConnections().contains(connection2));
        assertEquals(SoleConnectionEnforcementPolicy.REFUSE_CONNECTION, thrown.getPolicy());

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    @Test
    void anotherConnectionType()
    {
        final AMQPConnection<?> connection = Mockito.mock(AMQPConnection.class);
        final ConnectionSlot slot = _limiter.register(connection);
        assertTrue(_registry.isRegistered(connection));
        slot.free();
        assertFalse(_registry.isRegistered(connection));
        assertTrue(_registry.hasBeenRegistered(connection));
        verifyNoInteractions(connection);
    }

    @Test
    void multipleIndependentConnections()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection("C1", null);
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        final AMQPConnection_1_0<?> connection2 = newConnection("C2", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        final AMQPConnection_1_0<?> connection3 = newConnection("C3", SoleConnectionEnforcementPolicy.CLOSE_EXISTING);
        final ConnectionSlot slot3 = _limiter.register(connection3);
        assertTrue(_registry.isRegistered(connection3));

        slot3.free();
        assertFalse(_registry.isRegistered(connection3));
        assertTrue(_registry.hasBeenRegistered(connection3));

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    @Test
    void multipleIndependentConnections2()
    {
        final AMQPConnection_1_0<?> connection1 = newConnection(null, null);
        final ConnectionSlot slot1 = _limiter.register(connection1);
        assertTrue(_registry.isRegistered(connection1));

        final AMQPConnection_1_0<?> connection2 = newConnection(null, null);
        final ConnectionSlot slot2 = _limiter.register(connection2);
        assertTrue(_registry.isRegistered(connection2));

        final AMQPConnection_1_0<?> connection3 = newConnection(null, null);
        final ConnectionSlot slot3 = _limiter.register(connection3);
        assertTrue(_registry.isRegistered(connection3));

        slot3.free();
        assertFalse(_registry.isRegistered(connection3));
        assertTrue(_registry.hasBeenRegistered(connection3));

        slot2.free();
        assertFalse(_registry.isRegistered(connection2));
        assertTrue(_registry.hasBeenRegistered(connection2));

        slot1.free();
        assertFalse(_registry.isRegistered(connection1));
        assertTrue(_registry.hasBeenRegistered(connection1));
    }

    private AMQPConnection_1_0<?> newConnection(String id, SoleConnectionEnforcementPolicy policy)
    {
        final AMQPConnection_1_0<?> connection = Mockito.mock(AMQPConnection_1_0.class);
        doReturn(id).when(connection).getRemoteContainerId();
        doReturn(policy).when(connection).getSoleConnectionEnforcementPolicy();
        return connection;
    }

    static final class Registry implements ConnectionLimiter
    {
        private final Set<AMQPConnection<?>> _registered;
        private final Set<AMQPConnection<?>> _connections;
        private final ConnectionLimiter _subLimiter;

        public Registry()
        {
            _registered = new HashSet<>();
            _connections = new HashSet<>();
            _subLimiter = ConnectionLimiter.noLimits();
        }

        private Registry(final Registry limiter, final ConnectionLimiter subLimiter)
        {
            _registered = limiter._registered;
            _connections = limiter._connections;
            _subLimiter = Objects.requireNonNull(subLimiter);
        }

        @Override
        public ConnectionSlot register(final AMQPConnection<?> connection)
        {
            final ConnectionSlot slot = _subLimiter.register(connection);
            _registered.add(connection);
            _connections.add(connection);
            return slot.chainTo(() -> _connections.remove(connection));
        }

        @Override
        public ConnectionLimiter append(final ConnectionLimiter limiter)
        {
            return new Registry(this, _subLimiter.append(limiter));
        }

        public boolean isRegistered(final AMQPConnection<?> connection)
        {
            return _connections.contains(connection);
        }

        public boolean hasBeenRegistered(final AMQPConnection<?> connection)
        {
            return _registered.contains(connection);
        }
    }
}
