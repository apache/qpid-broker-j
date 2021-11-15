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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.AMQPConnection_1_0;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.security.limit.ConnectionLimiterService;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.AMQPConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PluggableService
public class StrongConnectionEstablishmentLimiter implements ConnectionLimiterService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StrongConnectionEstablishmentLimiter.class);

    private final Map<String, UsageCounter> _slots;

    private final ConnectionLimiter _underlyingLimiter;

    public StrongConnectionEstablishmentLimiter()
    {
        super();
        _slots = new ConcurrentHashMap<>();
        _underlyingLimiter = ConnectionLimiter.noLimits();
    }

    private StrongConnectionEstablishmentLimiter(StrongConnectionEstablishmentLimiter limiter, ConnectionLimiter underlyingLimiter)
    {
        super();
        _slots = limiter._slots;
        _underlyingLimiter = Objects.requireNonNull(underlyingLimiter);
    }

    @Override
    public String getType()
    {
        return "EstablishmentPolicy." + SoleConnectionDetectionPolicy.STRONG;
    }

    @Override
    public ConnectionSlot register(AMQPConnection<?> connection)
    {
        if (!(connection instanceof AMQPConnection_1_0) || connection.isClosing())
        {
            return _underlyingLimiter.register(connection);
        }
        LOGGER.debug("Registering a new connection '{}'", connection);
        final AMQPConnection_1_0<?> newConnection = (AMQPConnection_1_0<?>) connection;
        final String remoteContainerId = newConnection.getRemoteContainerId();
        if (remoteContainerId == null)
        {
            // 'container-id' is the mandatory field of open frame but could be null in integration or JUnit tests,
            // e.g. when AMQPConnection_1_0 is mocked in a test.
            LOGGER.warn(
                    "The connection '{}' without container ID, 'container-id' is the mandatory field of open frame",
                    connection);
            return _underlyingLimiter.register(connection);
        }

        LOGGER.debug("Checking a container slot for the connection '{}'", connection);
        try
        {
            return _slots.compute(remoteContainerId,
                            (containerId, counter) -> counter == null ? newUsageCounter(containerId) : counter.addUser())
                    .registerConnection(newConnection);
        }
        catch (RuntimeException e)
        {
            LOGGER.debug("Registering connection failed", e);
            deregisterUser(remoteContainerId);
            throw e;
        }
    }

    private void deregisterUser(final String containerId)
    {
        _slots.computeIfPresent(containerId, (id, slot) -> slot.removeUser());
    }

    private UsageCounter newUsageCounter(String containerId)
    {
        return new UsageCounter(new RemoteContainerSlot(containerId), 1L);
    }

    @Override
    public ConnectionLimiter append(ConnectionLimiter limiter)
    {
        return new StrongConnectionEstablishmentLimiter(this, _underlyingLimiter.append(limiter));
    }

    private static final class UsageCounter
    {
        private final long _counter;

        private final RemoteContainerSlot _slot;

        UsageCounter(RemoteContainerSlot slot, long counter)
        {
            _counter = counter;
            _slot = Objects.requireNonNull(slot);
        }

        public ConnectionSlot registerConnection(final AMQPConnection_1_0<?> connection)
        {
            return _slot.register(connection);
        }

        public UsageCounter addUser()
        {
            return new UsageCounter(_slot, _counter + 1L);
        }

        public UsageCounter removeUser()
        {
            return _counter <= 1 ? null : new UsageCounter(_slot, _counter - 1L);
        }
    }

    private final class RemoteContainerSlot
    {
        private final String _containerId;

        private final Set<AMQPConnection_1_0<?>> _connections;

        RemoteContainerSlot(String containerId)
        {
            super();
            _connections = new HashSet<>();
            _containerId = Objects.requireNonNull(containerId);
        }

        private synchronized ConnectionSlot register(final AMQPConnection_1_0<?> connection)
        {
            final SoleConnectionEnforcementPolicy soleConnectionPolicy = extractPolicy(connection);

            if (soleConnectionPolicy != null && !_connections.isEmpty())
            {
                LOGGER.debug("Single connection is required, sole connection policy: {}", soleConnectionPolicy);
                throw new SoleConnectionEnforcementPolicyException(soleConnectionPolicy, _connections);
            }

            final ConnectionSlot underlyingSlot = _underlyingLimiter.register(connection);
            _connections.add(connection);
            final ConnectionSlot slot = () ->
            {
                try
                {
                    remove(connection);
                }
                finally
                {
                    deregisterUser(_containerId);
                }
            };
            return slot.chainTo(underlyingSlot);
        }

        private SoleConnectionEnforcementPolicy extractPolicy(AMQPConnection_1_0<?> connection)
        {
            if (_connections.isEmpty())
            {
                return connection.getSoleConnectionEnforcementPolicy();
            }
            SoleConnectionEnforcementPolicy soleConnectionPolicy = null;

            final Iterator<AMQPConnection_1_0<?>> iterator = _connections.iterator();
            while (iterator.hasNext())
            {
                final AMQPConnection_1_0<?> existingConnection = iterator.next();
                if (existingConnection.isClosing())
                {
                    iterator.remove();
                }
                else
                {
                    soleConnectionPolicy = existingConnection.getSoleConnectionEnforcementPolicy();
                }
            }
            if (soleConnectionPolicy == null)
            {
                return connection.getSoleConnectionEnforcementPolicy();
            }
            return soleConnectionPolicy;
        }

        private synchronized void remove(final AMQPConnection_1_0<?> connection)
        {
            _connections.remove(connection);
        }
    }
}
