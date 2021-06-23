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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.server.security.limit.ConnectionLimiter.CachedLimiter;
import org.apache.qpid.server.transport.AMQPConnection;

public class CachedConnectionLimiterImpl implements CachedLimiter
{
    private final Map<AMQPConnection<?>, ConnectionSlot> _cache = new ConcurrentHashMap<>();

    private final AtomicReference<ConnectionLimiter> _limiter;

    public CachedConnectionLimiterImpl(ConnectionLimiter limiter)
    {
        super();
        _limiter = new AtomicReference<>(Objects.requireNonNull(limiter));
    }

    @Override
    public ConnectionSlot register(AMQPConnection<?> connection)
    {
        return _cache.computeIfAbsent(connection, this::newConnectionSlot);
    }

    @Override
    public boolean deregister(AMQPConnection<?> connection)
    {
        final ConnectionSlot connectionSlot = _cache.get(connection);
        if (connectionSlot != null)
        {
            connectionSlot.free();
            return true;
        }
        return false;
    }

    @Override
    public ConnectionLimiter append(ConnectionLimiter limiter)
    {
        return _limiter.get().append(limiter);
    }

    protected void swapLimiter(ConnectionLimiter limiter)
    {
        _limiter.set(Objects.requireNonNull(limiter));
    }

    private ConnectionSlot newConnectionSlot(AMQPConnection<?> connection)
    {
        return new ConnectionSlotImpl(connection, _limiter.get().register(connection));
    }

    private final class ConnectionSlotImpl implements ConnectionSlot
    {
        private final AMQPConnection<?> _connection;

        private final ConnectionSlot _slot;

        ConnectionSlotImpl(AMQPConnection<?> connection, ConnectionSlot slot)
        {
            _connection = Objects.requireNonNull(connection);
            _slot = Objects.requireNonNull(slot);
        }

        @Override
        public void free()
        {
            if (_cache.remove(_connection, this))
            {
                _slot.free();
            }
        }
    }
}
