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

import java.util.Objects;

import org.apache.qpid.server.security.limit.ConnectionSlot.FreeSlot;
import org.apache.qpid.server.transport.AMQPConnection;

public interface ConnectionLimiter
{
    ConnectionSlot register(AMQPConnection<?> connection);

    ConnectionLimiter append(ConnectionLimiter limiter);

    static CachedLimiter noLimits()
    {
        return NoLimits.INSTANCE;
    }

    static CachedLimiter blockEveryone()
    {
        return BlockEveryone.INSTANCE;
    }

    interface CachedLimiter extends ConnectionLimiter
    {
        boolean deregister(AMQPConnection<?> connection);
    }

    final class NoLimits implements CachedLimiter
    {
        static CachedLimiter INSTANCE = new NoLimits();

        private NoLimits()
        {
            super();
        }

        @Override
        public ConnectionSlot register(AMQPConnection<?> connection)
        {
            return FreeSlot.INSTANCE;
        }

        @Override
        public boolean deregister(AMQPConnection<?> connection)
        {
            return true;
        }

        @Override
        public ConnectionLimiter append(ConnectionLimiter limiter)
        {
            return Objects.requireNonNull(limiter);
        }
    }

    final class BlockEveryone implements CachedLimiter
    {
        static CachedLimiter INSTANCE = new BlockEveryone();

        private BlockEveryone()
        {
            super();
        }

        @Override
        public ConnectionSlot register(AMQPConnection<?> connection)
        {
            throw new ConnectionLimitException("Opening any new connection is forbidden");
        }

        @Override
        public boolean deregister(AMQPConnection<?> connection)
        {
            return false;
        }

        @Override
        public ConnectionLimiter append(ConnectionLimiter limiter)
        {
            Objects.requireNonNull(limiter);
            return this;
        }
    }
}
