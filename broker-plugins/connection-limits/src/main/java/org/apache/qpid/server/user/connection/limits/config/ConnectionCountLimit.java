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

import java.util.Optional;

@FunctionalInterface
public interface ConnectionCountLimit extends CombinableLimit<ConnectionCountLimit>
{
    Integer getCountLimit();

    default boolean isUserBlocked()
    {
        return false;
    }

    @Override
    default boolean isEmpty()
    {
        return getCountLimit() == null;
    }

    @Override
    default ConnectionCountLimit then(ConnectionCountLimit other)
    {
        if (other != null && isEmpty())
        {
            return other;
        }
        return this;
    }

    @Override
    default ConnectionCountLimit mergeWith(ConnectionCountLimit second)
    {
        if (second == null || isUserBlocked())
        {
            return this;
        }
        if (second.isUserBlocked())
        {
            return second;
        }
        final Integer counter = ConnectionLimitsImpl.min(getCountLimit(), second.getCountLimit());
        return () -> counter;
    }

    static ConnectionCountLimit newInstance(Rule rule)
    {
        if (rule.isUserBlocked())
        {
            return blockedUser();
        }
        if (rule.getCountLimit() == null)
        {
            return noLimits();
        }
        final Integer counterLimit = rule.getCountLimit();
        return () -> counterLimit;
    }

    static ConnectionCountLimit noLimits()
    {
        return NoLimits.INSTANCE;
    }

    static ConnectionCountLimit blockedUser()
    {
        return Blocked.INSTANCE;
    }

    final class NoLimits implements ConnectionCountLimit
    {
        static final NoLimits INSTANCE = new NoLimits();

        private NoLimits()
        {
            super();
        }

        @Override
        public Integer getCountLimit()
        {
            return null;
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

        @Override
        public ConnectionCountLimit mergeWith(ConnectionCountLimit second)
        {
            return Optional.ofNullable(second).orElse(this);
        }

        @Override
        public ConnectionCountLimit then(ConnectionCountLimit other)
        {
            return Optional.ofNullable(other).orElse(this);
        }
    }

    final class Blocked implements ConnectionCountLimit
    {
        static final Blocked INSTANCE = new Blocked();

        private Blocked()
        {
            super();
        }

        @Override
        public Integer getCountLimit()
        {
            return 0;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public boolean isUserBlocked()
        {
            return true;
        }

        @Override
        public ConnectionCountLimit then(ConnectionCountLimit other)
        {
            return this;
        }

        @Override
        public ConnectionCountLimit mergeWith(ConnectionCountLimit second)
        {
            return this;
        }
    }
}
