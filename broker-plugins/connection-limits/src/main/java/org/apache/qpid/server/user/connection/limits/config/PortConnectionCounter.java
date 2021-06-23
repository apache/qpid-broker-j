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

import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.user.connection.limits.outcome.AcceptRegistration;
import org.apache.qpid.server.user.connection.limits.outcome.RejectRegistration;

final class PortConnectionCounter
{
    private final Function<String, ConnectionCounter> _connectionCounterFactory;

    private final Map<String, ConnectionCounter> _connectionCounters = new ConcurrentHashMap<>();

    PortConnectionCounter(AbstractBuilder<?> builder)
    {
        super();
        _connectionCounterFactory = builder.getConnectionCounterFactory();
    }

    public AcceptRegistration register(AMQPConnection<?> connection, ConnectionLimiter subLimiter)
    {
        final Principal principal = connection.getAuthorizedPrincipal();
        if (principal == null)
        {
            throw new ConnectionLimitException("Unauthorized connection is forbidden");
        }
        final String userId = principal.getName();
        return _connectionCounters.computeIfAbsent(userId, _connectionCounterFactory)
                .registerConnection(
                        userId,
                        collectGroupPrincipals(connection.getSubject()),
                        connection,
                        subLimiter);
    }

    private Set<String> collectGroupPrincipals(Subject subject)
    {
        if (subject == null)
        {
            return Collections.emptySet();
        }
        final Set<String> principalNames = new HashSet<>();
        for (final Principal principal : subject.getPrincipals(GroupPrincipal.class))
        {
            principalNames.add(principal.getName());
        }
        return principalNames;
    }

    static Builder newBuilder(Duration defaultFrequencyPeriod)
    {
        if (defaultFrequencyPeriod == null || defaultFrequencyPeriod.isNegative())
        {
            return new BuilderWithoutFrequencyImpl();
        }
        return new BuilderImpl();
    }

    public interface Builder
    {
        Builder add(Rule rule);

        Builder addAll(Collection<? extends Rule> rule);

        PortConnectionCounter build();
    }

    private interface ConnectionCounter extends ConnectionSlot
    {
        AcceptRegistration registerConnection(
                String userId, Set<String> groups, AMQPConnection<?> connection, ConnectionLimiter subLimiter);
    }

    abstract static class AbstractBuilder<T extends CombinableLimit<T>> implements Builder
    {
        final Map<String, T> _userLimits = new HashMap<>();
        T _defaultUserLimits;

        abstract T newLimits(Rule rule);

        abstract Function<String, ConnectionCounter> getConnectionCounterFactory();

        AbstractBuilder(T defaultUserLimits)
        {
            super();
            _defaultUserLimits = defaultUserLimits;
        }

        @Override
        public Builder add(Rule rule)
        {
            addImpl(rule);
            return this;
        }

        @Override
        public Builder addAll(Collection<? extends Rule> rule)
        {
            if (rule != null)
            {
                rule.forEach(this::addImpl);
            }
            return this;
        }

        @Override
        public PortConnectionCounter build()
        {
            return new PortConnectionCounter(this);
        }

        private void addImpl(Rule rule)
        {
            if (rule == null)
            {
                return;
            }
            final T newLimits = newLimits(rule);
            if (newLimits.isEmpty())
            {
                return;
            }
            final String id = rule.getIdentity();
            if (RulePredicates.isAllUser(id))
            {
                _defaultUserLimits = newLimits.mergeWith(_defaultUserLimits);
            }
            else
            {
                _userLimits.merge(id, newLimits, CombinableLimit::mergeWith);
            }
        }
    }

    private static final class BuilderImpl extends AbstractBuilder<ConnectionLimits>
    {
        BuilderImpl()
        {
            super(ConnectionLimits.noLimits());
        }

        @Override
        ConnectionLimits newLimits(Rule rule)
        {
            if (rule.getFrequencyLimit() != null &&
                    (rule.getFrequencyPeriod() == null || rule.getFrequencyPeriod().isNegative()))
            {
                throw new IllegalArgumentException("Connection frequency limit needs a time period");
            }
            return rule;
        }

        Function<String, ConnectionCounter> getConnectionCounterFactory()
        {
            return userId -> new CombinedConnectionCounterImpl(
                    new LimitCompiler<>(_userLimits, _defaultUserLimits, ConnectionLimits::noLimits));
        }
    }

    private static final class BuilderWithoutFrequencyImpl extends AbstractBuilder<ConnectionCountLimit>
    {
        BuilderWithoutFrequencyImpl()
        {
            super(ConnectionCountLimit.noLimits());
        }

        @Override
        ConnectionCountLimit newLimits(Rule rule)
        {
            return ConnectionCountLimit.newInstance(rule);
        }

        @Override
        Function<String, ConnectionCounter> getConnectionCounterFactory()
        {
            return userId -> new ConnectionCounterImpl(
                    new LimitCompiler<>(_userLimits, _defaultUserLimits, ConnectionCountLimit::noLimits));
        }
    }

    private static final class CombinedConnectionCounterImpl implements ConnectionCounter
    {
        private final LimitCompiler<ConnectionLimits> _limits;

        private final Queue<Instant> _registrationTime = new LinkedList<>();

        private long _counter = 0L;

        CombinedConnectionCounterImpl(LimitCompiler<ConnectionLimits> limits)
        {
            super();
            _limits = limits;
        }

        @Override
        public synchronized void free()
        {
            _counter = Math.max(_counter - 1L, 0L);
        }

        @Override
        public AcceptRegistration registerConnection(
                String userId, Set<String> groups, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            final ConnectionLimits limits = _limits.compileLimits(userId, groups);
            if (limits.isUserBlocked())
            {
                throw RejectRegistration.blockedUser(userId, connection.getPort().getName());
            }
            final Integer connectionCountLimit = limits.getCountLimit();
            final Map<Duration, Integer> connectionFrequencyLimit = limits.getFrequencyLimits();

            if (connectionFrequencyLimit.isEmpty())
            {
                if (connectionCountLimit == null)
                {
                    return noLimits(userId, connection, subLimiter);
                }
                return countLimit(connectionCountLimit, userId, connection, subLimiter);
            }
            if (connectionCountLimit == null)
            {
                return frequencyLimit(connectionFrequencyLimit, userId, connection, subLimiter);
            }
            return bothLimits(connectionCountLimit, connectionFrequencyLimit, userId, connection, subLimiter);
        }

        private synchronized AcceptRegistration noLimits(
                String id, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            _registrationTime.clear();
            final AcceptRegistration result = AcceptRegistration.newInstance(
                    chainTo(subLimiter.register(connection)),
                    id,
                    _counter + 1L,
                    connection.getPort().getName());
            _counter = _counter + 1L;
            return result;
        }

        private synchronized AcceptRegistration countLimit(
                Integer countLimit, String id, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            _registrationTime.clear();
            checkCounter(countLimit, id, connection);

            final AcceptRegistration result = AcceptRegistration.newInstance(
                    chainTo(subLimiter.register(connection)),
                    id,
                    _counter + 1L,
                    connection.getPort().getName());
            _counter = _counter + 1L;
            return result;
        }

        private synchronized AcceptRegistration frequencyLimit(
                Map<Duration, Integer> frequencyLimit, String id, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            final Instant now = checkFrequencyLimit(frequencyLimit, id, connection);

            final AcceptRegistration result = AcceptRegistration.newInstance(
                    chainTo(subLimiter.register(connection)),
                    id,
                    _counter + 1L,
                    connection.getPort().getName());
            _registrationTime.add(now);
            _counter = _counter + 1L;
            return result;
        }

        private synchronized AcceptRegistration bothLimits(
                Integer countLimit, Map<Duration, Integer> frequencyLimit, String id,
                AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            final Instant now = checkFrequencyLimit(frequencyLimit, id, connection);
            checkCounter(countLimit, id, connection);

            final AcceptRegistration result = AcceptRegistration.newInstance(
                    chainTo(subLimiter.register(connection)),
                    id,
                    _counter + 1L,
                    connection.getPort().getName());
            _registrationTime.add(now);
            _counter = _counter + 1L;
            return result;
        }

        private void checkCounter(Integer countLimit, String id, AMQPConnection<?> connection)
        {
            if (_counter >= countLimit)
            {
                throw RejectRegistration.breakingConnectionCount(id, countLimit, connection.getPort().getName());
            }
        }

        private Instant checkFrequencyLimit(
                final Map<Duration, Integer> frequencyLimit, String id, AMQPConnection<?> connection)
        {
            final Instant now = Instant.now();
            final NavigableSet<FrequencyPeriod> periods = new TreeSet<>();
            for (final Entry<Duration, Integer> entry : frequencyLimit.entrySet())
            {
                periods.add(new FrequencyPeriod(now, entry));
            }

            final Iterator<Instant> registrationTimeIterator = _registrationTime.iterator();
            int counter = _registrationTime.size();
            boolean obsoleteRegistrationTime = true;

            while (registrationTimeIterator.hasNext() && !periods.isEmpty())
            {
                final Instant registrationTime = registrationTimeIterator.next();

                final Iterator<FrequencyPeriod> periodIterator = periods.iterator();
                FrequencyPeriod period;
                while (periodIterator.hasNext() &&
                        (period = periodIterator.next()).isPeriodBeginningBefore(registrationTime))
                {
                    if (!period.isCounterUnderLimit(counter))
                    {
                        throw RejectRegistration.breakingConnectionFrequency(
                                id, period.getLimit(), period.getDuration(), connection.getPort().getName());
                    }
                    obsoleteRegistrationTime = false;
                    periodIterator.remove();
                }
                counter--;
                if (obsoleteRegistrationTime)
                {
                    registrationTimeIterator.remove();
                }
            }
            return now;
        }
    }

    private static final class ConnectionCounterImpl implements ConnectionCounter
    {
        private final LimitCompiler<ConnectionCountLimit> _limits;

        private long _counter = 0L;

        ConnectionCounterImpl(LimitCompiler<ConnectionCountLimit> limits)
        {
            super();
            _limits = limits;
        }

        @Override
        public synchronized void free()
        {
            _counter = Math.max(_counter - 1L, 0L);
        }

        @Override
        public AcceptRegistration registerConnection(
                String userId, Set<String> groups, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            final ConnectionCountLimit limits = _limits.compileLimits(userId, groups);
            if (limits.isUserBlocked())
            {
                throw RejectRegistration.blockedUser(userId, connection.getPort().getName());
            }
            if (limits.getCountLimit() == null)
            {
                return noLimits(userId, connection, subLimiter);
            }

            return countLimit(limits.getCountLimit(), userId, connection, subLimiter);
        }

        private synchronized AcceptRegistration noLimits(
                String id, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            final AcceptRegistration result = AcceptRegistration.newInstance(
                    chainTo(subLimiter.register(connection)),
                    id,
                    _counter + 1L,
                    connection.getPort().getName());
            _counter = _counter + 1L;
            return result;
        }

        private synchronized AcceptRegistration countLimit(
                Integer countLimit, String id, AMQPConnection<?> connection, ConnectionLimiter subLimiter)
        {
            if (_counter >= countLimit)
            {
                throw RejectRegistration.breakingConnectionCount(id, countLimit, connection.getPort().getName());
            }
            final AcceptRegistration result = AcceptRegistration.newInstance(
                    chainTo(subLimiter.register(connection)),
                    id,
                    _counter + 1L,
                    connection.getPort().getName());
            _counter = _counter + 1L;
            return result;
        }
    }

    private static final class LimitCompiler<T extends CombinableLimit<T>>
    {
        private final Map<String, T> _limitMap;
        private final T _defaultLimits;
        private final Supplier<T> _noLimits;

        LimitCompiler(Map<String, T> limitMap, T defaultLimits, Supplier<T> noLimits)
        {
            _limitMap = new HashMap<>(limitMap);
            _defaultLimits = Objects.requireNonNull(defaultLimits);
            _noLimits = Objects.requireNonNull(noLimits);
        }

        public T compileLimits(String userId, Set<String> groups)
        {
            final T userLimits = _noLimits.get().mergeWith(_limitMap.get(userId));

            T groupLimits = _noLimits.get();
            for (final String group : groups)
            {
                groupLimits = groupLimits.mergeWith(_limitMap.get(group));
            }

            return userLimits.then(groupLimits).then(_defaultLimits);
        }
    }

    private static final class FrequencyPeriod implements Comparable<FrequencyPeriod>
    {
        private final Instant _startTime;
        private final Duration _duration;
        private final int _limit;

        FrequencyPeriod(Instant now, Map.Entry<Duration, Integer> limit)
        {
            _startTime = now.minus(limit.getKey());
            _duration = limit.getKey();
            _limit = limit.getValue();
        }

        @Override
        public int compareTo(FrequencyPeriod o)
        {
            return _startTime.compareTo(o._startTime);
        }

        @Override
        public int hashCode()
        {
            return _startTime.hashCode();
        }

        @Override
        public boolean equals(Object o)
        {
            if (o instanceof FrequencyPeriod)
            {
                return _startTime.equals(((FrequencyPeriod) o)._startTime);
            }
            return false;
        }

        boolean isPeriodBeginningBefore(Instant time)
        {
            return _startTime.isBefore(time);
        }

        boolean isCounterUnderLimit(int counter)
        {
            return counter < _limit;
        }

        Duration getDuration()
        {
            return _duration;
        }

        int getLimit()
        {
            return _limit;
        }
    }
}
