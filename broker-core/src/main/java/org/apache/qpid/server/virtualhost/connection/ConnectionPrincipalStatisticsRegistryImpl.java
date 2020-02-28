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

package org.apache.qpid.server.virtualhost.connection;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.ConnectionPrincipalStatistics;
import org.apache.qpid.server.virtualhost.ConnectionPrincipalStatisticsRegistry;
import org.apache.qpid.server.virtualhost.ConnectionStatisticsRegistrySettings;

public class ConnectionPrincipalStatisticsRegistryImpl implements ConnectionPrincipalStatisticsRegistry
{
    private final Map<Principal, ConnectionPrincipalStatisticsImpl> _principalStatistics = new ConcurrentHashMap<>();
    private final ConnectionStatisticsRegistrySettings _settings;

    public ConnectionPrincipalStatisticsRegistryImpl(final ConnectionStatisticsRegistrySettings settings)
    {
        _settings = settings;
    }

    @Override
    public ConnectionPrincipalStatistics connectionOpened(final AMQPConnection<?> connection)
    {
        final Subject subject = connection.getSubject();
        final AuthenticatedPrincipal principal = AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(subject);
        return _principalStatistics.compute(principal,
                                            (p, s) -> connectionOpened(s, connection.getCreatedTime()));
    }

    @Override
    public ConnectionPrincipalStatistics connectionClosed(final AMQPConnection<?> connection)
    {
        final Subject subject = connection.getSubject();
        final AuthenticatedPrincipal principal = AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(subject);
        return _principalStatistics.computeIfPresent(principal, (p, s) -> connectionClosed(s));
    }

    @Override
    public void reevaluateConnectionStatistics()
    {
        new HashSet<>(_principalStatistics.keySet()).forEach(this::reevaluateConnectionStatistics);
    }

    @Override
    public void reset()
    {
        _principalStatistics.clear();
    }

    int getConnectionCount(final Principal principal)
    {
        ConnectionPrincipalStatistics cs = _principalStatistics.get(principal);
        if (cs != null)
        {
            return cs.getConnectionCount();
        }
        return 0;
    }

    int getConnectionFrequency(final Principal principal)
    {
        ConnectionPrincipalStatistics cs = _principalStatistics.get(principal);
        if (cs != null)
        {
            return cs.getConnectionFrequency();
        }
        return 0;
    }

    private ConnectionPrincipalStatisticsImpl connectionOpened(final ConnectionPrincipalStatisticsImpl current,
                                                           final Date createdTime)
    {
        if (current == null)
        {
            return new ConnectionPrincipalStatisticsImpl(1, Collections.singletonList(createdTime.getTime()));
        }
        else
        {
            final long frequencyPeriod = getConnectionFrequencyPeriodMillis();
            final List<Long> connectionCreatedTimes;
            if (frequencyPeriod > 0)
            {
                connectionCreatedTimes = findTimesWithinPeriod(current.getLatestConnectionCreatedTimes(), frequencyPeriod);
                connectionCreatedTimes.add(createdTime.getTime());
            }
            else
            {
                connectionCreatedTimes = Collections.emptyList();
            }
            return new ConnectionPrincipalStatisticsImpl(current.getConnectionCount() + 1, connectionCreatedTimes);
        }
    }

    private ConnectionPrincipalStatisticsImpl connectionClosed(final ConnectionPrincipalStatisticsImpl current)
    {
        return createStatisticsOrNull(Math.max(0, current.getConnectionCount() - 1), current.getLatestConnectionCreatedTimes());
    }

    private void reevaluateConnectionStatistics(final Principal authenticatedPrincipal)
    {
        _principalStatistics.computeIfPresent(authenticatedPrincipal, (principal, current) -> reevaluate(current));
    }

    private ConnectionPrincipalStatisticsImpl reevaluate(final ConnectionPrincipalStatisticsImpl current)
    {
        return createStatisticsOrNull(current.getConnectionCount(), current.getLatestConnectionCreatedTimes());
    }

    private ConnectionPrincipalStatisticsImpl createStatisticsOrNull(final int openConnectionCount,
                                                                 final List<Long> currentConnectionCreatedTimes)
    {
        final List<Long> connectionCreatedTimes = findTimesWithinPeriod(currentConnectionCreatedTimes, getConnectionFrequencyPeriodMillis());
        if (openConnectionCount == 0 && connectionCreatedTimes.isEmpty())
        {
            return null;
        }
        return new ConnectionPrincipalStatisticsImpl(openConnectionCount, connectionCreatedTimes);
    }

    private List<Long> findTimesWithinPeriod(final Collection<Long> currentConnectionCreatedTimes,
                                             final long frequencyPeriod)
    {

        final List<Long> connectionCreatedTimes;
        if (frequencyPeriod > 0)
        {
            final long periodEnd = System.currentTimeMillis();
            final long periodStart = periodEnd - frequencyPeriod;
            connectionCreatedTimes = findTimesBetween(currentConnectionCreatedTimes, periodStart, periodEnd);
        }
        else
        {
            connectionCreatedTimes = Collections.emptyList();
        }
        return connectionCreatedTimes;
    }

    private List<Long> findTimesBetween(final Collection<Long> connectionCreatedTimes,
                                        final long periodStart,
                                        final long periodEnd)
    {
        return connectionCreatedTimes.stream().mapToLong(Long::longValue)
                                     .filter(t -> t >= periodStart && t <= periodEnd)
                                     .boxed()
                                     .collect(Collectors.toCollection(ArrayList::new));
    }

    private long getConnectionFrequencyPeriodMillis()
    {
        return _settings.getConnectionFrequencyPeriod().toMillis();
    }
}
