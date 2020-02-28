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

package org.apache.qpid.server.security.access.config.connection;

import javax.security.auth.Subject;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.security.access.config.DynamicRule;
import org.apache.qpid.server.transport.AMQPConnection;

public abstract class ConnectionPrincipalStatisticsRule implements DynamicRule
{
    private final int _limit;

    ConnectionPrincipalStatisticsRule(final int limit)
    {
        _limit = limit;
    }

    int getLimit()
    {
        return _limit;
    }

    @Override
    public boolean matches(final Subject subject)
    {
        AMQPConnection<?> connection = getConnection(subject);
        if (connection != null)
        {
            return matches(connection);
        }
        return false;
    }

    abstract boolean matches(final AMQPConnection<?> connection);

    private AMQPConnection<?> getConnection(final Subject subject)
    {
        if (subject != null)
        {
            final ConnectionPrincipal principal = subject.getPrincipals(ConnectionPrincipal.class)
                                                         .stream()
                                                         .findFirst()
                                                         .orElse(null);
            if (principal != null)
            {
                return principal.getConnection();
            }
        }
        return null;
    }
}
