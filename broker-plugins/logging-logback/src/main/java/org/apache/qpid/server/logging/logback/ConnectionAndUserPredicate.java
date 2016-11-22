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

package org.apache.qpid.server.logging.logback;

import java.security.AccessController;
import java.util.Set;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.SocketConnectionPrincipal;

class ConnectionAndUserPredicate implements PredicateAndLoggerNameAndLevelFilter.Predicate
{
    private static final Pattern MATCH_ALL = Pattern.compile(".*");
    private Pattern _usernamePattern = MATCH_ALL;
    private Pattern _connectionNamePattern = MATCH_ALL;
    private Pattern _remoteContainerIdPattern = MATCH_ALL;

    @Override
    public boolean evaluate(final ILoggingEvent event)
    {
        String userPrincipalString = "";
        String connectionString = "";
        String remoteContainerName = "";

        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<SocketConnectionPrincipal> connectionPrincipals = subject.getPrincipals(SocketConnectionPrincipal.class);
        Set<AuthenticatedPrincipal> userPrincipals = subject.getPrincipals(AuthenticatedPrincipal.class);
        if (!connectionPrincipals.isEmpty())
        {
            SocketConnectionPrincipal socketConnectionPrincipal = connectionPrincipals.iterator().next();
            connectionString = socketConnectionPrincipal.getName();
            if (socketConnectionPrincipal instanceof ConnectionPrincipal)
            {
                remoteContainerName = ((ConnectionPrincipal) socketConnectionPrincipal).getConnection().getRemoteContainerName();
                if (remoteContainerName == null)
                {
                    remoteContainerName = "";
                }
            }
        }
        if (!userPrincipals.isEmpty())
        {
            userPrincipalString = new GenericPrincipal(userPrincipals.iterator().next()).toExternalForm();
        }

        return _usernamePattern.matcher(userPrincipalString).matches()
               && _connectionNamePattern.matcher(connectionString).matches()
               && _remoteContainerIdPattern.matcher(remoteContainerName).matches();
    }

    void setConnectionNamePattern(final String connectionName)
    {
        if (connectionName != null)
        {
            _connectionNamePattern = Pattern.compile(connectionName);
        }
        else
        {
            _connectionNamePattern = MATCH_ALL;
        }
    }

    void setRemoteContainerIdPattern(final String remoteContainerId)
    {
        if (remoteContainerId != null)
        {
            _remoteContainerIdPattern = Pattern.compile(remoteContainerId);
        }
        else
        {
            _remoteContainerIdPattern = MATCH_ALL;
        }
    }

    void setUsernamePattern(final String usernamePattern)
    {
        if (usernamePattern != null)
        {
            _usernamePattern = Pattern.compile(usernamePattern);
        }
        else
        {
            _usernamePattern = MATCH_ALL;
        }
    }
}
