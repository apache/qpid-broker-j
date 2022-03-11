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
package org.apache.qpid.server.security.access.firewall;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.Stream;

import javax.security.auth.Subject;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.security.access.config.FirewallRule;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.security.auth.SocketConnectionPrincipal;

abstract class AbstractFirewallRuleImpl implements FirewallRule
{
    AbstractFirewallRuleImpl()
    {
        super();
    }

    abstract boolean matches(InetAddress addressOfClient);

    @Override
    public boolean matches(final Subject subject)
    {
        final Set<ConnectionPrincipal> connectionPrincipals = subject.getPrincipals(ConnectionPrincipal.class);
        final Set<ManagementConnectionPrincipal> mgmtConnectionPrincipals = subject.getPrincipals(ManagementConnectionPrincipal.class);
        if (!connectionPrincipals.isEmpty() || !mgmtConnectionPrincipals.isEmpty())
        {
            return Stream.concat(connectionPrincipals.stream(), mgmtConnectionPrincipals.stream())
                .map(SocketConnectionPrincipal::getRemoteAddress)
                .filter(InetSocketAddress.class::isInstance)
                .map(address -> matches(((InetSocketAddress) address).getAddress()))
                .findFirst().orElse(true);
        }
        return true;
    }
}
