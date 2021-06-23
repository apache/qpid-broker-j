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
import java.net.SocketAddress;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.security.access.config.FirewallRule;

abstract class AbstractFirewallRuleImpl implements FirewallRule
{
    AbstractFirewallRuleImpl()
    {
        super();
    }

    @Override
    public boolean matches(final Subject subject)
    {
        final InetAddress addressOfClient = getAddressOfClient(subject);
        if (addressOfClient == null)
        {
            return true;
        }
        return matches(addressOfClient);
    }

    private InetAddress getAddressOfClient(final Subject subject)
    {
        final Set<ConnectionPrincipal> principals = subject.getPrincipals(ConnectionPrincipal.class);
        if(!principals.isEmpty())
        {
            final SocketAddress address = principals.iterator().next().getConnection().getRemoteSocketAddress();
            if(address instanceof InetSocketAddress)
            {
                return ((InetSocketAddress) address).getAddress();
            }
        }
        return null;
    }

    protected abstract boolean matches(InetAddress addressOfClient);
}
