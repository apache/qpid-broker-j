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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkFirewallRule extends AbstractFirewallRuleImpl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkFirewallRule.class);

    private final Set<InetNetwork> _networks = new HashSet<>();

    public NetworkFirewallRule(String... networks)
    {
        this(Arrays.asList(networks));
    }

    public NetworkFirewallRule(Collection<String> networks)
    {
        super();
        for (final String network : networks)
        {
            try
            {
                _networks.add(InetNetwork.getFromString(network));
            }
            catch (UnknownHostException uhe)
            {
                LOGGER.error(String.format("Cannot resolve address: '%s'", network), uhe);
            }
        }
        LOGGER.debug("Created {}", this);
    }

    @Override
    boolean matches(InetAddress ip)
    {
        for (final InetNetwork network : _networks)
        {
            if (network.contains(ip))
            {
                LOGGER.debug("Client address '{}' matches configured network '{}'", ip, network);
                return true;
            }
        }
        LOGGER.debug("Client address '{}' does not match any configured networks", ip);
        return false;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final NetworkFirewallRule that = (NetworkFirewallRule) o;
        return _networks.equals(that._networks);
    }

    @Override
    public int hashCode()
    {
        return _networks.hashCode();
    }

    @Override
    public String toString()
    {
        return "NetworkFirewallRule[" +
                "networks=" + _networks +
                ']';
    }
}
