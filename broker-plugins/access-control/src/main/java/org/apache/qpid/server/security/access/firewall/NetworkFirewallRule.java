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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkFirewallRule extends FirewallRule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkFirewallRule.class);
    private List<InetNetwork> _networks;

    public NetworkFirewallRule(String... networks)
    {
        _networks = new ArrayList<InetNetwork>();
        for (int i = 0; i < networks.length; i++)
        {
            String network = networks[i];
            try
            {
                InetNetwork inetNetwork = InetNetwork.getFromString(network);
                if (!_networks.contains(inetNetwork))
                {
                    _networks.add(inetNetwork);
                }
            }
            catch (java.net.UnknownHostException uhe)
            {
                LOGGER.error("Cannot resolve address: " + network, uhe);
            }
        }

        LOGGER.debug("Created {}", this);
    }

    @Override
    protected boolean matches(InetAddress ip)
    {
        for (InetNetwork network : _networks)
        {
            if (network.contains(ip))
            {

                LOGGER.debug("Client address {} matches configured network {}", ip, network);

                return true;
            }
        }

        LOGGER.debug("Client address {} does not match any configured networks", ip);

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

        return !(_networks != null ? !_networks.equals(that._networks) : that._networks != null);

    }

    @Override
    public int hashCode()
    {
        return _networks != null ? _networks.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "NetworkFirewallRule[" +
               "networks=" + _networks +
               ']';
    }
}
