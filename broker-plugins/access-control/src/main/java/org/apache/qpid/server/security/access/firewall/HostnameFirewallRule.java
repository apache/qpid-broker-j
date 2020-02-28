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
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostnameFirewallRule extends FirewallRule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HostnameFirewallRule.class);

    private static final long DNS_TIMEOUT = 30000;
    private static final ExecutorService DNS_LOOKUP = Executors.newCachedThreadPool();

    private Pattern[] _hostnamePatterns;
    private String[] _hostnames;

    public HostnameFirewallRule(String... hostnames)
    {
        _hostnames = hostnames;

        int i = 0;
        _hostnamePatterns = new Pattern[hostnames.length];
        for (String hostname : hostnames)
        {
            _hostnamePatterns[i++] = Pattern.compile(hostname);
        }

        LOGGER.debug("Created {}", this);

    }

    @Override
    protected boolean matches(InetAddress remote)
    {
        String hostname = getHostname(remote);
        if (hostname == null)
        {
            throw new AccessControlFirewallException("DNS lookup failed for address " + remote);
        }
        for (Pattern pattern : _hostnamePatterns)
        {
            boolean hostnameMatches = pattern.matcher(hostname).matches();

            if (hostnameMatches)
            {
                if(LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Hostname " + hostname + " matches rule " + pattern.toString());
                }
                return true;
            }
        }

        LOGGER.debug("Hostname {} matches no configured hostname patterns", hostname);

        return false;
    }

    /**
     * @param remote
     *            the InetAddress to look up
     * @return the hostname, null if not found, takes longer than
     *         {@link #DNS_LOOKUP} to find or otherwise fails
     */
    private String getHostname(final InetAddress remote) throws AccessControlFirewallException
    {
        FutureTask<String> lookup = new FutureTask<String>(new Callable<String>()
        {
            @Override
            public String call()
            {
                return remote.getCanonicalHostName();
            }
        });
        DNS_LOOKUP.execute(lookup);

        try
        {
            return lookup.get(DNS_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (Exception e)
        {
            LOGGER.warn("Unable to look up hostname from address " + remote, e);
            return null;
        }
        finally
        {
            lookup.cancel(true);
        }
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

        final HostnameFirewallRule that = (HostnameFirewallRule) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(_hostnames, that._hostnames);

    }

    @Override
    public int hashCode()
    {
        return _hostnames != null ? Arrays.hashCode(_hostnames) : 0;
    }

    @Override
    public String toString()
    {
        return "HostnameFirewallRule[" +
               "hostnames=" + Arrays.toString(_hostnames) +
               ']';
    }
}
