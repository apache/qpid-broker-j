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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.qpid.server.security.access.config.predicates.RulePredicate;

public class HostnameFirewallRule extends AbstractFirewallRuleImpl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HostnameFirewallRule.class);

    private static final long DNS_TIMEOUT = 30000;
    private static final ExecutorService DNS_LOOKUP = Executors.newCachedThreadPool();

    private final List<Pattern> _hostnamePatterns;
    private final Set<String> _hostnames;

    public HostnameFirewallRule(String... hostnames)
    {
        this(Arrays.asList(hostnames));
    }

    public HostnameFirewallRule(Collection<String> hostnames)
    {
        super();
        _hostnames = new HashSet<>(hostnames);
        _hostnamePatterns = new ArrayList<>(_hostnames.size());

        for (final String hostname : _hostnames)
        {
            _hostnamePatterns.add(Pattern.compile(hostname));
        }

        LOGGER.debug("Created {}", this);
    }

    private HostnameFirewallRule(HostnameFirewallRule rule, RulePredicate subPredicate)
    {
        super(subPredicate);
        _hostnames = rule._hostnames;
        _hostnamePatterns = rule._hostnamePatterns;
    }

    @Override
    boolean matches(InetAddress remote)
    {
        final String hostname = getHostname(remote);
        if (hostname == null)
        {
            throw new AccessControlFirewallException("DNS lookup failed for address " + remote);
        }
        for (final Pattern pattern : _hostnamePatterns)
        {
            if (pattern.matcher(hostname).matches())
            {
                LOGGER.debug("Hostname '{}' matches rule '{}'", hostname, pattern);
                return true;
            }
        }
        LOGGER.debug("Hostname '{}' matches no configured hostname patterns", hostname);
        return false;
    }

    @Override
    RulePredicate copy(RulePredicate subPredicate)
    {
        return new HostnameFirewallRule(this, subPredicate);
    }

    /**
     * @param remote the InetAddress to look up
     * @return the hostname, null if not found, takes longer than
     * {@link #DNS_LOOKUP} to find or otherwise fails
     */
    private String getHostname(final InetAddress remote)
    {
        final FutureTask<String> lookup = new FutureTask<>(remote::getCanonicalHostName);
        DNS_LOOKUP.execute(lookup);

        try
        {
            return lookup.get(DNS_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (RuntimeException | InterruptedException | ExecutionException | TimeoutException e)
        {
            LOGGER.warn(String.format("Unable to look up hostname from address '%s'", remote), e);
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
        return _hostnames.equals(that._hostnames);
    }

    @Override
    public int hashCode()
    {
        return _hostnames.hashCode();
    }

    @Override
    public String toString()
    {
        return "HostnameFirewallRule[" +
                "hostnames=" + _hostnames +
                ']';
    }
}
