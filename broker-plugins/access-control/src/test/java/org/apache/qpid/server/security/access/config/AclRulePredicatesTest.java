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
package org.apache.qpid.server.security.access.config;

import static org.apache.qpid.server.security.access.config.ObjectProperties.Property.ATTRIBUTES;
import static org.apache.qpid.server.security.access.config.ObjectProperties.Property.CLASS;
import static org.apache.qpid.server.security.access.config.ObjectProperties.Property.CONNECTION_LIMIT;
import static org.apache.qpid.server.security.access.config.ObjectProperties.Property.FROM_HOSTNAME;
import static org.apache.qpid.server.security.access.config.ObjectProperties.Property.FROM_NETWORK;
import static org.apache.qpid.server.security.access.config.ObjectProperties.Property.NAME;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import org.apache.qpid.server.security.access.firewall.FirewallRule;
import org.apache.qpid.server.security.access.firewall.FirewallRuleFactory;
import org.apache.qpid.test.utils.UnitTestBase;

public class AclRulePredicatesTest extends UnitTestBase
{
    private AclRulePredicates _aclRulePredicates = new AclRulePredicates();
    private FirewallRuleFactory _firewallRuleFactory = mock(FirewallRuleFactory.class);

    @Before
    public void setUp() throws Exception
    {
        _aclRulePredicates.setFirewallRuleFactory(_firewallRuleFactory);

        when(_firewallRuleFactory.createForHostname(any())).thenReturn(mock(FirewallRule.class));
        when(_firewallRuleFactory.createForNetwork(any())).thenReturn(mock(FirewallRule.class));
    }

    @Test
    public void testParse()
    {
        String name = "name";
        String className = "class";

        _aclRulePredicates.parse(NAME.name(), name);
        _aclRulePredicates.parse(CLASS.name(), className);

        assertEquals(name, _aclRulePredicates.getObjectProperties().get(NAME));
        assertEquals(className, _aclRulePredicates.getObjectProperties().get(CLASS));
    }

    @Test
    public void testParseHostnameFirewallRule()
    {
        String hostname = "hostname1,hostname2";
        _aclRulePredicates.parse(FROM_HOSTNAME.name(), hostname);

        verify(_firewallRuleFactory).createForHostname(new String[] {"hostname1", "hostname2"});
    }

    @Test
    public void testParseNetworkFirewallRule()
    {
        _aclRulePredicates.setFirewallRuleFactory(_firewallRuleFactory);

        String networks = "network1,network2";
        _aclRulePredicates.parse(FROM_NETWORK.name(), networks);

        verify(_firewallRuleFactory).createForNetwork(new String[] {"network1", "network2"});
    }

    @Test
    public void testParseThrowsExceptionIfBothHostnameAndNetworkSpecified()
    {
        _aclRulePredicates.parse(FROM_NETWORK.name(), "network1,network2");
        try
        {
            _aclRulePredicates.parse(FROM_HOSTNAME.name(), "hostname1,hostname2");
            fail("Exception not thrown");
        }
        catch(IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testParseAttributesRule()
    {
        String attributes = "attribute1,attribute2";
        _aclRulePredicates.parse(ATTRIBUTES.name(), attributes);

        final Set<String> attributesSet = Sets.newSet(attributes.split(","));
        assertEquals("Unexpected attributes",
                            attributesSet,
                            _aclRulePredicates.getObjectProperties().getAttributeNames());

    }

    @Test
    public void testGetParsedProperties()
    {
        _aclRulePredicates.parse(ATTRIBUTES.name(), "attribute1,attribute2");
        _aclRulePredicates.parse(FROM_NETWORK.name(), "network1,network2");
        _aclRulePredicates.parse(CONNECTION_LIMIT.name(), "20");

        final Map<ObjectProperties.Property, String> properties = _aclRulePredicates.getParsedProperties();

        assertThat(properties, allOf(hasEntry(ATTRIBUTES, "attribute1,attribute2"),
                                     hasEntry(FROM_NETWORK, "network1,network2"),
                                     hasEntry(CONNECTION_LIMIT, "20")));
    }
}
