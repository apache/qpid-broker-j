/*
 *
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
 *
 */

package org.apache.qpid.server.security.access.plugins;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class RuleBasedAccessControlProviderImplTest extends UnitTestBase
{
    private static final String ACL_RULE_PRINCIPAL = "guest";
    private RuleBasedAccessControlProviderImpl _aclProvider;
    private String _nameAttributeValue;

    @Before
    public void setUp()
    {
        final Map<String, Object>
                attributes = Collections.singletonMap(RuleBasedAccessControlProvider.NAME, getTestName());
        final Broker<?> broker = BrokerTestHelper.createBrokerMock();
        _aclProvider = new RuleBasedAccessControlProviderImpl(attributes, broker);
        _aclProvider.create();

        _nameAttributeValue = getTestName();
    }

    @Test
    public void testLoadACLWithFromHostnameFirewallRule()
    {
        loadAclAndAssertRule(ObjectProperties.Property.FROM_HOSTNAME, "localhost");
    }

    @Test
    public void testLoadACLWithFromNetworkFirewallRule()
    {
        loadAclAndAssertRule(ObjectProperties.Property.FROM_NETWORK, "192.168.1.0/24");
    }

    @Test
    public void testLoadACLWithFromNetworkFirewallRuleContainingWildcard()
    {
        loadAclAndAssertRule(ObjectProperties.Property.FROM_NETWORK, "192.168.1.*");
    }

    @Test
    public void testLoadACLWithAttributes()
    {
        loadAclAndAssertRule(ObjectProperties.Property.ATTRIBUTES,
                             String.join(",", ConfiguredObject.NAME, ConfiguredObject.LIFETIME_POLICY));
    }

    private void loadAclAndAssertRule(final ObjectProperties.Property attributeName,
                                      final String attributeValue)
    {
        final String acl = String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST %s=\"%s\" name=\"%s\"",
                                         ACL_RULE_PRINCIPAL,
                                         attributeName,
                                         attributeValue,
                                         _nameAttributeValue);

        _aclProvider.loadFromFile(DataUrlUtils.getDataUrlForBytes(acl.getBytes(UTF_8)));

        final List<AclRule> rules = _aclProvider.getRules();
        assertThat(rules, is(notNullValue()));
        assertThat(rules.size(), is(equalTo(1)));

        final AclRule rule = rules.get(0);
        assertThat(rule, is(notNullValue()));
        assertThat(rule.getObjectType(), is(equalTo(ObjectType.VIRTUALHOST)));
        assertThat(rule.getIdentity(), is(equalTo(ACL_RULE_PRINCIPAL)));
        assertThat(rule.getOperation(), is(equalTo(LegacyOperation.ACCESS)));
        assertThat(rule.getOutcome(), is(equalTo(RuleOutcome.ALLOW_LOG)));
        assertThat(rule.getAttributes(),
                   allOf(aMapWithSize(2),
                         hasEntry(attributeName, attributeValue),
                         hasEntry(ObjectProperties.Property.NAME, _nameAttributeValue)));
    }

}
