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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.security.access.firewall.FirewallRule;
import org.apache.qpid.test.utils.UnitTestBase;

public class AclActionTest extends UnitTestBase
{
    private static final String TEST_HOSTNAME = "localhost";
    private static final String TEST_ATTRIBUTES = "test";

    @Test
    public void testEqualsAndHashCode()
    {
        AclRulePredicates predicates = createAclRulePredicates();
        ObjectType objectType = ObjectType.EXCHANGE;
        LegacyOperation operation = LegacyOperation.ACCESS;

        AclAction aclAction = new AclAction(operation, objectType, predicates);
        AclAction equalAclAction = new AclAction(operation, objectType, predicates);

        assertTrue(aclAction.equals(aclAction));
        assertTrue(aclAction.equals(equalAclAction));
        assertTrue(equalAclAction.equals(aclAction));

        assertTrue(aclAction.hashCode() == equalAclAction.hashCode());

        assertFalse("Different operation should cause aclActions to be unequal",
                           aclAction.equals(new AclAction(LegacyOperation.BIND, objectType, predicates)));


        assertFalse("Different operation type should cause aclActions to be unequal",
                           aclAction.equals(new AclAction(operation, ObjectType.GROUP, predicates)));

        assertFalse("Different predicates should cause aclActions to be unequal",
                           aclAction.equals(new AclAction(operation, objectType, createAclRulePredicates())));
    }

    @Test
    public void testGetAttributes()
    {
        final ObjectType objectType = ObjectType.VIRTUALHOST;
        final LegacyOperation operation = LegacyOperation.ACCESS;
        final AclRulePredicates predicates = new AclRulePredicates();
        predicates.parse(ObjectProperties.Property.FROM_HOSTNAME.name(), TEST_HOSTNAME);
        predicates.parse(ObjectProperties.Property.ATTRIBUTES.name(), TEST_ATTRIBUTES);
        predicates.parse(ObjectProperties.Property.NAME.name(), getTestName());

        final AclAction aclAction = new AclAction(operation, objectType, predicates);
        final Map<ObjectProperties.Property, String> attributes = aclAction.getAttributes();

        assertThat(attributes,
                   allOf(aMapWithSize(3),
                         hasEntry(ObjectProperties.Property.FROM_HOSTNAME, TEST_HOSTNAME),
                         hasEntry(ObjectProperties.Property.ATTRIBUTES, TEST_ATTRIBUTES),
                         hasEntry(ObjectProperties.Property.NAME, getTestName())));
    }

    private AclRulePredicates createAclRulePredicates()
    {
        AclRulePredicates predicates = mock(AclRulePredicates.class);
        when(predicates.getDynamicRule()).thenReturn(mock(FirewallRule.class));
        when(predicates.getObjectProperties()).thenReturn(mock(ObjectProperties.class));
        return predicates;
    }

}
