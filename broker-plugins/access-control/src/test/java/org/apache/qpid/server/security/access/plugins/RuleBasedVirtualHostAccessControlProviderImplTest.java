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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class RuleBasedVirtualHostAccessControlProviderImplTest extends UnitTestBase
{
    private RuleBasedVirtualHostAccessControlProviderImpl _aclProvider;

    @BeforeEach
    void setUp()
    {
        final Map<String, Object> virtualHostAttributes = Map.of(QueueManagingVirtualHost.NAME, "testVH",
                QueueManagingVirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        final Map<String, Object> attributes = Map.of(RuleBasedAccessControlProvider.NAME, RuleBasedVirtualHostAccessControlProviderImplTest.class.getName());
        final QueueManagingVirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(virtualHostAttributes, this);
        _aclProvider = new RuleBasedVirtualHostAccessControlProviderImpl(attributes, virtualHost);
        _aclProvider.create();
    }

    @Test
    void setValidAttributes()
    {
        final List<Object> rules = List.of(Map.of("identity", "user",
                "operation", "PUBLISH",
                "outcome", "ALLOW_LOG",
                "objectType", "EXCHANGE",
                "attributes", Map.of("ROUTING_KEY", "routing_key", "NAME", "xxx")));
        final Map<String, Object> attributes = Map.of("name", "changed", "rules", rules);

        assertDoesNotThrow(() ->_aclProvider.setAttributes(attributes));
    }

    @Test
    void setInvalidAttributes()
    {
        final List<Object> rules = List.of(Map.of("identity", "user",
                "operation", "PUBLISH",
                "outcome", "ALLOW_LOG",
                "objectType", "EXCHANGE",
                "attributes", Map.of("FOO", "bar", "ROUTING_KEY", "routing_key", "NAME", "xxx")));
        final Map<String, Object> attributes = Map.of("name", "changed", "rules", rules);

        final IllegalArgumentException exception =  assertThrows(IllegalArgumentException.class,
            () -> _aclProvider.setAttributes(attributes), "Expected exception not thrown");

        assertEquals("No enum constant org.apache.qpid.server.security.access.config.Property.FOO", exception.getMessage());
    }
}
