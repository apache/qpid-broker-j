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
 *
 */
package org.apache.qpid.server.security.auth.manager;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class SimpleLDAPAuthenticationManagerFactoryTest extends UnitTestBase
{
    private final ConfiguredObjectFactory _factory = BrokerModel.getInstance().getObjectFactory();
    private final Map<String, Object> _configuration = new HashMap<>();
    private final Broker<?> _broker = BrokerTestHelper.createBrokerMock();
    private final TrustStore<?> _trustStore = mock(TrustStore.class);

    @BeforeEach
    public void setUp() throws Exception
    {
        when(_trustStore.getName()).thenReturn("mytruststore");
        when(_trustStore.getId()).thenReturn(randomUUID());
        _configuration.clear();
        _configuration.put(AuthenticationProvider.ID, randomUUID());
        _configuration.put(AuthenticationProvider.NAME, getClass().getName());
    }

    @AfterAll
    public void tearDown()
    {
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(List.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLdapCreated()
    {
        _configuration.put(AuthenticationProvider.TYPE, SimpleLDAPAuthenticationManager.PROVIDER_TYPE);
        _configuration.put("providerUrl", "ldaps://example.com:636/");
        _configuration.put("searchContext", "dc=example");
        _configuration.put("searchFilter", "(uid={0})");
        _configuration.put("ldapContextFactory", TestLdapDirectoryContext.class.getName());
        _factory.create(AuthenticationProvider.class, _configuration, _broker);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLdapsWhenTrustStoreNotFound()
    {
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(List.of(_trustStore));
        _configuration.put(AuthenticationProvider.TYPE, SimpleLDAPAuthenticationManager.PROVIDER_TYPE);
        _configuration.put("providerUrl", "ldaps://example.com:636/");
        _configuration.put("searchContext", "dc=example");
        _configuration.put("searchFilter", "(uid={0})");
        _configuration.put("trustStore", "notfound");

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> _factory.create(AuthenticationProvider.class, _configuration, _broker),
                "Exception not thrown");

        assertTrue(thrown.getMessage().contains("name 'notfound'"), "Message does not include underlying issue ");
        assertTrue(thrown.getMessage().contains("trustStore"), "Message does not include the attribute name");
        assertTrue(thrown.getMessage().contains("TrustStore"), "Message does not include the expected type");
    }
}
