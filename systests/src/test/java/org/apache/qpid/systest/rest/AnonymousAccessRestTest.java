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
package org.apache.qpid.systest.rest;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Plugin;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class AnonymousAccessRestTest extends QpidRestTestCase
{
    @Override
    public void startDefaultBroker()
    {
        // prevent broker from starting in setUp
    }

    public void startBrokerNow() throws Exception
    {
        super.startDefaultBroker();

        // reset credentials
        getRestTestHelper().setUsernameAndPassword(null, null);
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        TestBrokerConfiguration config = getDefaultBrokerConfiguration();

        Map<String, Object> anonymousAuthProviderAttributes = new HashMap<String, Object>();
        anonymousAuthProviderAttributes.put(AuthenticationProvider.TYPE, AnonymousAuthenticationManager.PROVIDER_TYPE);
        anonymousAuthProviderAttributes.put(AuthenticationProvider.NAME, TestBrokerConfiguration.ENTRY_NAME_ANONYMOUS_PROVIDER);
        config.addObjectConfiguration(AuthenticationProvider.class, anonymousAuthProviderAttributes);

        // set anonymous authentication provider on http port for the tests
        config.setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, Port.AUTHENTICATION_PROVIDER,
                TestBrokerConfiguration.ENTRY_NAME_ANONYMOUS_PROVIDER);
        config.setObjectAttribute(Plugin.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT, HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED, false);

    }

    public void testGetWithAnonymousProvider() throws Exception
    {
        startBrokerNow();

        Map<String, Object> brokerDetails = getRestTestHelper().getJsonAsSingletonList("broker");
        assertNotNull("Unexpected broker attributes", brokerDetails);
        assertNotNull("Unexpected value of attribute " + Broker.ID, brokerDetails.get(Broker.ID));
    }

    public void testPutAnonymousProvider() throws Exception
    {
        startBrokerNow();

        Map<String, Object> brokerAttributes = new HashMap<String, Object>();
        String newBrokerName = getName();
        brokerAttributes.put(Broker.NAME, newBrokerName);

        getRestTestHelper().submitRequest("broker", "PUT", brokerAttributes, HttpServletResponse.SC_OK);

        Map<String, Object> brokerDetails = getRestTestHelper().getJsonAsSingletonList("broker");
        assertNotNull("Unexpected broker attributes", brokerDetails);
        assertNotNull("Unexpected value of attribute " + Broker.ID, brokerDetails.get(Broker.ID));
        assertEquals("Unexpected default virtual host", newBrokerName, brokerDetails.get(Broker.NAME));
    }

    public void testGetWithPasswordAuthProvider() throws Exception
    {
        getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, Port.AUTHENTICATION_PROVIDER,
                                                           TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER);
        startBrokerNow();

        getRestTestHelper().submitRequest("broker", "GET", HttpServletResponse.SC_UNAUTHORIZED);
    }

    public void testPutWithPasswordAuthProvider() throws Exception
    {
        String newBrokerName = getName();
        getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, Port.AUTHENTICATION_PROVIDER,
                                                           TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER);
        startBrokerNow();

        Map<String, Object> brokerAttributes = new HashMap<String, Object>();
        brokerAttributes.put(Broker.NAME, newBrokerName);

        getRestTestHelper().submitRequest("broker", "PUT", brokerAttributes, HttpServletResponse.SC_UNAUTHORIZED);
    }
}
