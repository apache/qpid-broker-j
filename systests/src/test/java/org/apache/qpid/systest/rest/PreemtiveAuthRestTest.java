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

import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.UNTRUSTED_KEYSTORE;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Plugin;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.test.utils.TestSSLConstants;

public class PreemtiveAuthRestTest extends QpidRestTestCase
{
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";

    @Override
    public void startDefaultBroker() throws Exception
    {
        //don't call super method, we will configure the broker in the test before doing so
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        //do nothing, we will configure this locally
    }

    private void configure(boolean useSsl, final boolean useClientAuth) throws Exception
    {
        super.customizeConfiguration();

        setSystemProperty("javax.net.debug", "ssl");
        if (useSsl)
        {
            Map<String, Object> portAttributes = new HashMap<>();
            portAttributes.put(Port.PROTOCOLS, Collections.singleton(Protocol.HTTP));
            portAttributes.put(Port.TRANSPORTS, Collections.singleton(Transport.SSL));
            portAttributes.put(Port.KEY_STORE, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE);

            if (useClientAuth)
            {
                portAttributes.put(Port.TRUST_STORES, Collections.singleton(TestBrokerConfiguration.ENTRY_NAME_SSL_TRUSTSTORE));
                portAttributes.put(Port.NEED_CLIENT_AUTH, "true");
                portAttributes.put(Port.AUTHENTICATION_PROVIDER, EXTERNAL_AUTHENTICATION_PROVIDER);

                Map<String, Object> externalProviderAttributes = new HashMap<>();
                externalProviderAttributes.put(AuthenticationProvider.TYPE, ExternalAuthenticationManager.PROVIDER_TYPE);
                externalProviderAttributes.put(AuthenticationProvider.NAME, EXTERNAL_AUTHENTICATION_PROVIDER);
                getDefaultBrokerConfiguration().addObjectConfiguration(AuthenticationProvider.class, externalProviderAttributes);
            }

            getDefaultBrokerConfiguration().setObjectAttributes(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, portAttributes);
        }
    }

    private void verifyGetBrokerAttempt(int responseCode) throws IOException
    {
        assertEquals(responseCode, getRestTestHelper().submitRequest("broker", "GET"));
    }

    public void testBasicAuth() throws Exception
    {
        configure(false, false);
        super.startDefaultBroker();

        _restTestHelper.setUsernameAndPassword(USERNAME, PASSWORD);
        verifyGetBrokerAttempt(HttpServletResponse.SC_OK);
    }

    public void testBasicAuth_WrongPassword() throws Exception
    {
        configure(false, false);
        super.startDefaultBroker();

        _restTestHelper.setUsernameAndPassword(USERNAME, "badpassword");
        verifyGetBrokerAttempt(HttpServletResponse.SC_UNAUTHORIZED);
    }

    public void testBasicAuthWhenDisabled() throws Exception
    {
        configure(false, false);
        getDefaultBrokerConfiguration().setObjectAttribute(Plugin.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT, HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED, false);
        super.startDefaultBroker();
        getRestTestHelper().setUseSsl(false);
        // Try the attempt with authentication, it should fail because
        // BASIC auth is disabled by default on non-secure connections.
        getRestTestHelper().setUsernameAndPassword(USERNAME, PASSWORD);
        verifyGetBrokerAttempt(HttpServletResponse.SC_UNAUTHORIZED);
    }

    public void testBasicAuth_Https() throws Exception
    {
        configure(true, false);
        super.startDefaultBroker();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpsPort());
        _restTestHelper.setUseSsl(true);
        _restTestHelper.setTruststore(TRUSTSTORE, TRUSTSTORE_PASSWORD);

        // Try the attempt with authentication, it should succeed because
        // BASIC auth is enabled by default on secure connections.
        _restTestHelper.setUsernameAndPassword(USERNAME, PASSWORD);
        verifyGetBrokerAttempt(HttpServletResponse.SC_OK);
    }

    public void testBasicAuthWhenDisabled_Https() throws Exception
    {
        configure(true, false);
        getDefaultBrokerConfiguration().setObjectAttribute(Plugin.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT, HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED, false);
        super.startDefaultBroker();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpsPort());
        _restTestHelper.setUseSsl(true);
        _restTestHelper.setTruststore(TRUSTSTORE, TRUSTSTORE_PASSWORD);

        // Try the attempt with authentication, it should fail because
        // BASIC auth is now disabled on secure connections.
        _restTestHelper.setUsernameAndPassword(USERNAME, PASSWORD);
        verifyGetBrokerAttempt(HttpServletResponse.SC_UNAUTHORIZED);
    }

    public void testClientCertAuth() throws Exception
    {
        configure(true, true);
        super.startDefaultBroker();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpsPort());
        _restTestHelper.setUseSsl(true);
        _restTestHelper.setUseSslAuth(true);
        _restTestHelper.setTruststore(TRUSTSTORE, TRUSTSTORE_PASSWORD);
        _restTestHelper.setKeystore(KEYSTORE, KEYSTORE_PASSWORD);

        _restTestHelper.setUsernameAndPassword(null, null);
        verifyGetBrokerAttempt(HttpServletResponse.SC_OK);
    }

    public void testClientCertAuth_UntrustedClientCert() throws Exception
    {
        configure(true, true);
        super.startDefaultBroker();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpsPort());
        _restTestHelper.setUseSsl(true);
        _restTestHelper.setUseSslAuth(true);
        _restTestHelper.setTruststore(TRUSTSTORE, TRUSTSTORE_PASSWORD);
        _restTestHelper.setKeystore(UNTRUSTED_KEYSTORE, KEYSTORE_PASSWORD);
        _restTestHelper.setClientAuthAlias(TestSSLConstants.CERT_ALIAS_UNTRUSTED_CLIENT);

        _restTestHelper.setUsernameAndPassword(null, null);

        try
        {
            getRestTestHelper().submitRequest("broker", "GET");
            fail("Exception not thrown");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void testPreemptiveDoesNotCreateSession() throws Exception
    {
        configure(false, false);
        super.startDefaultBroker();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());

        _restTestHelper.setUsernameAndPassword(USERNAME, PASSWORD);
        final HttpURLConnection firstConnection = _restTestHelper.openManagementConnection("broker", "GET");
        assertEquals("Unexpected server response", HttpServletResponse.SC_OK, firstConnection.getResponseCode());
        List<String> cookies = firstConnection.getHeaderFields().get("Set-Cookie");
        assertNull("Should not create session cookies", cookies);
    }
}
