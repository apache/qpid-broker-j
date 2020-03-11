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

package org.apache.qpid.server.security.auth.manager;

import static org.apache.commons.codec.CharEncoding.UTF_8;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.security.Principal;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.security.TokenCarryingPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.test.EmbeddedKdcResource;
import org.apache.qpid.server.test.KerberosUtilities;
import org.apache.qpid.test.utils.JvmVendor;
import org.apache.qpid.test.utils.SystemPropertySetter;
import org.apache.qpid.test.utils.UnitTestBase;

public class SpnegoAuthenticatorTest extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpnegoAuthenticatorTest.class);
    private static final String CLIENT_NAME = "client";
    private static final String HOST_NAME = InetAddress.getLoopbackAddress().getCanonicalHostName();
    private static final String SERVER_NAME = "AMQP/" + HOST_NAME;
    private static final String ANOTHER_SERVICE = "foo/" + HOST_NAME;
    private static final String REALM = "QPID.ORG";
    private static final String LOGIN_CONFIG = "login.config";
    private static final KerberosUtilities UTILS = new KerberosUtilities();;

    @ClassRule
    public static final EmbeddedKdcResource KDC = new EmbeddedKdcResource(HOST_NAME, 0, "QpidTestKerberosServer", REALM);

    @ClassRule
    public static final SystemPropertySetter SYSTEM_PROPERTY_SETTER = new SystemPropertySetter();

    private SpnegoAuthenticator _spnegoAuthenticator;
    private KerberosAuthenticationManager _kerberosAuthenticationManager;

    @BeforeClass
    public static void createKeyTabs() throws Exception
    {
        assumeThat(getJvmVendor(), not(JvmVendor.IBM));
        KDC.createPrincipal("broker.keytab", SERVER_NAME + "@" + REALM);
        KDC.createPrincipal("client.keytab", CLIENT_NAME + "@" + REALM);
        KDC.createPrincipal("another.keytab", ANOTHER_SERVICE + "@" + REALM);
        final URL resource = KerberosAuthenticationManagerTest.class.getClassLoader().getResource(LOGIN_CONFIG);
        LOGGER.debug("JAAS config:" + resource);
        assertNotNull(resource);
        SYSTEM_PROPERTY_SETTER.setSystemProperty("java.security.auth.login.config", URLDecoder.decode(resource.getPath(), UTF_8));
        SYSTEM_PROPERTY_SETTER.setSystemProperty("javax.security.auth.useSubjectCredsOnly", "false");

        KerberosUtilities.debugConfig();
    }

    @Before
    public void setUp()
    {
        _kerberosAuthenticationManager = mock(KerberosAuthenticationManager.class);
        when(_kerberosAuthenticationManager.getSpnegoLoginConfigScope()).thenReturn("com.sun.security.jgss.accept");
        when(_kerberosAuthenticationManager.isStripRealmFromPrincipalName()).thenReturn(true);

        _spnegoAuthenticator = new SpnegoAuthenticator(_kerberosAuthenticationManager);
    }

    @Test
    public void testAuthenticate() throws GSSException
    {
        final String token = Base64.getEncoder().encodeToString(buildToken(SERVER_NAME));
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;

        final AuthenticationResult result = _spnegoAuthenticator.authenticate(authenticationHeader);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        final Principal principal = result.getMainPrincipal();
        assertTrue(principal instanceof TokenCarryingPrincipal);
        assertEquals(CLIENT_NAME, principal.getName());

        final Map<String, String> tokens = ((TokenCarryingPrincipal)principal).getTokens();
        assertNotNull(tokens);
        assertTrue(tokens.containsKey(SpnegoAuthenticator.RESPONSE_AUTH_HEADER_NAME));
    }

    @Test
    public void testAuthenticateNoAuthenticationHeader()
    {
        final AuthenticationResult result = _spnegoAuthenticator.authenticate((String) null);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testAuthenticateNoNegotiatePrefix() throws GSSException
    {
        final String token = Base64.getEncoder().encodeToString(buildToken(SERVER_NAME));
        final AuthenticationResult result = _spnegoAuthenticator.authenticate(token);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testAuthenticateEmptyToken()
    {
        final AuthenticationResult result =
                _spnegoAuthenticator.authenticate(SpnegoAuthenticator.NEGOTIATE_PREFIX + "");
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testAuthenticateInvalidToken()
    {
        final AuthenticationResult result =
                _spnegoAuthenticator.authenticate(SpnegoAuthenticator.NEGOTIATE_PREFIX + "Zm9v");
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testAuthenticateWrongConfigName() throws GSSException
    {
        when(_kerberosAuthenticationManager.getSpnegoLoginConfigScope()).thenReturn("foo");
        final String token = Base64.getEncoder().encodeToString(buildToken(SERVER_NAME));
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;

        final AuthenticationResult result = _spnegoAuthenticator.authenticate(authenticationHeader);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testAuthenticateWrongServer() throws GSSException
    {
        final String token = Base64.getEncoder().encodeToString(buildToken(ANOTHER_SERVICE));
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;

        final AuthenticationResult result = _spnegoAuthenticator.authenticate(authenticationHeader);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    private byte[] buildToken(final String anotherService) throws GSSException
    {
        return UTILS.buildToken(CLIENT_NAME, anotherService);
    }
}
