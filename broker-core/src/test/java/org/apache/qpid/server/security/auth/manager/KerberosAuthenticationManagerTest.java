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

import static org.apache.qpid.server.security.auth.manager.KerberosAuthenticationManager.GSSAPI_MECHANISM;
import static org.apache.qpid.server.test.KerberosUtilities.ACCEPT_SCOPE;
import static org.apache.qpid.server.test.KerberosUtilities.CLIENT_PRINCIPAL_FULL_NAME;
import static org.apache.qpid.server.test.KerberosUtilities.CLIENT_PRINCIPAL_NAME;
import static org.apache.qpid.server.test.KerberosUtilities.HOST_NAME;
import static org.apache.qpid.server.test.KerberosUtilities.LOGIN_CONFIG;
import static org.apache.qpid.server.test.KerberosUtilities.REALM;
import static org.apache.qpid.server.test.KerberosUtilities.SERVER_PROTOCOL;
import static org.apache.qpid.server.test.KerberosUtilities.SERVICE_PRINCIPAL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.qpid.server.security.TokenCarryingPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.test.KerberosUtilities;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.test.utils.EmbeddedKdcExtension;
import org.apache.qpid.test.utils.SystemPropertySetter;
import org.apache.qpid.test.utils.UnitTestBase;

public class KerberosAuthenticationManagerTest extends UnitTestBase
{
    private static final String ANOTHER_SERVICE = "foo/" + HOST_NAME;
    private static final String ANOTHER_SERVICE_FULL_NAME = ANOTHER_SERVICE + "@" + REALM;
    private static final KerberosUtilities UTILS = new KerberosUtilities();

    @RegisterExtension
    public static final EmbeddedKdcExtension KDC = new EmbeddedKdcExtension(
            HOST_NAME, 0, "QpidTestKerberosServer", REALM);

    @RegisterExtension
    public static final SystemPropertySetter SYSTEM_PROPERTY_SETTER = new SystemPropertySetter();

    private static File _clientKeyTabFile;
    private static String _config;

    private KerberosAuthenticationManager _kerberosAuthenticationProvider;
    private Broker<?> _broker;
    private SpnegoAuthenticator _spnegoAuthenticator;

    @BeforeAll
    public static void createKeyTabs() throws Exception
    {
        KDC.createPrincipal("another.keytab", ANOTHER_SERVICE_FULL_NAME);
        _config = UTILS.prepareConfiguration(HOST_NAME, SYSTEM_PROPERTY_SETTER);
        _clientKeyTabFile = UTILS.prepareKeyTabs(KDC);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        SYSTEM_PROPERTY_SETTER.setSystemProperty(LOGIN_CONFIG, _config);
        _broker = BrokerTestHelper.createBrokerMock();
        _kerberosAuthenticationProvider = createKerberosAuthenticationProvider(ACCEPT_SCOPE);
        _spnegoAuthenticator = new SpnegoAuthenticator(_kerberosAuthenticationProvider);
    }

    @Test
    public void testCreateSaslNegotiator() throws Exception
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn(HOST_NAME);
        final SaslNegotiator negotiator = _kerberosAuthenticationProvider.createSaslNegotiator(GSSAPI_MECHANISM,
                                                                                               saslSettings,
                                                                                               null);
        assertNotNull(negotiator, "Could not create SASL negotiator");
        try
        {
            final AuthenticationResult result = authenticate(negotiator);
            assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
            assertEquals(new KerberosPrincipal(CLIENT_PRINCIPAL_FULL_NAME).getName(), result.getMainPrincipal().getName());
        }
        finally
        {
            negotiator.dispose();
        }
    }

    @Test
    public void testSeveralKerberosAuthenticationProviders()
    {
        final Map<String, Object> attributes =
                Map.of(AuthenticationProvider.NAME, getTestName() + "2");
        final KerberosAuthenticationManager kerberosAuthenticationProvider =
                new KerberosAuthenticationManager(attributes, _broker);

        assertThrows(IllegalConfigurationException.class, kerberosAuthenticationProvider::create, "Exception expected");
    }

    @Test
    public void testCreateKerberosAuthenticationProvidersWithNonExistingJaasLoginModule()
    {
        when(_broker.getChildren(AuthenticationProvider.class)).thenReturn(Set.of());
        SYSTEM_PROPERTY_SETTER.setSystemProperty(LOGIN_CONFIG, "config.module." + System.nanoTime());
        final Map<String, Object> attributes = Map.of(AuthenticationProvider.NAME, getTestName());
        final KerberosAuthenticationManager kerberosAuthenticationProvider =
                new KerberosAuthenticationManager(attributes, _broker);

        assertThrows(IllegalConfigurationException.class, kerberosAuthenticationProvider::create, "Exception expected");
    }

    @Test
    public void testAuthenticateUsingNegotiationToken() throws Exception
    {
        final byte[] negotiationTokenBytes =
                UTILS.buildToken(CLIENT_PRINCIPAL_NAME, _clientKeyTabFile, SERVICE_PRINCIPAL_NAME);
        final String token = Base64.getEncoder().encodeToString(negotiationTokenBytes);
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;
        final AuthenticationResult result = _kerberosAuthenticationProvider.authenticate(authenticationHeader);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
    }

    @Test
    public void testAuthenticate() throws Exception
    {
        final String token = Base64.getEncoder().encodeToString(buildToken(SERVICE_PRINCIPAL_NAME));
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;
        final AuthenticationResult result = _spnegoAuthenticator.authenticate(authenticationHeader);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());

        final Principal principal = result.getMainPrincipal();
        assertTrue(principal instanceof TokenCarryingPrincipal);
        assertEquals(CLIENT_PRINCIPAL_FULL_NAME, principal.getName());

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
    public void testAuthenticateNoNegotiatePrefix() throws Exception
    {
        final String token = Base64.getEncoder().encodeToString(buildToken(SERVICE_PRINCIPAL_NAME));
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
    public void testAuthenticateWrongConfigName() throws Exception
    {
        _kerberosAuthenticationProvider.delete();

        _kerberosAuthenticationProvider = createKerberosAuthenticationProvider("foo");
        _spnegoAuthenticator = new SpnegoAuthenticator(_kerberosAuthenticationProvider);

        final String token = Base64.getEncoder().encodeToString(buildToken(SERVICE_PRINCIPAL_NAME));
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;

        final AuthenticationResult result = _spnegoAuthenticator.authenticate(authenticationHeader);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testAuthenticateWrongServer() throws Exception
    {
        final String token = Base64.getEncoder().encodeToString(buildToken(ANOTHER_SERVICE));
        final String authenticationHeader = SpnegoAuthenticator.NEGOTIATE_PREFIX + token;

        final AuthenticationResult result = _spnegoAuthenticator.authenticate(authenticationHeader);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    private KerberosAuthenticationManager createKerberosAuthenticationProvider(String acceptScope)
    {
        when(_broker.getChildren(AuthenticationProvider.class)).thenReturn(List.of());
        final Map<String, String> context = Map.of(KerberosAuthenticationManager.GSSAPI_SPNEGO_CONFIG, acceptScope);
        final Map<String, Object> attributes = Map.of(AuthenticationProvider.NAME, getTestName(),
                AuthenticationProvider.CONTEXT, context);
        KerberosAuthenticationManager kerberosAuthenticationProvider = new KerberosAuthenticationManager(attributes, _broker);
        kerberosAuthenticationProvider.create();
        when(_broker.getChildren(AuthenticationProvider.class)).thenReturn(Set.of(kerberosAuthenticationProvider));
        return kerberosAuthenticationProvider;
    }

    private byte[] buildToken(final String anotherService) throws Exception
    {
        return UTILS.buildToken(KerberosUtilities.CLIENT_PRINCIPAL_NAME, _clientKeyTabFile, anotherService);
    }

    private AuthenticationResult authenticate(final SaslNegotiator negotiator) throws Exception
    {
        final LoginContext lc = UTILS.createKerberosKeyTabLoginContext(getTestName(),CLIENT_PRINCIPAL_FULL_NAME,
                                                                       _clientKeyTabFile);

        Subject clientSubject = null;
        try
        {
            lc.login();
            clientSubject = lc.getSubject();
            debug("LoginContext subject {}", clientSubject);
            final SaslClient saslClient = createSaslClient(clientSubject);
            return performNegotiation(clientSubject, saslClient, negotiator);
        }
        finally
        {
            if (clientSubject != null)
            {
                lc.logout();
            }
        }
    }

    private void debug(final String message, final Object... args)
    {
        UTILS.debug(message, args);
    }

    private AuthenticationResult performNegotiation(final Subject clientSubject,
                                                    final SaslClient saslClient,
                                                    final SaslNegotiator negotiator)
            throws PrivilegedActionException
    {
        AuthenticationResult result;
        byte[] response = null;
        boolean initiated = false;
        do
        {
            if (!initiated)
            {
                initiated = true;
                debug("Sending initial challenge");
                response = Subject.doAs(clientSubject, (PrivilegedExceptionAction<byte[]>) () ->
                {
                    if (saslClient.hasInitialResponse())
                    {
                        return saslClient.evaluateChallenge(new byte[0]);
                    }
                    return null;
                });
                debug("Initial challenge sent");
            }

            debug("Handling response: {}", StringUtil.toHex(response));
            result = negotiator.handleResponse(response);

            final byte[] challenge = result.getChallenge();

            if (challenge != null)
            {
                debug("Challenge: {}", StringUtil.toHex(challenge));
                response = Subject.doAs(clientSubject, (PrivilegedExceptionAction<byte[]>) () ->
                        saslClient.evaluateChallenge(challenge));
            }
        }
        while (result.getStatus() == AuthenticationResult.AuthenticationStatus.CONTINUE);

        debug("Result {}", result.getStatus());
        return result;
    }

    private SaslClient createSaslClient(final Subject clientSubject) throws PrivilegedActionException
    {
        return Subject.doAs(clientSubject, (PrivilegedExceptionAction<SaslClient>) () ->
        {
            final Map<String, String> props = Map.of("javax.security.sasl.server.authentication", "true");
            return Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM}, null, SERVER_PROTOCOL, HOST_NAME, props, null);
        });
    }
}
