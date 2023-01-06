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
package org.apache.qpid.server.security.auth.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.SaslUtil;
import org.apache.qpid.test.utils.UnitTestBase;

public class SimpleAuthenticationManagerTest extends UnitTestBase
{
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASSWORD = "testPassword";

    private SimpleAuthenticationManager _authenticationManager;

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> authManagerAttrs = Map.of(AuthenticationProvider.NAME,"MANAGEMENT_MODE_AUTHENTICATION",
                AuthenticationProvider.ID, randomUUID());
        final SimpleAuthenticationManager authManager = new SimpleAuthenticationManager(authManagerAttrs,
                BrokerTestHelper.createBrokerMock());
        authManager.addUser(TEST_USER, TEST_PASSWORD);
        _authenticationManager = authManager;
    }

    @Test
    public void testGetMechanisms()
    {
        final List<String> mechanisms = _authenticationManager.getMechanisms();
        assertEquals(4, (long) mechanisms.size(), "Unexpected number of mechanisms");
        assertTrue(mechanisms.contains("PLAIN"), "PLAIN was not present: " + mechanisms);
        assertTrue(mechanisms.contains("CRAM-MD5"), "CRAM-MD5 was not present: " + mechanisms);
        assertTrue(mechanisms.contains("SCRAM-SHA-1"), "SCRAM-SHA-1 was not present: " + mechanisms);
        assertTrue(mechanisms.contains("SCRAM-SHA-256"), "SCRAM-SHA-256 was not present: " + mechanisms);
    }

    @Test
    public void testCreateSaslNegotiatorForUnsupportedMechanisms()
    {
        final List<String> unsupported = List.of("EXTERNAL", "CRAM-MD5-HEX", "CRAM-MD5-HASHED", "ANONYMOUS", "GSSAPI");
        for (final String mechanism : unsupported)
        {
            final SaslNegotiator negotiator = _authenticationManager.createSaslNegotiator(mechanism, null, null);
            assertNull(negotiator, "Mechanism " + mechanism + " should not be supported by SimpleAuthenticationManager");
        }
    }

    @Test
    public void testAuthenticateWithPlainSaslServer() throws Exception
    {
        final AuthenticationResult result = authenticatePlain(TEST_USER, TEST_PASSWORD);
        assertAuthenticated(result);
    }

    @Test
    public void testAuthenticateWithPlainSaslServerInvalidPassword() throws Exception
    {
        final AuthenticationResult result = authenticatePlain(TEST_USER, "wrong-password");
        assertUnauthenticated(result);
    }

    @Test
    public void testAuthenticateWithPlainSaslServerInvalidUsername() throws Exception
    {
        final AuthenticationResult result = authenticatePlain("wrong-user", TEST_PASSWORD);
        assertUnauthenticated(result);
    }

    @Test
    public void testAuthenticateWithCramMd5SaslServer() throws Exception
    {
        final AuthenticationResult result = authenticateCramMd5(TEST_USER, TEST_PASSWORD);
        assertAuthenticated(result);
    }

    @Test
    public void testAuthenticateWithCramMd5SaslServerInvalidPassword() throws Exception
    {
        final AuthenticationResult result = authenticateCramMd5(TEST_USER, "wrong-password");
        assertUnauthenticated(result);
    }

    @Test
    public void testAuthenticateWithCramMd5SaslServerInvalidUsername() throws Exception
    {
        final AuthenticationResult result = authenticateCramMd5("wrong-user", TEST_PASSWORD);
        assertUnauthenticated(result);
    }

    @Test
    public void testAuthenticateValidCredentials()
    {
        final AuthenticationResult result = _authenticationManager.authenticate(TEST_USER, TEST_PASSWORD);
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus(), "Unexpected authentication result");
        assertAuthenticated(result);
    }

    @Test
    public void testAuthenticateInvalidPassword()
    {
        final AuthenticationResult result = _authenticationManager.authenticate(TEST_USER, "invalid");
        assertUnauthenticated(result);
    }

    @Test
    public void testAuthenticateInvalidUserName()
    {
        final AuthenticationResult result = _authenticationManager.authenticate("invalid", TEST_PASSWORD);
        assertUnauthenticated(result);
    }

    private void assertAuthenticated(final AuthenticationResult result)
    {
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus(), "Unexpected authentication result");
        final Principal principal = result.getMainPrincipal();
        assertEquals(TEST_USER, principal.getName(), "Unexpected principal name");
        final Set<Principal> principals = result.getPrincipals();
        assertEquals(1, (long) principals.size(), "Unexpected principals size");
        assertEquals(TEST_USER, principals.iterator().next().getName(), "Unexpected principal name");
    }

    private void assertUnauthenticated(final AuthenticationResult result)
    {
        assertEquals(AuthenticationStatus.ERROR, result.getStatus(), "Unexpected authentication result");
        assertNull(result.getMainPrincipal(), "Unexpected principal");
        final Set<Principal> principals = result.getPrincipals();
        assertEquals(0, (long) principals.size(), "Unexpected principals size");
    }

    private AuthenticationResult authenticatePlain(final String userName, final String userPassword) throws Exception
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        final SaslNegotiator saslNegotiator = _authenticationManager.createSaslNegotiator("PLAIN", saslSettings, null);
        final byte[] response = SaslUtil.generatePlainClientResponse(userName, userPassword);
        return saslNegotiator.handleResponse(response);
    }

    private AuthenticationResult authenticateCramMd5(final String userName, final String userPassword) throws Exception
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn("testHost");
        final SaslNegotiator saslNegotiator = _authenticationManager.createSaslNegotiator("CRAM-MD5", saslSettings, null);
        final AuthenticationResult result = saslNegotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationStatus.CONTINUE, result.getStatus(), "Unexpected SASL status");
        final byte[] challenge = result.getChallenge();
        final byte[] response = SaslUtil.generateCramMD5ClientResponse(userName, userPassword, challenge);
        return saslNegotiator.handleResponse(response);
    }
}
