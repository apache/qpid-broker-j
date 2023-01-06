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

package org.apache.qpid.server.security.auth.sasl.plain;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider;
import org.apache.qpid.test.utils.UnitTestBase;

public class PlainNegotiatorTest extends UnitTestBase
{
    private static final String VALID_PASSWORD = "testPassword";
    private static final String VALID_USERNAME = "testUsername";
    public static final String RESPONSE_FORMAT_STRING = "\0%s\0%s";
    private static final String VALID_RESPONSE = String.format(RESPONSE_FORMAT_STRING, VALID_USERNAME, VALID_PASSWORD);

    private UsernamePasswordAuthenticationProvider<?> _authenticationProvider;
    private PlainNegotiator _negotiator;
    private AuthenticationResult _successfulResult;
    private AuthenticationResult _errorResult;

    @BeforeEach
    public void setUp() throws Exception
    {
        _successfulResult = mock(AuthenticationResult.class);
        _errorResult = mock(AuthenticationResult.class);
        _authenticationProvider = mock(UsernamePasswordAuthenticationProvider.class);
        when(_authenticationProvider.authenticate(eq(VALID_USERNAME), eq(VALID_PASSWORD))).thenReturn(_successfulResult);
        when(_authenticationProvider.authenticate(eq(VALID_USERNAME), not(eq(VALID_PASSWORD)))).thenReturn(_errorResult);
        when(_authenticationProvider.authenticate(not(eq(VALID_USERNAME)), anyString())).thenReturn(_errorResult);
        _negotiator = new PlainNegotiator(_authenticationProvider);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_negotiator != null)
        {
            _negotiator.dispose();
        }
    }

    @Test
    public void testHandleResponse()
    {
        final AuthenticationResult result = _negotiator.handleResponse(VALID_RESPONSE.getBytes(US_ASCII));
        verify(_authenticationProvider).authenticate(eq(VALID_USERNAME), eq(VALID_PASSWORD));
        assertEquals(_successfulResult, result, "Unexpected authentication result");
    }

    @Test
    public void testMultipleAuthenticationAttempts()
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes(US_ASCII));
        assertEquals(_successfulResult, firstResult, "Unexpected first authentication result");
        final AuthenticationResult secondResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes(US_ASCII));
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second authentication result");
    }

    @Test
    public void testHandleInvalidUser()
    {
        final AuthenticationResult result = _negotiator.handleResponse(String.format(RESPONSE_FORMAT_STRING, "invalidUser", VALID_PASSWORD).getBytes(US_ASCII));
        assertEquals(_errorResult, result, "Unexpected authentication result");
    }

    @Test
    public void testHandleInvalidPassword()
    {
        final AuthenticationResult result = _negotiator.handleResponse(String.format(RESPONSE_FORMAT_STRING, VALID_USERNAME, "invalidPassword").getBytes(US_ASCII));
        assertEquals(_errorResult, result, "Unexpected authentication result");
    }

    @Test
    public void testHandleNeverSendAResponse()
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, firstResult.getStatus(),
                "Unexpected authentication status");
        assertArrayEquals(new byte[0], firstResult.getChallenge(), "Unexpected authentication challenge");

        final AuthenticationResult secondResult = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected first authentication result");
    }

    @Test
    public void testHandleNoInitialResponse()
    {
        final AuthenticationResult result = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, result.getStatus(),
                "Unexpected authentication status");
        assertArrayEquals(new byte[0], result.getChallenge(), "Unexpected authentication challenge");

        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        assertEquals(_successfulResult, firstResult, "Unexpected first authentication result");
    }

    @Test
    public void testHandleNoInitialResponseNull()
    {
        final AuthenticationResult result = _negotiator.handleResponse(null);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, result.getStatus(),
                "Unexpected authentication status");
        assertArrayEquals(new byte[0], result.getChallenge(), "Unexpected authentication challenge");

        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        assertEquals(_successfulResult, firstResult, "Unexpected first authentication result");
    }
}