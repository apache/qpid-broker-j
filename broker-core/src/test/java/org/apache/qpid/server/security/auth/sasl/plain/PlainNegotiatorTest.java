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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider;
import org.apache.qpid.test.utils.UnitTestBase;

public class PlainNegotiatorTest extends UnitTestBase
{
    private static final String VALID_PASSWORD = "testPassword";
    private static final String VALID_USERNAME = "testUsername";
    public static final String RESPONSE_FORMAT_STRING = "\0%s\0%s";
    private static final String VALID_RESPONSE = String.format(RESPONSE_FORMAT_STRING, VALID_USERNAME, VALID_PASSWORD);
    private UsernamePasswordAuthenticationProvider _authenticationProvider;
    private PlainNegotiator _negotiator;
    private AuthenticationResult _successfulResult;
    private AuthenticationResult _errorResult;

    @Before
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

    @After
    public void tearDown() throws Exception
    {
        if (_negotiator != null)
        {
            _negotiator.dispose();
        }
    }

    @Test
    public void testHandleResponse() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(VALID_RESPONSE.getBytes(US_ASCII));
        verify(_authenticationProvider).authenticate(eq(VALID_USERNAME), eq(VALID_PASSWORD));
        assertEquals("Unexpected authentication result", _successfulResult, result);
    }

    @Test
    public void testMultipleAuthenticationAttempts() throws Exception
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected first authentication result", _successfulResult, firstResult);
        final AuthenticationResult secondResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected second authentication result",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
    }

    @Test
    public void testHandleInvalidUser() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(String.format(RESPONSE_FORMAT_STRING, "invalidUser", VALID_PASSWORD).getBytes(US_ASCII));
        assertEquals("Unexpected authentication result", _errorResult, result);
    }

    @Test
    public void testHandleInvalidPassword() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(String.format(RESPONSE_FORMAT_STRING, VALID_USERNAME, "invalidPassword").getBytes(US_ASCII));
        assertEquals("Unexpected authentication result", _errorResult, result);
    }

    @Test
    public void testHandleNeverSendAResponse() throws Exception
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected authentication status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            firstResult.getStatus());
        assertArrayEquals("Unexpected authentication challenge", new byte[0], firstResult.getChallenge());

        final AuthenticationResult secondResult = _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first authentication result",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
    }

    @Test
    public void testHandleNoInitialResponse() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected authentication status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            result.getStatus());
        assertArrayEquals("Unexpected authentication challenge", new byte[0], result.getChallenge());

        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        assertEquals("Unexpected first authentication result", _successfulResult, firstResult);
    }

    @Test
    public void testHandleNoInitialResponseNull() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(null);
        assertEquals("Unexpected authentication status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            result.getStatus());
        assertArrayEquals("Unexpected authentication challenge", new byte[0], result.getChallenge());

        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        assertEquals("Unexpected first authentication result", _successfulResult, firstResult);
    }
}