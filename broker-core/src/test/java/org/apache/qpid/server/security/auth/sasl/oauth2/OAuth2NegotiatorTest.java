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

package org.apache.qpid.server.security.auth.sasl.oauth2;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.test.utils.UnitTestBase;

public class OAuth2NegotiatorTest extends UnitTestBase
{
    private static final String VALID_TOKEN = "token";
    private static final byte[] VALID_RESPONSE = ("auth=Bearer " + VALID_TOKEN + "\1\1").getBytes();
    private static final byte[] VALID_TOKEN_WITH_CRUD =
            ("user=xxx\1auth=Bearer " + VALID_TOKEN + "\1host=localhost\1\1").getBytes();
    private static final byte[] RESPONSE_WITH_NO_TOKEN = "host=localhost\1\1".getBytes();
    private static final byte[] RESPONSE_WITH_MALFORMED_AUTH = "auth=wibble\1\1".getBytes();
    private OAuth2Negotiator _negotiator;
    private OAuth2AuthenticationProvider<?> _authenticationProvider;

    @Before
    public void setUp() throws Exception
    {
        _authenticationProvider = mock(OAuth2AuthenticationProvider.class);
        _negotiator = new OAuth2Negotiator(_authenticationProvider, null);
    }

    @Test
    public void testHandleResponse_ResponseHasAuthOnly() throws Exception
    {
        doHandleResponseWithValidResponse(VALID_RESPONSE);
    }

    @Test
    public void testHandleResponse_ResponseAuthAndOthers() throws Exception
    {
        doHandleResponseWithValidResponse(VALID_TOKEN_WITH_CRUD);
    }

    @Test
    public void testHandleResponse_ResponseAuthAbsent() throws Exception
    {
        AuthenticationResult actualResult = _negotiator.handleResponse(RESPONSE_WITH_NO_TOKEN);
        assertEquals("Unexpected result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            actualResult.getStatus());

        assertNull("Unexpected result principal", actualResult.getMainPrincipal());
    }

    @Test
    public void testHandleResponse_ResponseAuthMalformed() throws Exception
    {
        AuthenticationResult actualResult = _negotiator.handleResponse(RESPONSE_WITH_MALFORMED_AUTH);
        assertEquals("Unexpected result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            actualResult.getStatus());
        assertNull("Unexpected result principal", actualResult.getMainPrincipal());
    }

    private void doHandleResponseWithValidResponse(final byte[] validResponse)
    {
        final AuthenticationResult expectedResult = mock(AuthenticationResult.class);
        when(_authenticationProvider.authenticateViaAccessToken(eq(VALID_TOKEN), any())).thenReturn(expectedResult);
        AuthenticationResult actualResult = _negotiator.handleResponse(validResponse);
        assertEquals("Unexpected result", expectedResult, actualResult);

        verify(_authenticationProvider).authenticateViaAccessToken(eq(VALID_TOKEN), any());

        AuthenticationResult secondResult = _negotiator.handleResponse(validResponse);
        assertEquals("Unexpected second result status",
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
    }

    @Test
    public void testHandleNoInitialResponseNull() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(null);
        assertEquals("Unexpected authentication status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            result.getStatus());
        assertArrayEquals("Unexpected authentication challenge", new byte[0], result.getChallenge());
    }
}
