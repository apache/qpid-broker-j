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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
    public void setUp() throws Exception
    {
        _authenticationProvider = mock(OAuth2AuthenticationProvider.class);
        _negotiator = new OAuth2Negotiator(_authenticationProvider, null);
    }

    @Test
    public void testHandleResponse_ResponseHasAuthOnly()
    {
        doHandleResponseWithValidResponse(VALID_RESPONSE);
    }

    @Test
    public void testHandleResponse_ResponseAuthAndOthers()
    {
        doHandleResponseWithValidResponse(VALID_TOKEN_WITH_CRUD);
    }

    @Test
    public void testHandleResponse_ResponseAuthAbsent()
    {
        final AuthenticationResult actualResult = _negotiator.handleResponse(RESPONSE_WITH_NO_TOKEN);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, actualResult.getStatus(),
                "Unexpected result status");

        assertNull(actualResult.getMainPrincipal(), "Unexpected result principal");
    }

    @Test
    public void testHandleResponse_ResponseAuthMalformed()
    {
        final AuthenticationResult actualResult = _negotiator.handleResponse(RESPONSE_WITH_MALFORMED_AUTH);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, actualResult.getStatus(),
                "Unexpected result status");
        assertNull(actualResult.getMainPrincipal(), "Unexpected result principal");
    }

    private void doHandleResponseWithValidResponse(final byte[] validResponse)
    {
        final AuthenticationResult expectedResult = mock(AuthenticationResult.class);
        when(_authenticationProvider.authenticateViaAccessToken(eq(VALID_TOKEN), any())).thenReturn(expectedResult);
        final AuthenticationResult actualResult = _negotiator.handleResponse(validResponse);
        assertEquals(expectedResult, actualResult, "Unexpected result");

        verify(_authenticationProvider).authenticateViaAccessToken(eq(VALID_TOKEN), any());

        final AuthenticationResult secondResult = _negotiator.handleResponse(validResponse);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
    }

    @Test
    public void testHandleNoInitialResponse()
    {
        final AuthenticationResult result = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, result.getStatus(),
                "Unexpected authentication status");
        assertArrayEquals(new byte[0], result.getChallenge(), "Unexpected authentication challenge");
    }

    @Test
    public void testHandleNoInitialResponseNull()
    {
        final AuthenticationResult result = _negotiator.handleResponse(null);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, result.getStatus(),
                "Unexpected authentication status");
        assertArrayEquals(new byte[0], result.getChallenge(), "Unexpected authentication challenge");
    }
}
