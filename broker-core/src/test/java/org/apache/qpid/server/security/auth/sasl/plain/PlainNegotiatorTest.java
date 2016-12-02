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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider;
import org.apache.qpid.test.utils.QpidTestCase;

public class PlainNegotiatorTest extends QpidTestCase
{
    private static final String VALID_PASSWORD = "testPassword";
    private static final String VALID_USERNAME = "testUsername";
    private static final String VALID_RESPONSE = String.format("\0%s\0%s", VALID_USERNAME, VALID_PASSWORD);
    private UsernamePasswordAuthenticationProvider _authenticationProvider;
    private PlainNegotiator _negotiator;
    private AuthenticationResult _expectedResult;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _expectedResult = mock(AuthenticationResult.class);
        _authenticationProvider = mock(UsernamePasswordAuthenticationProvider.class);
        when(_authenticationProvider.authenticate(eq(VALID_USERNAME), eq(VALID_PASSWORD))).thenReturn(_expectedResult);
        _negotiator = new PlainNegotiator(_authenticationProvider);
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        if (_negotiator != null)
        {
            _negotiator.dispose();
        }
    }

    public void testHandleResponse() throws Exception
    {
        final AuthenticationResult result = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        verify(_authenticationProvider).authenticate(eq(VALID_USERNAME), eq(VALID_PASSWORD));
        assertEquals("Unexpected authentication result", _expectedResult, result);
    }

    public void testMultipleAuthenticationAttempts() throws Exception
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        assertEquals("Unexpected first authentication result", _expectedResult, firstResult);
        final AuthenticationResult secondResult = _negotiator.handleResponse(VALID_RESPONSE.getBytes());
        assertEquals("Unexpected second authentication result", AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus());
    }
}