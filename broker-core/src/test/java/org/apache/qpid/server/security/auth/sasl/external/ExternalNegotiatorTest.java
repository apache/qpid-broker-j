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

package org.apache.qpid.server.security.auth.sasl.external;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;

import javax.security.auth.x500.X500Principal;

import org.junit.Test;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.test.utils.UnitTestBase;

public class ExternalNegotiatorTest extends UnitTestBase
{
    private static final String VALID_USER_DN = "cn=test,dc=example,dc=com";
    private static final String VALID_USER_NAME = "test@example.com";
    private static final String USERNAME_NO_CN_DC = "ou=test,o=example,o=com";

    @Test
    public void testHandleResponseUseFullDNValidExternalPrincipal() throws Exception
    {
        ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        X500Principal externalPrincipal = new X500Principal(VALID_USER_DN);
        ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            firstResult.getStatus());

        String principalName = firstResult.getMainPrincipal().getName();
        assertTrue(String.format("Unexpected first result principal '%s'", principalName),
                          VALID_USER_DN.equalsIgnoreCase(principalName));


        AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
    }

    @Test
    public void testHandleResponseNotUseFullDNValidExternalPrincipal() throws Exception
    {
        ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(false);
        X500Principal externalPrincipal = new X500Principal(VALID_USER_DN);
        ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            firstResult.getStatus());
        String principalName = firstResult.getMainPrincipal().getName();
        assertEquals("Unexpected first result principal", VALID_USER_NAME, principalName);

        AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
    }

    @Test
    public void testHandleResponseNotUseFullDN_No_CN_DC_In_ExternalPrincipal() throws Exception
    {
        ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(false);
        X500Principal externalPrincipal = new X500Principal(USERNAME_NO_CN_DC);
        ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            firstResult.getStatus());
        assertNull("Unexpected first result principal", firstResult.getMainPrincipal());
    }

    @Test
    public void testHandleResponseUseFullDN_No_CN_DC_In_ExternalPrincipal() throws Exception
    {
        ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        X500Principal externalPrincipal = new X500Principal(USERNAME_NO_CN_DC);
        ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            firstResult.getStatus());
        String principalName = firstResult.getMainPrincipal().getName();
        assertTrue(String.format("Unexpected first result principal '%s'", principalName),
                          USERNAME_NO_CN_DC.equalsIgnoreCase(principalName));

        AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
    }

    @Test
    public void testHandleResponseFailsWithoutExternalPrincipal() throws Exception
    {
        ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, null);

        AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            firstResult.getStatus());
        assertNull("Unexpected first result principal", firstResult.getMainPrincipal());
    }


    @Test
    public void testHandleResponseSucceedsForNonX500Principal() throws Exception
    {
        ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        Principal principal = mock(Principal.class);
        ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, principal);

        AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            firstResult.getStatus());
        assertEquals("Unexpected first result principal", principal, firstResult.getMainPrincipal());

        AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
    }
}