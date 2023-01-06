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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.test.utils.UnitTestBase;

public class ExternalNegotiatorTest extends UnitTestBase
{
    private static final String VALID_USER_DN = "cn=test,dc=example,dc=com";
    private static final String VALID_USER_NAME = "test@example.com";
    private static final String USERNAME_NO_CN_DC = "ou=test,o=example,o=com";

    @Test
    public void testHandleResponseUseFullDNValidExternalPrincipal()
    {
        final ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        final X500Principal externalPrincipal = new X500Principal(VALID_USER_DN);
        final ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        final AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, firstResult.getStatus(),
                "Unexpected first result status");

        final String principalName = firstResult.getMainPrincipal().getName();
        assertTrue(VALID_USER_DN.equalsIgnoreCase(principalName), String.format("Unexpected first result principal '%s'",
                principalName));


        final AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
    }

    @Test
    public void testHandleResponseNotUseFullDNValidExternalPrincipal()
    {
        final ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(false);
        final X500Principal externalPrincipal = new X500Principal(VALID_USER_DN);
        final ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        final AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, firstResult.getStatus(),
                "Unexpected first result status");
        final String principalName = firstResult.getMainPrincipal().getName();
        assertEquals(VALID_USER_NAME, principalName, "Unexpected first result principal");

        final AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
    }

    @Test
    public void testHandleResponseNotUseFullDN_No_CN_DC_In_ExternalPrincipal()
    {
        final ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(false);
        final X500Principal externalPrincipal = new X500Principal(USERNAME_NO_CN_DC);
        final ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        final AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, firstResult.getStatus(),
                "Unexpected first result status");
        assertNull(firstResult.getMainPrincipal(), "Unexpected first result principal");
    }

    @Test
    public void testHandleResponseUseFullDN_No_CN_DC_In_ExternalPrincipal()
    {
        final ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        final X500Principal externalPrincipal = new X500Principal(USERNAME_NO_CN_DC);
        final ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, externalPrincipal);

        final AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, firstResult.getStatus(),
                "Unexpected first result status");
        final String principalName = firstResult.getMainPrincipal().getName();
        assertTrue(USERNAME_NO_CN_DC.equalsIgnoreCase(principalName),
                String.format("Unexpected first result principal '%s'", principalName));

        final AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
    }

    @Test
    public void testHandleResponseFailsWithoutExternalPrincipal()
    {
        final ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        final ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, null);

        final AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, firstResult.getStatus(),
                "Unexpected first result status");
        assertNull(firstResult.getMainPrincipal(), "Unexpected first result principal");
    }


    @Test
    public void testHandleResponseSucceedsForNonX500Principal()
    {
        final ExternalAuthenticationManager<?> externalAuthenticationManager = mock(ExternalAuthenticationManager.class);
        when(externalAuthenticationManager.getUseFullDN()).thenReturn(true);
        final Principal principal = mock(Principal.class);
        final ExternalNegotiator negotiator = new ExternalNegotiator(externalAuthenticationManager, principal);

        final AuthenticationResult firstResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, firstResult.getStatus(),
                "Unexpected first result status");
        assertEquals(principal, firstResult.getMainPrincipal(), "Unexpected first result principal");

        final AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
    }
}