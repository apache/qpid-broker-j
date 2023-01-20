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
package org.apache.qpid.server.security.auth;

import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper.assertOnlyContainsWrapped;
import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper
        .assertOnlyContainsWrappedAndSecondaryPrincipals;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class AuthenticationResultTest extends UnitTestBase
{
    @Test
    public void testConstructWithAuthenticationStatusContinue()
    {
        final AuthenticationResult authenticationResult =
                new AuthenticationResult(AuthenticationResult.AuthenticationStatus.CONTINUE);
        assertSame(AuthenticationResult.AuthenticationStatus.CONTINUE, authenticationResult.getStatus());
        assertTrue(authenticationResult.getPrincipals().isEmpty());
    }

    @Test
    public void testConstructWithAuthenticationStatusError()
    {
        final AuthenticationResult authenticationResult =
                new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
        assertSame(AuthenticationResult.AuthenticationStatus.ERROR, authenticationResult.getStatus());
        assertTrue(authenticationResult.getPrincipals().isEmpty());
    }

    @Test
    public void testConstructWithAuthenticationStatusSuccessThrowsException()
    {
        assertThrows(IllegalArgumentException.class,
                () -> new AuthenticationResult(AuthenticationResult.AuthenticationStatus.SUCCESS),
                "Exception not thrown");
    }

    @Test
    public void testConstructWithPrincipal()
    {
        final Principal mainPrincipal = mock(Principal.class);
        final AuthenticationResult authenticationResult = new AuthenticationResult(mainPrincipal);

        assertOnlyContainsWrapped(mainPrincipal, authenticationResult.getPrincipals());
        assertSame(AuthenticationResult.AuthenticationStatus.SUCCESS, authenticationResult.getStatus());
    }

    @Test
    public void testConstructWithNullPrincipalThrowsException()
    {
        assertThrows(IllegalArgumentException.class,
                () -> new AuthenticationResult((Principal) null),
                "Exception not thrown");
    }

    @Test
    public void testConstructWithSetOfPrincipals()
    {
        final Principal mainPrincipal = mock(Principal.class);
        final Principal secondaryPrincipal = mock(Principal.class);
        final Set<Principal> secondaryPrincipals = Set.of(secondaryPrincipal);
        final AuthenticationResult authenticationResult =
                new AuthenticationResult(mainPrincipal, secondaryPrincipals, null);

        assertOnlyContainsWrappedAndSecondaryPrincipals(mainPrincipal, secondaryPrincipals, authenticationResult.getPrincipals());
        assertSame(AuthenticationResult.AuthenticationStatus.SUCCESS, authenticationResult.getStatus());
    }

    @Test
    public void testConstructWithSetOfPrincipalsDeDuplicatesMainPrincipal()
    {
        final Principal mainPrincipal = mock(Principal.class);
        final Principal secondaryPrincipal = mock(Principal.class);
        final Set<Principal> secondaryPrincipalsContainingDuplicateOfMainPrincipal =
                Set.of(secondaryPrincipal, mainPrincipal);
        final Set<Principal> deDuplicatedSecondaryPrincipals = Set.of(secondaryPrincipal);
        final AuthenticationResult authenticationResult = new AuthenticationResult(
                mainPrincipal, secondaryPrincipalsContainingDuplicateOfMainPrincipal, null);

        assertOnlyContainsWrappedAndSecondaryPrincipals(mainPrincipal, deDuplicatedSecondaryPrincipals, authenticationResult.getPrincipals());

        assertSame(AuthenticationResult.AuthenticationStatus.SUCCESS, authenticationResult.getStatus());
    }
}
