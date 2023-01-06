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
package org.apache.qpid.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.test.utils.UnitTestBase;

public class SubjectCreatorTest extends UnitTestBase
{
    private static final UsernamePrincipal USERNAME_PRINCIPAL = new UsernamePrincipal("username", null);
    private static final String PASSWORD = "password";

    private final AuthenticationProvider<?> _authenticationProvider = mock(AuthenticationProvider.class);

    private final GroupProvider<?> _groupManager1 = mock(GroupProvider.class);
    private final GroupProvider<?> _groupManager2 = mock(GroupProvider.class);

    private final Principal _group1 = mock(Principal.class);
    private final Principal _group2 = mock(Principal.class);

    private SubjectCreator _subjectCreator;
    private AuthenticationResult _authenticationResult;
    private final SaslNegotiator _testSaslNegotiator = mock(SaslNegotiator.class);
    private final byte[] _saslResponseBytes = PASSWORD.getBytes();
    private EventLogger _eventLogger;

    @BeforeEach
    public void setUp()
    {
        when(_groupManager1.getGroupPrincipalsForUser(USERNAME_PRINCIPAL)).thenReturn(Set.of(_group1));
        when(_groupManager2.getGroupPrincipalsForUser(USERNAME_PRINCIPAL)).thenReturn(Set.of(_group2));

        _subjectCreator = new SubjectCreator(_authenticationProvider, Set.of(_groupManager1, _groupManager2), null);
        _eventLogger = mock(EventLogger.class);
        when(_authenticationProvider.getEventLogger()).thenReturn(_eventLogger);
        _authenticationResult = new AuthenticationResult(USERNAME_PRINCIPAL);
    }

    @Test
    public void testSaslAuthenticationSuccessReturnsSubjectWithUserAndGroupPrincipals()
    {
        when(_testSaslNegotiator.handleResponse(_saslResponseBytes)).thenReturn(_authenticationResult);

        final SubjectAuthenticationResult result = _subjectCreator.authenticate(_testSaslNegotiator, _saslResponseBytes);

        final Subject actualSubject = result.getSubject();
        assertEquals(3, (long) actualSubject.getPrincipals().size(),
                     "Should contain one user principal and two groups ");

        assertTrue(actualSubject.getPrincipals().contains(new AuthenticatedPrincipal(USERNAME_PRINCIPAL)));
        assertTrue(actualSubject.getPrincipals().contains(_group1));
        assertTrue(actualSubject.getPrincipals().contains(_group2));
        assertTrue(actualSubject.isReadOnly());
    }

    @Test
    public void testAuthenticateUnsuccessfulReturnsNullSubjectAndCorrectStatus()
    {
        testUnsuccessfulAuthentication(AuthenticationResult.AuthenticationStatus.CONTINUE);
        testUnsuccessfulAuthentication(AuthenticationResult.AuthenticationStatus.ERROR);
    }

    private void testUnsuccessfulAuthentication(final AuthenticationStatus expectedStatus)
    {
        final AuthenticationResult failedAuthenticationResult = new AuthenticationResult(expectedStatus);

        when(_testSaslNegotiator.handleResponse(_saslResponseBytes)).thenReturn(failedAuthenticationResult);

        final SubjectAuthenticationResult subjectAuthenticationResult =
                _subjectCreator.authenticate(_testSaslNegotiator, _saslResponseBytes);

        assertSame(expectedStatus, subjectAuthenticationResult.getStatus());
        assertNull(subjectAuthenticationResult.getSubject());

        if (expectedStatus == AuthenticationStatus.ERROR)
        {
            final ArgumentCaptor<LogMessage> argument = ArgumentCaptor.forClass(LogMessage.class);
            verify(_eventLogger).message(argument.capture());
            assertTrue(argument.getValue().toString().startsWith("ATH-1010"), "Unexpected operational log message");

        }
    }

    @Test
    public void testGetGroupPrincipals()
    {
        getAndAssertGroupPrincipals(_group1, _group2);
    }

    @Test
    public void testGetGroupPrincipalsWhenAGroupManagerReturnsNull()
    {
        when(_groupManager1.getGroupPrincipalsForUser(USERNAME_PRINCIPAL)).thenReturn(null);

        getAndAssertGroupPrincipals(_group2);
    }

    @Test
    public void testGetGroupPrincipalsWhenAGroupManagerReturnsEmptySet()
    {
        when(_groupManager2.getGroupPrincipalsForUser(USERNAME_PRINCIPAL)).thenReturn(new HashSet<>());

        getAndAssertGroupPrincipals(_group1);
    }

    private void getAndAssertGroupPrincipals(Principal... expectedGroups)
    {
        final Set<Principal> actualGroupPrincipals = _subjectCreator.getGroupPrincipals(USERNAME_PRINCIPAL);
        final Set<Principal> expectedGroupPrincipals = new HashSet<>(Arrays.asList(expectedGroups));
        assertEquals(expectedGroupPrincipals, actualGroupPrincipals);
    }
}
