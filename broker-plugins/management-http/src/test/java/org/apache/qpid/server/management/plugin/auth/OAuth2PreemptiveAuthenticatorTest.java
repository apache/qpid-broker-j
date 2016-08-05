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
package org.apache.qpid.server.management.plugin.auth;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.test.utils.QpidTestCase;

public class OAuth2PreemptiveAuthenticatorTest extends QpidTestCase
{
    private static final String TEST_AUTHORIZED_USER = "testAuthorizedUser";
    private static final String TEST_UNAUTHORIZED_USER = "testUnauthorizedUser";
    private static final String TEST_VALID_ACCESS_TOKEN = "testValidAccessToken";
    private static final String TEST_INVALID_ACCESS_TOKEN = "testInvalidAccessToken";
    private static final String TEST_UNAUTHORIZED_ACCESS_TOKEN = "testUnauthorizedAccessToken";

    private OAuth2PreemptiveAuthenticator _authenticator;
    private HttpManagementConfiguration _mockConfiguration;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        OAuth2AuthenticationProvider<?> mockAuthProvider = createMockOAuth2AuthenticationProvider();
        _mockConfiguration = mock(HttpManagementConfiguration.class);
        when(_mockConfiguration.getAuthenticationProvider(any(HttpServletRequest.class))).thenReturn(mockAuthProvider);

        _authenticator = new OAuth2PreemptiveAuthenticator();
    }

    public void testAttemptAuthenticationSuccessful() throws Exception
    {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("Authorization")).thenReturn("Bearer " + TEST_VALID_ACCESS_TOKEN);
        Subject subject = _authenticator.attemptAuthentication(mockRequest, _mockConfiguration);
        assertNotNull("Authenticator failed unexpectedly", subject);
        final Set<Principal> principals = subject.getPrincipals();
        assertEquals("Subject created with unexpected principal", TEST_AUTHORIZED_USER, principals.iterator().next().getName());
    }

    public void testAttemptAuthenticationUnauthorizedUser() throws Exception
    {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("Authorization")).thenReturn("Bearer " + TEST_UNAUTHORIZED_ACCESS_TOKEN);
        Subject subject = _authenticator.attemptAuthentication(mockRequest, _mockConfiguration);
        assertNotNull("Authenticator failed unexpectedly", subject);
        final Set<Principal> principals = subject.getPrincipals();
        assertEquals("Subject created with unexpected principal", TEST_UNAUTHORIZED_USER, principals.iterator().next().getName());
    }

    public void testAttemptAuthenticationInvalidToken() throws Exception
    {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("Authorization")).thenReturn("Bearer " + TEST_INVALID_ACCESS_TOKEN);
        Subject subject = _authenticator.attemptAuthentication(mockRequest, _mockConfiguration);
        assertNull("Authenticator did not fail with invalid access token", subject);
    }

    public void testAttemptAuthenticationMissingHeader() throws Exception
    {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        Subject subject = _authenticator.attemptAuthentication(mockRequest, _mockConfiguration);
        assertNull("Authenticator did not failed without authentication header", subject);
    }

    public void testAttemptAuthenticationMalformedHeader() throws Exception
    {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("Authorization")).thenReturn("malformed Bearer " + TEST_UNAUTHORIZED_ACCESS_TOKEN);
        Subject subject = _authenticator.attemptAuthentication(mockRequest, _mockConfiguration);
        assertNull("Authenticator did not failed with malformed authentication header", subject);
    }

    private OAuth2AuthenticationProvider<?> createMockOAuth2AuthenticationProvider() throws URISyntaxException
    {
        OAuth2AuthenticationProvider authenticationProvider = mock(OAuth2AuthenticationProvider.class);
        SubjectCreator mockSubjectCreator = mock(SubjectCreator.class);
        SubjectAuthenticationResult mockSuccessfulSubjectAuthenticationResult = mock(SubjectAuthenticationResult.class);
        SubjectAuthenticationResult mockUnauthorizedSubjectAuthenticationResult = mock(SubjectAuthenticationResult.class);
        final Subject successfulSubject = new Subject(true,
                                                      Collections.singleton(new AuthenticatedPrincipal(new UsernamePrincipal(
                                                              TEST_AUTHORIZED_USER,
                                                              null))),
                                                      Collections.emptySet(),
                                                      Collections.emptySet());
        final Subject unauthorizedSubject = new Subject(true,
                                                        Collections.singleton(new AuthenticatedPrincipal(new UsernamePrincipal(
                                                                TEST_UNAUTHORIZED_USER,
                                                                null))),
                                                        Collections.emptySet(),
                                                        Collections.emptySet());
        AuthenticationResult mockSuccessfulAuthenticationResult = mock(AuthenticationResult.class);
        AuthenticationResult mockUnauthorizedAuthenticationResult = mock(AuthenticationResult.class);
        AuthenticationResult failedAuthenticationResult = new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                                                                   new Exception("authentication failed"));
        SubjectAuthenticationResult failedSubjectAuthenticationResult = new SubjectAuthenticationResult(failedAuthenticationResult);

        when(authenticationProvider.getSubjectCreator(any(Boolean.class))).thenReturn(mockSubjectCreator);
        when(authenticationProvider.authenticateViaAccessToken(TEST_VALID_ACCESS_TOKEN)).thenReturn(mockSuccessfulAuthenticationResult);
        when(authenticationProvider.authenticateViaAccessToken(TEST_INVALID_ACCESS_TOKEN)).thenReturn(failedAuthenticationResult);
        when(authenticationProvider.authenticateViaAccessToken(TEST_UNAUTHORIZED_ACCESS_TOKEN)).thenReturn(mockUnauthorizedAuthenticationResult);

        when(mockSuccessfulSubjectAuthenticationResult.getSubject()).thenReturn(successfulSubject);
        when(mockUnauthorizedSubjectAuthenticationResult.getSubject()).thenReturn(unauthorizedSubject);

        when(mockSubjectCreator.createResultWithGroups(mockSuccessfulAuthenticationResult)).thenReturn(mockSuccessfulSubjectAuthenticationResult);
        when(mockSubjectCreator.createResultWithGroups(mockUnauthorizedAuthenticationResult)).thenReturn(mockUnauthorizedSubjectAuthenticationResult);
        when(mockSubjectCreator.createResultWithGroups(failedAuthenticationResult)).thenReturn(failedSubjectAuthenticationResult);

        return authenticationProvider;
    }
}
