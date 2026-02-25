/*
 *
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
package org.apache.qpid.server.management.plugin.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.MultiMap;
import org.eclipse.jetty.util.UrlEncoded;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.test.utils.UnitTestBase;

public class OAuth2InteractiveAuthenticatorTest extends UnitTestBase
{
    private static final String TEST_AUTHORIZATION_ENDPOINT = "testAuthEndpoint";
    private static final int TEST_PORT = 64756;
    private static final int TEST_REMOTE_PORT = 0;
    private static final String TEST_OAUTH2_SCOPE = "testScope";
    private static final String TEST_REQUEST_HOST = "http://localhost";
    private static final String TEST_REQUEST_PATH = "/foo/bar";
    private static final String TEST_REQUEST_QUERY = "?baz=fnord";
    private static final String TEST_REQUEST = TEST_REQUEST_HOST + ":" + TEST_PORT + TEST_REQUEST_PATH + TEST_REQUEST_QUERY;
    private static final String TEST_CLIENT_ID = "testClientId";
    private static final String TEST_STATE = "testState";
    private static final String TEST_VALID_AUTHORIZATION_CODE = "testValidAuthorizationCode";
    private static final String TEST_INVALID_AUTHORIZATION_CODE = "testInvalidAuthorizationCode";
    private static final String TEST_UNAUTHORIZED_AUTHORIZATION_CODE = "testUnauthorizedAuthorizationCode";
    private static final String TEST_AUTHORIZED_USER = "testAuthorizedUser";
    private static final String TEST_UNAUTHORIZED_USER = "testUnauthorizedUser";
    private static final String ATTR_SUBJECT = "Qpid.subject"; // this is private in HttpManagementUtil
    public static final String TEST_REMOTE_HOST = "testRemoteHost";

    private OAuth2InteractiveAuthenticator _authenticator;
    private HttpManagementConfiguration _mockConfiguration;
    private OAuth2AuthenticationProvider<?> _mockAuthProvider;
    private HttpPort _mockPort;

    @BeforeEach
    public void setUp() throws Exception
    {

        _mockPort = mock(HttpPort.class);
        _mockAuthProvider = createMockOAuth2AuthenticationProvider(_mockPort);
        _mockConfiguration = mock(HttpManagementConfiguration.class);
        when(_mockConfiguration.getAuthenticationProvider(any(HttpServletRequest.class))).thenReturn(_mockAuthProvider);
        when(_mockConfiguration.getPort(any(HttpServletRequest.class))).thenReturn(_mockPort);

        _authenticator = new OAuth2InteractiveAuthenticator();
    }

    @Test
    public void testInitialRedirect() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH,
                                                           Map.of("baz", "fnord"), sessionAttributes);
        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);

        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition =
                !(authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler);
        assertTrue(condition, "Authenticator has failed unexpectedly");

        HttpServletResponse mockResponse = mock(HttpServletResponse.class);
        authenticationHandler.handleAuthentication(mockResponse);

        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        verify(mockResponse).sendRedirect(argument.capture());
        Map<String, String> params = getRedirectParameters(argument.getValue());

        assertTrue(argument.getValue().startsWith(TEST_AUTHORIZATION_ENDPOINT), "Wrong redirect host");
        assertEquals("code", params.get("response_type"), "Wrong response_type");
        assertEquals(TEST_CLIENT_ID, params.get("client_id"), "Wrong client_id");
        assertEquals(TEST_REQUEST_HOST, params.get("redirect_uri"), "Wrong redirect_uri");
        assertEquals(TEST_OAUTH2_SCOPE, params.get("scope"), "Wrong scope");
        String stateAttrName = HttpManagementUtil.getRequestSpecificAttributeName(OAuth2InteractiveAuthenticator.STATE_NAME, mockRequest);
        assertNotNull(sessionAttributes.get(stateAttrName), "State was not set on the session");
        assertEquals(sessionAttributes.get(stateAttrName), params.get("state"), "Wrong state");
    }

    @Test
    public void testValidLogin() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        sessionAttributes.put(OAuth2InteractiveAuthenticator.STATE_NAME, TEST_STATE);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE, TEST_REQUEST);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.REDIRECT_URI_SESSION_ATTRIBUTE, TEST_REQUEST_HOST);
        Map<String, String> requestParameters = new HashMap<>();
        requestParameters.put("state", TEST_STATE);
        requestParameters.put("code", TEST_VALID_AUTHORIZATION_CODE);
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH, requestParameters, sessionAttributes);

        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);
        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition1 =
                !(authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler);
        assertTrue(condition1, "Authenticator has failed unexpectedly");

        HttpServletResponse mockResponse = mock(HttpServletResponse.class);
        authenticationHandler.handleAuthentication(mockResponse);

        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        verify(mockResponse).sendRedirect(argument.capture());

        assertEquals(TEST_REQUEST, argument.getValue(), "Wrong redirect");
        String attrSubject = HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SUBJECT, mockRequest);
        assertNotNull(sessionAttributes.get(attrSubject), "No subject on session");
        final boolean condition = sessionAttributes.get(attrSubject) instanceof Subject;
        assertTrue(condition, "Subject on session is no a Subject");
        final Set<Principal> principals = ((Subject) sessionAttributes.get(attrSubject)).getPrincipals();
        assertEquals(TEST_AUTHORIZED_USER, principals.iterator().next().getName(),
                "Subject created with unexpected principal");
    }

    @Test
    public void testNoStateOnSession() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        sessionAttributes.put(OAuth2InteractiveAuthenticator.ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE, TEST_REQUEST);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.REDIRECT_URI_SESSION_ATTRIBUTE, TEST_REQUEST_HOST);
        Map<String, String> requestParameters = new HashMap<>();
        requestParameters.put("state", TEST_STATE);
        requestParameters.put("code", TEST_VALID_AUTHORIZATION_CODE);
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH, requestParameters, sessionAttributes);

        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);
        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition =
                authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler;
        assertTrue(condition, "Authenticator did not fail with no state on session");
    }

    @Test
    public void testNoStateOnRequest() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        sessionAttributes.put(OAuth2InteractiveAuthenticator.STATE_NAME, TEST_STATE);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE, TEST_REQUEST);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.REDIRECT_URI_SESSION_ATTRIBUTE, TEST_REQUEST_HOST);
        Map<String, String> requestParameters = new HashMap<>();
        requestParameters.put("code", TEST_VALID_AUTHORIZATION_CODE);
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH, requestParameters, sessionAttributes);

        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);
        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition =
                authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler;
        assertTrue(condition, "Authenticator did not fail with no state on request");
    }

    @Test
    public void testWrongStateOnRequest() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        sessionAttributes.put(OAuth2InteractiveAuthenticator.STATE_NAME, TEST_STATE);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE, TEST_REQUEST);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.REDIRECT_URI_SESSION_ATTRIBUTE, TEST_REQUEST_HOST);
        Map<String, String> requestParameters = new HashMap<>();
        requestParameters.put("state", "WRONG" + TEST_STATE);
        requestParameters.put("code", TEST_VALID_AUTHORIZATION_CODE);
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH, requestParameters, sessionAttributes);

        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);
        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition =
                authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler;
        assertTrue(condition, "Authenticator did not fail with wrong state on request");
    }

    @Test
    public void testInvalidAuthorizationCode() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        sessionAttributes.put(OAuth2InteractiveAuthenticator.STATE_NAME, TEST_STATE);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE, TEST_REQUEST);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.REDIRECT_URI_SESSION_ATTRIBUTE, TEST_REQUEST_HOST);
        Map<String, String> requestParameters = new HashMap<>();
        requestParameters.put("state", TEST_STATE);
        requestParameters.put("code", TEST_INVALID_AUTHORIZATION_CODE);
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH, requestParameters, sessionAttributes);

        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);

        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition =
                !(authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler);
        assertTrue(condition, "Authenticator has failed unexpectedly");

        HttpServletResponse mockResponse = mock(HttpServletResponse.class);
        authenticationHandler.handleAuthentication(mockResponse);
        verify(mockResponse).sendError(eq(401));

    }

    @Test
    public void testUnauthorizedAuthorizationCode() throws Exception
    {
        Map<String, Object> sessionAttributes = new HashMap<>();
        sessionAttributes.put(OAuth2InteractiveAuthenticator.STATE_NAME, TEST_STATE);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE, TEST_REQUEST);
        sessionAttributes.put(OAuth2InteractiveAuthenticator.REDIRECT_URI_SESSION_ATTRIBUTE, TEST_REQUEST_HOST);
        Map<String, String> requestParameters = new HashMap<>();
        requestParameters.put("state", TEST_STATE);
        requestParameters.put("code", TEST_UNAUTHORIZED_AUTHORIZATION_CODE);
        HttpServletRequest mockRequest = createMockRequest(TEST_REQUEST_HOST, TEST_REQUEST_PATH, requestParameters, sessionAttributes);

        HttpRequestInteractiveAuthenticator.AuthenticationHandler authenticationHandler = _authenticator.getAuthenticationHandler(mockRequest,
                                                                                                                                  _mockConfiguration);
        assertNotNull(authenticationHandler, "Authenticator does not feel responsible");
        final boolean condition =
                !(authenticationHandler instanceof OAuth2InteractiveAuthenticator.FailedAuthenticationHandler);
        assertTrue(condition, "Authenticator has failed unexpectedly");

        HttpServletResponse mockResponse = mock(HttpServletResponse.class);
        authenticationHandler.handleAuthentication(mockResponse);
        verify(mockResponse).sendError(eq(403), any(String.class));
    }

    private Map<String, String> getRedirectParameters(final String redirectLocation)
    {
        final Fields parameterMap = new Fields();
        UrlEncoded.decodeUtf8To(HttpURI.from(redirectLocation).getQuery(), parameterMap);
        Map<String,String> parameters = new HashMap<>(parameterMap.getSize());
        for (Map.Entry<String, List<String>> paramEntry : parameterMap.toMultiMap().entrySet())
        {
            assertEquals(1, (long) paramEntry.getValue().size(),
                    String.format("param '%s' specified more than once", paramEntry.getKey()));

            parameters.put(paramEntry.getKey(), paramEntry.getValue().get(0));
        }
        return parameters;
    }

    private OAuth2AuthenticationProvider<?> createMockOAuth2AuthenticationProvider(final HttpPort mockPort) throws URISyntaxException
    {
        OAuth2AuthenticationProvider authenticationProvider = mock(OAuth2AuthenticationProvider.class);
        Broker mockBroker = mock(Broker.class);
        SubjectCreator mockSubjectCreator = mock(SubjectCreator.class);
        when(_mockPort.getSubjectCreator(anyBoolean(), anyString())).thenReturn(mockSubjectCreator);
        SubjectAuthenticationResult mockSuccessfulSubjectAuthenticationResult = mock(SubjectAuthenticationResult.class);
        SubjectAuthenticationResult mockUnauthorizedSubjectAuthenticationResult = mock(SubjectAuthenticationResult.class);
        final Subject successfulSubject = new Subject(true,
                                                      Set.of(new AuthenticatedPrincipal(new UsernamePrincipal(
                                                              TEST_AUTHORIZED_USER,
                                                              null))),
                                                      Set.of(),
                                                      Set.of());
        final Subject unauthorizedSubject = new Subject(true,
                                                        Set.of(new AuthenticatedPrincipal(new UsernamePrincipal(
                                                                TEST_UNAUTHORIZED_USER,
                                                                null))),
                                                        Set.of(),
                                                        Set.of());
        AuthenticationResult mockSuccessfulAuthenticationResult = mock(AuthenticationResult.class);
        AuthenticationResult mockUnauthorizedAuthenticationResult = mock(AuthenticationResult.class);
        AuthenticationResult failedAuthenticationResult = new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                                                                   new Exception("authentication failed"));
        SubjectAuthenticationResult failedSubjectAuthenticationResult = new SubjectAuthenticationResult(failedAuthenticationResult);

        doAnswer(invocationOnMock ->
        {
            final Subject subject = Subject.getSubject(AccessController.getContext());
            if (!subject.getPrincipals().iterator().next().getName().equals(TEST_AUTHORIZED_USER))
            {
                throw new AccessControlException("access denied");
            }
            return null;
        }).when(mockBroker).authorise(eq(Operation.PERFORM_ACTION("manage")));

        when(authenticationProvider.getAuthorizationEndpointURI(any())).thenReturn(new URI(TEST_AUTHORIZATION_ENDPOINT));
        when(authenticationProvider.getClientId()).thenReturn(TEST_CLIENT_ID);
        when(authenticationProvider.getScope()).thenReturn(TEST_OAUTH2_SCOPE);
        when(authenticationProvider.getParent()).thenReturn(mockBroker);
        when(authenticationProvider.authenticateViaAuthorizationCode(matches(TEST_VALID_AUTHORIZATION_CODE), matches(TEST_REQUEST_HOST), any())).thenReturn(mockSuccessfulAuthenticationResult);
        when(authenticationProvider.authenticateViaAuthorizationCode(matches(TEST_INVALID_AUTHORIZATION_CODE), matches(TEST_REQUEST_HOST), any())).thenReturn(failedAuthenticationResult);
        when(authenticationProvider.authenticateViaAuthorizationCode(matches(TEST_UNAUTHORIZED_AUTHORIZATION_CODE), matches(TEST_REQUEST_HOST), any())).thenReturn(mockUnauthorizedAuthenticationResult);

        when(mockSuccessfulSubjectAuthenticationResult.getSubject()).thenReturn(successfulSubject);
        when(mockUnauthorizedSubjectAuthenticationResult.getSubject()).thenReturn(unauthorizedSubject);

        when(mockSubjectCreator.createResultWithGroups(mockSuccessfulAuthenticationResult)).thenReturn(mockSuccessfulSubjectAuthenticationResult);
        when(mockSubjectCreator.createResultWithGroups(mockUnauthorizedAuthenticationResult)).thenReturn(mockUnauthorizedSubjectAuthenticationResult);
        when(mockSubjectCreator.createResultWithGroups(failedAuthenticationResult)).thenReturn(failedSubjectAuthenticationResult);

        return authenticationProvider;
    }

    private HttpServletRequest createMockRequest(String host, String path,
                                                 final Map<String, String> query,
                                                 Map<String, Object> sessionAttributes)
    {
        final HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        UUID portId = UUID.randomUUID();
        HttpPort port = mock(HttpPort.class);
        when(mockRequest.getAttribute(eq("org.apache.qpid.server.model.Port"))).thenReturn(port);
        when(port.getId()).thenReturn(portId);
        when(mockRequest.getParameterNames()).thenReturn(Collections.enumeration(query.keySet()));
        doAnswer(invocationOnMock ->
        {
            final Object[] arguments = invocationOnMock.getArguments();
            assertEquals(1, (long) arguments.length, "Unexpected number of arguments");
            final String paramName = (String) arguments[0];
            return new String[]{query.get(paramName)};
        }).when(mockRequest).getParameterValues(any(String.class));
        when(mockRequest.isSecure()).thenReturn(false);
        Map<String,Object> originalAttrs = new HashMap<>(sessionAttributes);
        sessionAttributes.clear();
        for(Map.Entry<String,Object> entry : originalAttrs.entrySet())
        {
            sessionAttributes.put(HttpManagementUtil.getRequestSpecificAttributeName(entry.getKey(), mockRequest), entry.getValue());
        }
        final HttpSession mockHttpSession = createMockHttpSession(sessionAttributes);
        when(mockRequest.getSession()).thenReturn(mockHttpSession);
        when(mockRequest.getServletPath()).thenReturn("");
        when(mockRequest.getPathInfo()).thenReturn(path);
        final StringBuffer url = new StringBuffer(host + path);
        when(mockRequest.getRequestURL()).thenReturn(url);
        when(mockRequest.getRemoteHost()).thenReturn(TEST_REMOTE_HOST);
        when(mockRequest.getRemotePort()).thenReturn(TEST_REMOTE_PORT);
        when(mockRequest.getServerName()).thenReturn(TEST_REMOTE_HOST);


        return mockRequest;
    }

    private HttpSession createMockHttpSession(final Map<String, Object> sessionAttributes)
    {
        final HttpSession httpSession = mock(HttpSession.class);
        doAnswer(invocation ->
        {
            final Object[] arguments = invocation.getArguments();
            assertEquals(2, (long) arguments.length);
            sessionAttributes.put((String) arguments[0], arguments[1]);
            return null;
        }).when(httpSession).setAttribute(any(String.class), any(Object.class));
        doAnswer(invocation ->
        {
            final Object[] arguments = invocation.getArguments();
            assertEquals(1, (long) arguments.length);
            return sessionAttributes.get((String) arguments[0]);
        }).when(httpSession).getAttribute(any(String.class));
        ServletContext mockServletContext = mock(ServletContext.class);
        when(httpSession.getServletContext()).thenReturn(mockServletContext);
        return httpSession;
    }
}
