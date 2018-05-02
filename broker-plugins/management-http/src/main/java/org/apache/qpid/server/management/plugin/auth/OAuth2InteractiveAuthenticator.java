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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2Utils;

@PluggableService
public class OAuth2InteractiveAuthenticator implements HttpRequestInteractiveAuthenticator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2InteractiveAuthenticator.class);
    private static final String TYPE = "OAuth2";
    private static final int STATE_NONCE_BIT_SIZE = 256;
    static final String STATE_NAME = "stateNonce";
    static final String REDIRECT_URI_SESSION_ATTRIBUTE = "redirectURI";
    static final String ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE = "originalRequestURI";

    /** Authentication Endpoint error responses https://tools.ietf.org/html/rfc6749#section-4.2.2.1 */
    private static final Map<String, Integer> ERROR_RESPONSES;

    static
    {
        // Authentication Endpoint
        Map<String, Integer> errorResponses = new HashMap<>();
        errorResponses.put("invalid_request", 400);
        errorResponses.put("unauthorized_client", 400);
        errorResponses.put("unsupported_response_type", 400);
        errorResponses.put("invalid_scope", 400);
        errorResponses.put("access_denied", 403);
        errorResponses.put("server_error", 500);
        errorResponses.put("temporarily_unavailable", 503);
        ERROR_RESPONSES = Collections.unmodifiableMap(errorResponses);
    }

    private SecureRandom _random = new SecureRandom();

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public AuthenticationHandler getAuthenticationHandler(final HttpServletRequest request,
                                                          final HttpManagementConfiguration configuration)
    {
        final Port<?> port = configuration.getPort(request);

        if (configuration.getAuthenticationProvider(request) instanceof OAuth2AuthenticationProvider)
        {
            final OAuth2AuthenticationProvider oauth2Provider =
                    (OAuth2AuthenticationProvider) configuration.getAuthenticationProvider(request);
            final Map<String, String> requestParameters;
            try
            {
                requestParameters = getRequestParameters(request);
            }
            catch (IllegalArgumentException e)
            {
                return new FailedAuthenticationHandler(400, "Some request parameters are included more than once " + request, e);
            }

            String error = requestParameters.get("error");
            if (error != null)
            {
                int responseCode = decodeErrorAsResponseCode(error);
                String errorDescription = requestParameters.get("error_description");
                if (responseCode == 403)
                {
                    LOGGER.debug("Resource owner denies the access request");
                    return new FailedAuthenticationHandler(responseCode, "Resource owner denies the access request");

                }
                else
                {
                    LOGGER.warn("Authorization endpoint failed, error : '{}', error description '{}'",
                                error, errorDescription);
                    return new FailedAuthenticationHandler(responseCode, String.format("Authorization request failed :'%s'", error));
                }
            }

            final String authorizationCode = requestParameters.get("code");
            if (authorizationCode == null)
            {
                final String authorizationRedirectURL = buildAuthorizationRedirectURL(request, oauth2Provider);
                return response ->
                {
                    final NamedAddressSpace addressSpace = configuration.getPort(request).getAddressSpace(request.getServerName());

                    LOGGER.debug("Sending redirect to authorization endpoint {}", oauth2Provider.getAuthorizationEndpointURI(addressSpace));
                    response.sendRedirect(authorizationRedirectURL);
                };
            }
            else
            {
                final HttpSession httpSession = request.getSession();
                String state = requestParameters.get("state");

                if (state == null)
                {
                    LOGGER.warn("Deny login attempt with wrong state: {}", state);
                    return new FailedAuthenticationHandler(400, "No state set on request with authorization code grant: "
                                                           + request);
                }
                if (!checkState(request, state))
                {
                    LOGGER.warn("Deny login attempt with wrong state: {}", state);
                    return new FailedAuthenticationHandler(401, "Received request with wrong state: " + state);
                }
                final String redirectUri = (String) httpSession.getAttribute(HttpManagementUtil.getRequestSpecificAttributeName(
                        REDIRECT_URI_SESSION_ATTRIBUTE,
                        request));
                final String originalRequestUri = (String) httpSession.getAttribute(HttpManagementUtil.getRequestSpecificAttributeName(
                        ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE,
                        request));

                final NamedAddressSpace addressSpace = configuration.getPort(request).getAddressSpace(request.getServerName());

                return new AuthenticationHandler()
                {
                    @Override
                    public void handleAuthentication(final HttpServletResponse response) throws IOException
                    {
                        AuthenticationResult authenticationResult = oauth2Provider.authenticateViaAuthorizationCode(authorizationCode, redirectUri, addressSpace);
                        try
                        {
                            Subject subject = createSubject(authenticationResult);
                            authoriseManagement(subject);
                            HttpManagementUtil.saveAuthorisedSubject(request, subject);

                            LOGGER.debug("Successful login. Redirect to original resource {}", originalRequestUri);
                            response.sendRedirect(originalRequestUri);
                        }
                        catch (SecurityException e)
                        {
                            if (e instanceof AccessControlException)
                            {
                                LOGGER.info("User '{}' is not authorised for management", authenticationResult.getMainPrincipal());
                                response.sendError(403, "User is not authorised for management");
                            }
                            else
                            {
                                LOGGER.info("Authentication failed", authenticationResult.getCause());
                                response.sendError(401);
                            }
                        }
                    }

                    private Subject createSubject(final AuthenticationResult authenticationResult)
                    {
                        SubjectCreator subjectCreator = port.getSubjectCreator(request.isSecure(), request.getServerName());
                        SubjectAuthenticationResult result = subjectCreator.createResultWithGroups(authenticationResult);
                        Subject original = result.getSubject();

                        if (original == null)
                        {
                            throw new SecurityException("Only authenticated users can access the management interface");
                        }

                        Subject subject = HttpManagementUtil.createServletConnectionSubject(request, original);
                        return subject;
                    }

                    private void authoriseManagement(final Subject subject)
                    {
                        Broker broker = (Broker) oauth2Provider.getParent();
                        HttpManagementUtil.assertManagementAccess(broker, subject);
                    }
                };
            }
        }
        else
        {
            return null;
        }
    }

    @Override
    public LogoutHandler getLogoutHandler(final HttpServletRequest request,
                                          final HttpManagementConfiguration configuration)
    {
        if (configuration.getAuthenticationProvider(request) instanceof OAuth2AuthenticationProvider)
        {
            final OAuth2AuthenticationProvider oauth2Provider =
                    (OAuth2AuthenticationProvider) configuration.getAuthenticationProvider(request);

            if (oauth2Provider.getPostLogoutURI() != null)
            {
                final String postLogoutRedirect = oauth2Provider.getPostLogoutURI().toString();
                return new LogoutHandler()
                {
                    @Override
                    public void handleLogout(final HttpServletResponse response) throws IOException
                    {
                        response.sendRedirect(postLogoutRedirect);
                    }
                };
            }
        }
        return null;
    }

    private String buildAuthorizationRedirectURL(final HttpServletRequest request,
                                                 final OAuth2AuthenticationProvider oauth2Provider)
    {
        final String redirectUri = getRedirectUri(request);
        final String originalRequestUri = getOriginalRequestUri(request);

        NamedAddressSpace addressSpace = HttpManagementUtil.getPort(request).getAddressSpace(request.getServerName());

        final URI authorizationEndpointURI =
                oauth2Provider.getAuthorizationEndpointURI(addressSpace);

        final String authorizationEndpoint = authorizationEndpointURI.toString();
        final HttpSession httpSession = request.getSession();
        httpSession.setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(REDIRECT_URI_SESSION_ATTRIBUTE,
                                                                                    request), redirectUri);
        httpSession.setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(
                ORIGINAL_REQUEST_URI_SESSION_ATTRIBUTE,
                request), originalRequestUri);

        Map<String, String> queryArgs = new HashMap<>();
        queryArgs.put("client_id", oauth2Provider.getClientId());
        queryArgs.put("redirect_uri", redirectUri);
        queryArgs.put("response_type", "code");
        queryArgs.put("state", createState(request));
        if (oauth2Provider.getScope() != null)
        {
            queryArgs.put("scope", oauth2Provider.getScope());
        }

        StringBuilder urlBuilder = new StringBuilder(authorizationEndpoint);
        String query = authorizationEndpointURI.getQuery();
        if (query == null)
        {
            urlBuilder.append("?");
        }
        else if (query.length() > 0)
        {
            urlBuilder.append("&");
        }
        urlBuilder.append(OAuth2Utils.buildRequestQuery(queryArgs));

        return urlBuilder.toString();
    }

    private String getOriginalRequestUri(final HttpServletRequest request)
    {
        StringBuffer originalRequestURL = request.getRequestURL();
        final String queryString = request.getQueryString();
        if (queryString != null)
        {
            originalRequestURL.append("?").append(queryString);
        }
        return originalRequestURL.toString();
    }

    private Map<String, String> getRequestParameters(final HttpServletRequest request)
    {
        Map<String, String> requestParameters = new HashMap<>();
        Enumeration<String> parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements())
        {
            String parameterName = parameterNames.nextElement();
            String[] parameters = request.getParameterValues(parameterName);
            if (parameters == null)
            {
                throw new IllegalArgumentException(String.format("Request parameter '%s' is null", parameterName));
            }
            if (parameters.length != 1)
            {
                // having a parameter more than once violates the OAuth2 spec: http://tools.ietf.org/html/rfc6749#section-3.1
                throw new IllegalArgumentException(String.format("Request parameter '%s' MUST NOT occur more than once",
                                                                 parameterName));
            }
            requestParameters.put(parameterName, parameters[0]);
        }
        return requestParameters;
    }

    private String getRedirectUri(final HttpServletRequest request)
    {
        String servletPath = request.getServletPath() != null ? request.getServletPath() : "";
        String pathInfo = request.getPathInfo() != null ? request.getPathInfo() : "";
        final String requestURL = request.getRequestURL().toString();
        try
        {
            URI redirectURI = new URI(requestURL);
            String redirectString = redirectURI.normalize().toString();
            if (!redirectString.endsWith(servletPath + pathInfo))
            {
                throw new IllegalStateException(String.format("RequestURL has unexpected format '%s'", redirectString));
            }
            redirectString = redirectString.substring(0, redirectString.length() - (servletPath.length() + pathInfo.length()));

            return redirectString;
        }
        catch (URISyntaxException e)
        {
            throw new IllegalStateException(String.format("RequestURL has unexpected format '%s'",
                                                          requestURL), e);
        }
    }

    private String createState(HttpServletRequest request)
    {
        byte[] nonceBytes = new byte[STATE_NONCE_BIT_SIZE / 8];
        _random.nextBytes(nonceBytes);

        String nonce = Base64.getUrlEncoder().encodeToString(nonceBytes);
        request.getSession().setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(STATE_NAME, request), nonce);
        return nonce;
    }

    private boolean checkState(HttpServletRequest request, String state)
    {
        HttpSession session = request.getSession();
        String nonce = (String) session.getAttribute(HttpManagementUtil.getRequestSpecificAttributeName(STATE_NAME,
                                                                                                        request));
        session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(STATE_NAME, request));
        return state != null && state.equals(nonce);
    }

    private int decodeErrorAsResponseCode(final String error)
    {
        return ERROR_RESPONSES.containsKey(error) ? ERROR_RESPONSES.get(error) : 500;
    }

    class FailedAuthenticationHandler implements AuthenticationHandler
    {
        private final int _errorCode;
        private final Throwable _throwable;
        private final String _message;

        FailedAuthenticationHandler(int errorCode, String message)
        {
            this(errorCode, message, null);
        }

        FailedAuthenticationHandler(int errorCode, String message, Throwable t)
        {
            _errorCode = errorCode;
            _message = message;
            _throwable = t;
        }

        @Override
        public void handleAuthentication(final HttpServletResponse response) throws IOException
        {
            if (_throwable != null)
            {
                response.sendError(_errorCode, _message + ": " + _throwable);
            }
            else
            {
                response.sendError(_errorCode, _message);
            }
        }
    }
}
