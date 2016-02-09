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
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.management.plugin.servlet.ServletConnectionPrincipal;
import org.apache.qpid.server.model.Broker;
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
    private static final int STATE_NONCE_BIT_SIZE = 256;
    private static final String STATE_NAME = "stateNonce";
    private static final String TYPE = "OAuth2";

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

            final String authorizationCode = requestParameters.get("code");
            if (authorizationCode == null)
            {
                final String authorizationRedirectURL = buildAuthorizationRedirectURL(request, oauth2Provider);
                return new AuthenticationHandler()
                {
                    @Override
                    public void handleAuthentication(final HttpServletResponse response) throws IOException
                    {
                        LOGGER.debug("Sending redirect to authorization endpoint {}", oauth2Provider.getAuthorizationEndpointURI());
                        response.sendRedirect(authorizationRedirectURL);
                    }
                };
            }
            else
            {
                final HttpSession httpSession = request.getSession();
                String state = requestParameters.get("state");

                if (state == null)
                {
                    LOGGER.warn("Deny login attempt with wrong state: {}", state);
                    return new FailedAuthenticationHandler(400, "no state set on request with authorization code grant: "
                                                           + request);
                }
                if (!checkState(httpSession, state))
                {
                    LOGGER.warn("Deny login attempt with wrong state: {}", state);
                    return new FailedAuthenticationHandler(401, "Received request with wrong state: " + state);
                }
                final String redirectUri = (String) httpSession.getAttribute("redirectUri");
                return new AuthenticationHandler()
                {
                    @Override
                    public void handleAuthentication(final HttpServletResponse response) throws IOException
                    {
                        AuthenticationResult authenticationResult = oauth2Provider.authenticateViaAuthorizationCode(authorizationCode, redirectUri);
                        createSubject(authenticationResult);

                        LOGGER.debug("Successful login. Redirect to original resource {}", redirectUri);
                        response.sendRedirect(redirectUri);
                    }

                    private void createSubject(final AuthenticationResult authenticationResult)
                    {
                        SubjectCreator subjectCreator = oauth2Provider.getSubjectCreator(request.isSecure());
                        SubjectAuthenticationResult result = subjectCreator.createResultWithGroups(authenticationResult);

                        Subject subject = result.getSubject();

                        if (subject == null)
                        {
                            throw new SecurityException("Only authenticated users can access the management interface");
                        }

                        Subject original = subject;
                        subject = new Subject(false,
                                              original.getPrincipals(),
                                              original.getPublicCredentials(),
                                              original.getPrivateCredentials());
                        subject.getPrincipals().add(new ServletConnectionPrincipal(request));
                        subject.setReadOnly();

                        Broker broker = (Broker) oauth2Provider.getParent(Broker.class);
                        HttpManagementUtil.assertManagementAccess(broker.getSecurityManager(), subject);

                        HttpManagementUtil.saveAuthorisedSubject(httpSession, subject);
                    }
                };
            }
        }
        else
        {
            return null;
        }
    }

    private String buildAuthorizationRedirectURL(final HttpServletRequest request,
                                                 final OAuth2AuthenticationProvider oauth2Provider)
    {
        final String redirectUri = getRedirectUri(request);
        final String authorizationEndpoint = oauth2Provider.getAuthorizationEndpointURI().toString();
        final HttpSession httpSession = request.getSession();
        httpSession.setAttribute("redirectUri", redirectUri);

        Map<String, String> queryArgs = new HashMap<>();
        queryArgs.put("client_id", oauth2Provider.getClientId());
        queryArgs.put("redirect_uri", redirectUri);
        queryArgs.put("response_type", "code");
        queryArgs.put("state", createState(httpSession));
        if (oauth2Provider.getScope() != null)
        {
            queryArgs.put("scope", oauth2Provider.getScope());
        }

        // TODO: currently we assume, but don't check, that the authorizationEndpointURI does not contain a query string
        StringBuilder urlBuilder = new StringBuilder(authorizationEndpoint);
        urlBuilder.append("?");
        urlBuilder.append(OAuth2Utils.buildRequestQuery(queryArgs));

        return urlBuilder.toString();
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
        StringBuffer redirectUri = request.getRequestURL();
        final String queryString = request.getQueryString();
        if (queryString != null)
        {
            redirectUri.append(queryString);
        }
        return redirectUri.toString();
    }

    private String createState(HttpSession session)
    {
        byte[] nonceBytes = new byte[STATE_NONCE_BIT_SIZE / 8];
        _random.nextBytes(nonceBytes);

        String nonce = DatatypeConverter.printBase64Binary(nonceBytes);
        session.setAttribute(STATE_NAME, nonce);
        return nonce;
    }

    private boolean checkState(HttpSession session, String state)
    {
        String nonce = (String) session.getAttribute(STATE_NAME);
        session.removeAttribute(STATE_NAME);
        return state != null && state.equals(nonce);
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
