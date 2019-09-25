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

import java.security.Principal;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.TokenCarryingPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.KerberosAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.SpnegoAuthenticator;

@PluggableService
public class SpnegoInteractiveAuthenticator implements HttpRequestInteractiveAuthenticator
{

    @Override
    public AuthenticationHandler getAuthenticationHandler(final HttpServletRequest request,
                                                          final HttpManagementConfiguration configuration)
    {
        final AuthenticationProvider authenticationProvider = configuration.getAuthenticationProvider(request);
        if (authenticationProvider instanceof KerberosAuthenticationManager)
        {
            final KerberosAuthenticationManager kerberosProvider =
                    (KerberosAuthenticationManager) authenticationProvider;
            return response -> {
                final String authorizationHeader = request.getHeader(SpnegoAuthenticator.REQUEST_AUTH_HEADER_NAME);
                final AuthenticationResult authenticationResult = kerberosProvider.authenticate(authorizationHeader);
                if (authenticationResult == null
                    || authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.ERROR)
                {
                    response.setHeader(SpnegoAuthenticator.RESPONSE_AUTH_HEADER_NAME,
                                       SpnegoAuthenticator.RESPONSE_AUTH_HEADER_VALUE_NEGOTIATE);
                    response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
                }
                else
                {
                    final Principal principal = authenticationResult.getMainPrincipal();
                    if (principal instanceof TokenCarryingPrincipal)
                    {
                        ((TokenCarryingPrincipal) principal).getTokens().forEach(response::setHeader);
                    }

                    final Port<?> port = configuration.getPort(request);
                    final SubjectCreator subjectCreator = port.getSubjectCreator(request.isSecure(), request.getServerName());
                    final SubjectAuthenticationResult result = subjectCreator.createResultWithGroups(authenticationResult);
                    final Subject subject = HttpManagementUtil.createServletConnectionSubject(request, result.getSubject());

                    final Broker broker = (Broker) kerberosProvider.getParent();
                    HttpManagementUtil.assertManagementAccess(broker, subject);
                    HttpManagementUtil.saveAuthorisedSubject(request, subject);
                    request.getRequestDispatcher(HttpManagement.DEFAULT_LOGIN_URL).forward(request, response);
                }
            };
        }
        return null;
    }


    @Override
    public LogoutHandler getLogoutHandler(final HttpServletRequest request,
                                          final HttpManagementConfiguration configuration)
    {
        return response -> response.sendRedirect(HttpManagement.DEFAULT_LOGOUT_URL);
    }

    @Override
    public String getType()
    {
        return SpnegoAuthenticator.AUTH_TYPE;
    }
}
