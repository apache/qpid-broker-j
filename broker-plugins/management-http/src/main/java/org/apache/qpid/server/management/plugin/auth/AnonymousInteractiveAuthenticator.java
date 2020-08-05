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

package org.apache.qpid.server.management.plugin.auth;

import java.io.IOException;
import java.security.AccessControlException;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;

@PluggableService
public class AnonymousInteractiveAuthenticator implements HttpRequestInteractiveAuthenticator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AnonymousInteractiveAuthenticator.class);
    private static final String ANONYMOUS = "Anonymous";

    private static final LogoutHandler LOGOUT_HANDLER =
            response -> response.sendRedirect(HttpManagement.DEFAULT_LOGOUT_URL);


    @Override
    public AuthenticationHandler getAuthenticationHandler(final HttpServletRequest request,
                                                          final HttpManagementConfiguration configuration)
    {
        final Port<?> port = configuration.getPort(request);
        if (configuration.getAuthenticationProvider(request) instanceof AnonymousAuthenticationManager)
        {
            return response -> getLoginHandler(request, response, configuration, port);
        }
        else
        {
            return null;
        }
    }

    private void getLoginHandler(HttpServletRequest request, HttpServletResponse response,
                                 HttpManagementConfiguration configuration, Port<?> port) throws ServletException, IOException
    {
        final AnonymousAuthenticationManager authenticationProvider =
                (AnonymousAuthenticationManager) configuration.getAuthenticationProvider(request);
        final AuthenticationResult authenticationResult = authenticationProvider.getAnonymousAuthenticationResult();
        try
        {
            final SubjectAuthenticationResult result = port.getSubjectCreator(request.isSecure(), request.getServerName()).createResultWithGroups(authenticationResult);
            final Subject original = result.getSubject();

            if (original == null)
            {
                throw new SecurityException("Only authenticated users can access the management interface");
            }
            final Subject subject = HttpManagementUtil.createServletConnectionSubject(request, original);
            final Broker broker = (Broker) authenticationProvider.getParent();
            HttpManagementUtil.assertManagementAccess(broker, subject);
            HttpManagementUtil.saveAuthorisedSubject(request, subject);
            request.getRequestDispatcher(HttpManagement.DEFAULT_LOGIN_URL).forward(request, response);
        }
        catch (AccessControlException e)
        {
            LOGGER.info("User '{}' is not authorised for management", authenticationResult.getMainPrincipal());
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "User is not authorised for management");
        }
        catch (SecurityException e)
        {
            LOGGER.info("Authentication failed", authenticationResult.getCause());
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @Override
    public LogoutHandler getLogoutHandler(final HttpServletRequest request,
                                          final HttpManagementConfiguration configuration)
    {
        if(configuration.getAuthenticationProvider(request) instanceof AnonymousAuthenticationManager)
        {
            return LOGOUT_HANDLER;
        }
        else
        {
            return null;
        }
    }

    @Override
    public String getType()
    {
        return ANONYMOUS;
    }
}
