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
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;

@PluggableService
public class SSLClientCertInteractiveAuthenticator implements HttpRequestInteractiveAuthenticator
{
    private static final LogoutHandler LOGOUT_HANDLER =
            response -> response.sendRedirect(HttpManagement.DEFAULT_LOGOUT_URL);

    private SSLClientCertPreemptiveAuthenticator _preemptiveAuthenticator = new SSLClientCertPreemptiveAuthenticator();

    @Override
    public AuthenticationHandler getAuthenticationHandler(final HttpServletRequest request,
                                                          final HttpManagementConfiguration configuration)
    {
        final AuthenticationProvider authenticationProvider = configuration.getAuthenticationProvider(request);
        if (authenticationProvider instanceof ExternalAuthenticationManager)
        {
            return response -> {
                final Subject subject = _preemptiveAuthenticator.attemptAuthentication(request, configuration);
                if (subject != null)
                {
                    final Subject servletSubject = HttpManagementUtil.createServletConnectionSubject(request, subject);
                    HttpManagementUtil.assertManagementAccess((Broker) authenticationProvider.getParent(), servletSubject);
                    HttpManagementUtil.saveAuthorisedSubject(request, servletSubject);
                    response.sendRedirect("/");
                }
                else
                {
                    response.sendError(401);
                }
            };
        }
        return null;
    }

    @Override
    public LogoutHandler getLogoutHandler(final HttpServletRequest request,
                                          final HttpManagementConfiguration configuration)
    {
        return LOGOUT_HANDLER;
    }

    @Override
    public String getType()
    {
        return "SSLClientAuth";
    }
}
