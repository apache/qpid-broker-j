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

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpRequestInteractiveAuthenticator;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider;

@PluggableService
public class UsernamePasswordInteractiveLogin implements HttpRequestInteractiveAuthenticator
{
    private static String DEFAULT_LOGIN_URL = "login.html";

    private static final AuthenticationHandler REDIRECT_HANDLER = new AuthenticationHandler()
    {
        @Override
        public void handleAuthentication(final HttpServletResponse response) throws IOException
        {
            response.sendRedirect(DEFAULT_LOGIN_URL);
        }
    };

    @Override
    public AuthenticationHandler getAuthenticationHandler(final HttpServletRequest request,
                                                          final HttpManagementConfiguration configuration)
    {
        if(configuration.getAuthenticationProvider(request) instanceof UsernamePasswordAuthenticationProvider)
        {
            return REDIRECT_HANDLER;
        }
        else
        {
            return null;
        }
    }

    @Override
    public String getType()
    {
        return "UsernamePassword";
    }
}
