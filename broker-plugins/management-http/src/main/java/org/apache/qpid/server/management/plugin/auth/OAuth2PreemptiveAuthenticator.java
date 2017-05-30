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

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpRequestPreemptiveAuthenticator;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;

@PluggableService
public class OAuth2PreemptiveAuthenticator implements HttpRequestPreemptiveAuthenticator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2PreemptiveAuthenticator.class);
    private static final String TYPE = "OAuth2";
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public Subject attemptAuthentication(final HttpServletRequest request,
                                         final HttpManagementConfiguration configuration)
    {
        final Port<?> port = configuration.getPort(request);
        final AuthenticationProvider<?> authenticationProvider = configuration.getAuthenticationProvider(request);
        String authorizationHeader = request.getHeader("Authorization");
        String accessToken = null;

        if (authorizationHeader != null && authorizationHeader.startsWith(BEARER_PREFIX))
        {
            accessToken = authorizationHeader.substring(BEARER_PREFIX.length());
        }

        if (accessToken != null && authenticationProvider instanceof OAuth2AuthenticationProvider)
        {
            OAuth2AuthenticationProvider<?> oAuth2AuthProvider = (OAuth2AuthenticationProvider<?>) authenticationProvider;
            AuthenticationResult authenticationResult = oAuth2AuthProvider.authenticateViaAccessToken(accessToken, null);

            SubjectCreator subjectCreator = port.getSubjectCreator(request.isSecure(), request.getServerName());
            SubjectAuthenticationResult result = subjectCreator.createResultWithGroups(authenticationResult);

            return result.getSubject();
        }
        return null;
    }

    @Override
    public String getType()
    {
        return TYPE;
    }
}
