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

import java.nio.charset.StandardCharsets;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpRequestPreemptiveAuthenticator;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider;
import org.apache.qpid.server.util.Strings;

@PluggableService
public class BasicAuthPreemptiveAuthenticator implements HttpRequestPreemptiveAuthenticator
{

    private static final String BASIC_AUTH = "BasicAuth";

    @Override
    public Subject attemptAuthentication(final HttpServletRequest request, final HttpManagementConfiguration managementConfiguration)
    {
        final String header = request.getHeader("Authorization");
        final Port<?> port = managementConfiguration.getPort(request);
        final AuthenticationProvider<?> authenticationProvider = managementConfiguration.getAuthenticationProvider(request);
        final SubjectCreator subjectCreator = port.getSubjectCreator(request.isSecure(), request.getServerName());

        if (header != null && authenticationProvider instanceof UsernamePasswordAuthenticationProvider)
        {
            final UsernamePasswordAuthenticationProvider<?> namePasswdAuthProvider = (UsernamePasswordAuthenticationProvider<?>)authenticationProvider;

            final String[] tokens = header.split("\\s");
            if (tokens.length >= 2 && "BASIC".equalsIgnoreCase(tokens[0]))
            {
                boolean isBasicAuthSupported = false;
                if (request.isSecure())
                {
                    isBasicAuthSupported = managementConfiguration.isHttpsBasicAuthenticationEnabled();
                }
                else
                {
                    isBasicAuthSupported = managementConfiguration.isHttpBasicAuthenticationEnabled();
                }
                if (isBasicAuthSupported)
                {
                    final String base64UsernameAndPassword = tokens[1];
                    final String[] credentials = new String(Strings.decodePrivateBase64(base64UsernameAndPassword,
                            "basic authentication credentials"), StandardCharsets.UTF_8).split(":", 2);
                    if (credentials.length == 2)
                    {
                        final String username = credentials[0];
                        final String password = credentials[1];
                        final AuthenticationResult authenticationResult = namePasswdAuthProvider.authenticate(username, password);
                        final SubjectAuthenticationResult result = subjectCreator.createResultWithGroups(authenticationResult);

                        return result.getSubject();
                    }
                }
            }
        }
        return null;
    }

    @Override
    public String getType()
    {
        return BASIC_AUTH;
    }
}
