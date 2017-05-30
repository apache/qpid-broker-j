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

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpRequestPreemptiveAuthenticator;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;

@PluggableService
public class AnonymousPreemptiveAuthenticator implements HttpRequestPreemptiveAuthenticator
{

    private static final String ANONYMOUS = "Anonymous";

    @Override
    public Subject attemptAuthentication(final HttpServletRequest request,
                                         final HttpManagementConfiguration managementConfig)
    {
        final Port<?> port = managementConfig.getPort(request);
        final AuthenticationProvider authenticationProvider = managementConfig.getAuthenticationProvider(request);
        SubjectCreator subjectCreator = port.getSubjectCreator(request.isSecure(), request.getServerName());
        if(authenticationProvider instanceof AnonymousAuthenticationManager)
        {
            return subjectCreator.createResultWithGroups(((AnonymousAuthenticationManager) authenticationProvider).getAnonymousAuthenticationResult()).getSubject();
        }

        return null;
    }

    @Override
    public String getType()
    {
        return ANONYMOUS;
    }
}
