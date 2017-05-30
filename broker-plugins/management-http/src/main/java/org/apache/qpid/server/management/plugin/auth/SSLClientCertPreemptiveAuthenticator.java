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

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Collections;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpRequestPreemptiveAuthenticator;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

@PluggableService
public class SSLClientCertPreemptiveAuthenticator implements HttpRequestPreemptiveAuthenticator
{

    private static final String SSL_CLIENT_AUTH = "SSLClientAuth";
    private static final String CERTIFICATE_ATTRIBUTE_NAME = "javax.servlet.request.X509Certificate";

    @Override
    public Subject attemptAuthentication(final HttpServletRequest request,
                                         final HttpManagementConfiguration managementConfig)
    {
        final AuthenticationProvider authenticationProvider = managementConfig.getAuthenticationProvider(request);
        final Port<?> port = managementConfig.getPort(request);
        SubjectCreator subjectCreator = port.getSubjectCreator(request.isSecure(), request.getServerName());
        if(request.isSecure()
           && authenticationProvider instanceof ExternalAuthenticationManager
           && Collections.list(request.getAttributeNames()).contains(CERTIFICATE_ATTRIBUTE_NAME))
        {
            ExternalAuthenticationManager<?> externalAuthManager = (ExternalAuthenticationManager<?>)authenticationProvider;
            X509Certificate[] certificates = (X509Certificate[]) request.getAttribute(CERTIFICATE_ATTRIBUTE_NAME);
            if(certificates != null && certificates.length != 0)
            {
                Principal principal = certificates[0].getSubjectX500Principal();

                if(!externalAuthManager.getUseFullDN())
                {
                    String username;
                    String dn = ((X500Principal) principal).getName(X500Principal.RFC2253);


                    username = SSLUtil.getIdFromSubjectDN(dn);
                    principal = new UsernamePrincipal(username, authenticationProvider);
                }

                return subjectCreator.createSubjectWithGroups(new AuthenticatedPrincipal(principal));
            }
        }

        return null;
    }

    @Override
    public String getType()
    {
        return SSL_CLIENT_AUTH;
    }
}
