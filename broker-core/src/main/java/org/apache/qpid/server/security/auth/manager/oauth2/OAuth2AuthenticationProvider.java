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

package org.apache.qpid.server.security.auth.manager.oauth2;

import java.net.URI;
import java.util.List;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.CachingAuthenticationProvider;

@ManagedObject( category = false, type = "OAuth2" )
public interface OAuth2AuthenticationProvider<T extends OAuth2AuthenticationProvider<T>>
        extends CachingAuthenticationProvider<T>
{
    String AUTHENTICATION_OAUTH2_CONNECT_TIMEOUT = "qpid.authentication.oauth2.connectTimeout";
    @ManagedContextDefault(name = AUTHENTICATION_OAUTH2_CONNECT_TIMEOUT)
    int DEFAULT_AUTHENTICATION_OAUTH2_CONNECT_TIMEOUT = 60000;

    String AUTHENTICATION_OAUTH2_READ_TIMEOUT = "qpid.authentication.oauth2.readTimeout";
    @ManagedContextDefault(name = AUTHENTICATION_OAUTH2_READ_TIMEOUT)
    int DEFAULT_AUTHENTICATION_OAUTH2_READ_TIMEOUT = 60000;

    @ManagedAttribute( description = "Redirect URI to obtain authorization code grant", mandatory = true, defaultValue = "${this:defaultAuthorizationEndpointURI}")
    URI getAuthorizationEndpointURI();

    URI getAuthorizationEndpointURI(NamedAddressSpace addressSpace);

    @ManagedAttribute( description = "Token endpoint URI to exchange an authorization code grant for an access token", mandatory = true, defaultValue = "${this:defaultTokenEndpointURI}" )
    URI getTokenEndpointURI();

    URI getTokenEndpointURI(NamedAddressSpace addressSpace);

    @ManagedAttribute( description = "Whether to use basic authentication when accessing the token endpoint", defaultValue = "false" )
    boolean getTokenEndpointNeedsAuth();

    @ManagedAttribute( description = "Identity resolver endpoint URI to get user information associated with a given access token", mandatory = true, defaultValue = "${this:defaultIdentityResolverEndpointURI}"  )
    URI getIdentityResolverEndpointURI();

    URI getIdentityResolverEndpointURI(NamedAddressSpace addressSpace);


    @ManagedAttribute( description = "The type of the IdentityResolver", mandatory = true,
            validValues = {"org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProviderImpl#validIdentityResolvers()"})
    String getIdentityResolverType();

    @ManagedAttribute( description = "Redirect URI used when the user leaves the Web Management Console. If not specified, an internal page is used instead.")
    URI getPostLogoutURI();

    @ManagedAttribute( description = "Client ID to identify qpid to the OAuth endpoints", mandatory = true )
    String getClientId();

    @ManagedAttribute( description = "Client secret to identify qpid to the OAuth endpoints", secure = true )
    String getClientSecret();

    @ManagedAttribute( description = "The OAuth2 access token scope passed to the authorization endpoint", defaultValue = "${this:defaultScope}")
    String getScope();

    @ManagedAttribute( description = "TrustStore to use when contacting OAuth2 endpoints" )
    TrustStore getTrustStore();

    @Override
    @ManagedAttribute( defaultValue = "[ \"XOAUTH2\" ]")
    List<String> getSecureOnlyMechanisms();

    AuthenticationResult authenticateViaAuthorizationCode(String authorizationCode, final String redirectUri, NamedAddressSpace addressSpace);

    AuthenticationResult authenticateViaAccessToken(String accessToken,
                                                    final NamedAddressSpace addressSpace);

    @DerivedAttribute( description = "Default redirect URI to obtain authorization code grant")
    URI getDefaultAuthorizationEndpointURI();

    @DerivedAttribute( description = "Default token endpoint URI")
    URI getDefaultTokenEndpointURI();

    @DerivedAttribute( description = "Default identity resolver endpoint URI")
    URI getDefaultIdentityResolverEndpointURI();

    @DerivedAttribute( description = "Default OAuth access token scope passed to the authorization endpoint")
    String getDefaultScope();

    @DerivedAttribute
    List<String> getTlsProtocolWhiteList();

    @DerivedAttribute
    List<String> getTlsProtocolBlackList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteWhiteList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteBlackList();

    int getConnectTimeout();

    int getReadTimeout();
}
