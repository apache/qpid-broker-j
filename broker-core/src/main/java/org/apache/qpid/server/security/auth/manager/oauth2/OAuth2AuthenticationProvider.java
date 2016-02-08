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

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.AuthenticationResult;

@ManagedObject( category = false, type = "OAuth2" )
public interface OAuth2AuthenticationProvider<T extends OAuth2AuthenticationProvider<T>> extends AuthenticationProvider<T>
{
    @ManagedAttribute( description = "Redirect URI to obtain authorization code grant", mandatory = true )
    URI getAuthorizationEndpointURI();

    @ManagedAttribute( description = "Token endpoint URI", mandatory = true )
    URI getTokenEndpointURI();

    @ManagedAttribute( description = "Whether to use basic authentication when accessing the token endpoint", defaultValue = "false" )
    boolean getTokenEndpointNeedsAuth();

    @ManagedAttribute( description = "Identity resolver endpoint URI", mandatory = true )
    URI getIdentityResolverEndpointURI();

    @ManagedAttribute( description = "The type of the IdentityResolverFactory", mandatory = true )
    String getIdentityResolverFactoryType();

    @ManagedAttribute( description = "Client ID to identify qpid to the OAuth endpoints", mandatory = true )
    String getClientId();

    @ManagedAttribute( description = "Client secret to identify qpid to the OAuth endpoints", mandatory = true, secure = true )
    String getClientSecret();

    @ManagedAttribute( description = "The OAuth access token scope passed to the authorization endpoint" )
    String getScope();

    @ManagedAttribute( description = "TrustStore to use when contacting OAuth endpoints" )
    TrustStore getTrustStore();

    @ManagedAttribute( defaultValue = "[ \"XOAUTH2\" ]")
    List<String> getSecureOnlyMechanisms();

    AuthenticationResult authenticateViaAuthorizationCode(String authorizationCode, final String redirectUri);

    AuthenticationResult authenticateViaAccessToken(String accessToken);
}
