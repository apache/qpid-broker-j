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

package org.apache.qpid.server.security.auth.manager.oauth2;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.Pluggable;

public interface OAuth2IdentityResolverService extends Pluggable
{
    void validate(final OAuth2AuthenticationProvider<?> authProvider) throws IllegalConfigurationException;

    Principal getUserPrincipal(final OAuth2AuthenticationProvider<?> authProvider,
                               String accessToken, final NamedAddressSpace addressSpace) throws IOException, IdentityResolverException;

    URI getDefaultAuthorizationEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider);

    URI getDefaultTokenEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider);

    URI getDefaultIdentityResolverEndpointURI(OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider);

    String getDefaultScope(OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider);
}
