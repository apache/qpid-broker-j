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

package org.apache.qpid.server.security.auth.manager.oauth2.google;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.IdentityResolverException;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2IdentityResolverService;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2Utils;

/**
 * An identity resolver that calls Google's userinfo endpoint https://www.googleapis.com/oauth2/v3/userinfo.
 *
 * It requires that the authentication request includes the scope 'profile' in order that 'sub'
 * (the user identifier) appears in userinfo's response.
 *
 * For endpoint is documented:
 *
 * https://developers.google.com/identity/protocols/OpenIDConnect
 */
public class GoogleOAuth2IdentityResolverService implements OAuth2IdentityResolverService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleOAuth2IdentityResolverService.class);
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    private final OAuth2AuthenticationProvider _authenticationProvider;
    private final URI _userInfoEndpoint;
    private final TrustStore _trustStore;
    private final ObjectMapper _objectMapper = new ObjectMapper();

    public GoogleOAuth2IdentityResolverService(final OAuth2AuthenticationProvider authenticationProvider)
    {
        _authenticationProvider = authenticationProvider;
        _userInfoEndpoint = _authenticationProvider.getIdentityResolverEndpointURI();
        _trustStore = _authenticationProvider.getTrustStore();

        if (!Sets.newHashSet(_authenticationProvider.getScope().split("\\s")).contains("profile"))
        {
            throw new IllegalArgumentException("This identity resolver requires that scope 'profile' is included in"
                                               + " the authentication request.");
        }
    }

    @Override
    public Principal getUserPrincipal(String accessToken) throws IOException, IdentityResolverException
    {
        LOGGER.debug("About to call identity service '{}'", _userInfoEndpoint);

        HttpsURLConnection connection = (HttpsURLConnection) _userInfoEndpoint.toURL().openConnection();
        if (_trustStore != null)
        {
            OAuth2Utils.setTrustedCertificates(connection, _trustStore);
        }

        connection.setRequestProperty("Accept-Charset", UTF8);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + UTF8);
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Authorization", "Bearer " + accessToken);

        connection.connect();

        try (InputStream input = connection.getInputStream())
        {
            int responseCode = connection.getResponseCode();
            LOGGER.debug("Call to identity service '{}' complete, response code : {}",
                         _userInfoEndpoint, responseCode);

            Map<String, String> responseMap;
            try
            {
                responseMap = _objectMapper.readValue(input, Map.class);
            }
            catch (JsonProcessingException e)
            {
                throw new IOException(String.format("Identity resolver '%s' did not return json",
                                                    _userInfoEndpoint), e);
            }
            if (responseCode != 200)
            {
                throw new IdentityResolverException(String.format(
                        "Identity resolver '%s' failed, response code %d",
                        _userInfoEndpoint, responseCode));
            }

            final String googleId = responseMap.get("sub");
            if (googleId == null)
            {
                throw new IdentityResolverException(String.format(
                        "Identity resolver '%s' failed, response did not include 'sub'",
                        _userInfoEndpoint));
            }
            return new UsernamePrincipal(googleId);
        }
    }
}
