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

package org.apache.qpid.server.security.auth.manager.oauth2.github;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.IdentityResolverException;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2IdentityResolverService;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2Utils;
import org.apache.qpid.server.util.ConnectionBuilder;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * An identity resolver that calls GitHubs's user API https://developer.github.com/v3/users/
 *
 * It requires that the authentication request includes the scope 'user'
 *
 */
@PluggableService
public class GitHubOAuth2IdentityResolverService implements OAuth2IdentityResolverService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GitHubOAuth2IdentityResolverService.class);
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    public static final String TYPE = "GitHubUser";

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public void validate(final OAuth2AuthenticationProvider<?> authProvider) throws IllegalConfigurationException
    {
        if (!Sets.newHashSet(authProvider.getScope().split("\\s")).contains("user"))
        {
            throw new IllegalConfigurationException("This identity resolver requires that scope 'user' is included in"
                                               + " the authentication request.");
        }
    }

    @Override
    public Principal getUserPrincipal(final OAuth2AuthenticationProvider<?> authenticationProvider,
                                      String accessToken,
                                      final NamedAddressSpace addressSpace) throws IOException, IdentityResolverException
    {
        URL userInfoEndpoint = authenticationProvider.getIdentityResolverEndpointURI(addressSpace).toURL();
        TrustStore trustStore = authenticationProvider.getTrustStore();

        ConnectionBuilder connectionBuilder = new ConnectionBuilder(userInfoEndpoint);
        connectionBuilder.setConnectTimeout(authenticationProvider.getConnectTimeout())
                         .setReadTimeout(authenticationProvider.getReadTimeout());
        if (trustStore != null)
        {
            try
            {
                connectionBuilder.setTrustMangers(trustStore.getTrustManagers());
            }
            catch (GeneralSecurityException e)
            {
                throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
            }
        }
        connectionBuilder.setTlsProtocolWhiteList(authenticationProvider.getTlsProtocolWhiteList())
                         .setTlsProtocolBlackList(authenticationProvider.getTlsProtocolBlackList())
                         .setTlsCipherSuiteWhiteList(authenticationProvider.getTlsCipherSuiteWhiteList())
                         .setTlsCipherSuiteBlackList(authenticationProvider.getTlsCipherSuiteBlackList());

        LOGGER.debug("About to call identity service '{}'", userInfoEndpoint);
        HttpURLConnection connection = connectionBuilder.build();

        connection.setRequestProperty("Accept-Charset", UTF8);
        connection.setRequestProperty("Accept", "application/vnd.github.v3+json");
        connection.setRequestProperty("Authorization", "token " + accessToken);

        connection.connect();

        try (InputStream input = OAuth2Utils.getResponseStream(connection))
        {
            int responseCode = connection.getResponseCode();
            LOGGER.debug("Call to identity service '{}' complete, response code : {}",
                         userInfoEndpoint, responseCode);

            Map<String, String> responseMap;
            try
            {
                responseMap = _objectMapper.readValue(input, Map.class);
            }
            catch (JsonProcessingException e)
            {
                throw new IOException(String.format("Identity resolver '%s' did not return json",
                                                    userInfoEndpoint), e);
            }
            if (responseCode != 200)
            {
                throw new IdentityResolverException(String.format(
                        "Identity resolver '%s' failed, response code %d",
                        userInfoEndpoint, responseCode));
            }

            final String githubId = responseMap.get("login");
            if (githubId == null)
            {
                throw new IdentityResolverException(String.format(
                        "Identity resolver '%s' failed, response did not include 'login'",
                        userInfoEndpoint));
            }
            return new UsernamePrincipal(githubId, authenticationProvider);
        }
    }


    @Override
    public URI getDefaultAuthorizationEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI("https://github.com/login/oauth/authorize");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public URI getDefaultTokenEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI("https://github.com/login/oauth/access_token");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public URI getDefaultIdentityResolverEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI("https://api.github.com/user");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public String getDefaultScope(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        return "user";
    }
}
