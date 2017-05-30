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

package org.apache.qpid.server.security.auth.manager.oauth2.keycloak;

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
 * An identity resolver that calls Google's userinfo endpoint https://www.googleapis.com/oauth2/v3/userinfo.
 *
 * It requires that the authentication request includes the scope 'profile' in order that 'sub'
 * (the user identifier) appears in userinfo's response.
 *
 * For endpoint is documented:
 *
 * https://developers.google.com/identity/protocols/OpenIDConnect
 */
@PluggableService
public class KeycloakOAuth2IdentityResolverService implements OAuth2IdentityResolverService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeycloakOAuth2IdentityResolverService.class);
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    public static final String TYPE = "KeycloakOpenID";

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public void validate(final OAuth2AuthenticationProvider<?> authProvider) throws IllegalConfigurationException
    {
        if (!Sets.newHashSet(authProvider.getScope().split("\\s")).contains("openid"))
        {
            throw new IllegalConfigurationException("This identity resolver requires that scope 'openid' is included in"
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
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Authorization", "Bearer " + accessToken);

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

            String username = responseMap.get("preferred_username");
            if (username == null)
            {
                username = responseMap.get("sub");
                if (username == null)
                {

                    throw new IdentityResolverException(String.format(
                            "Identity resolver '%s' failed, response did not include 'sub'",
                            userInfoEndpoint));
                }
            }
            return new UsernamePrincipal(username, authenticationProvider);
        }
    }

    @Override
    public URI getDefaultAuthorizationEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI(getEndpointPrefix(oAuth2AuthenticationProvider) + "protocol/openid-connect/auth");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    private String getEndpointPrefix(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        String baseUrl = oAuth2AuthenticationProvider.getContextValue(String.class, "keycloak.baseUrl");
        String domain = oAuth2AuthenticationProvider.getContextValue(String.class, "keycloak.domain");
        return baseUrl + "/auth/realms/" + domain + "/";
    }

    @Override
    public URI getDefaultTokenEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI(getEndpointPrefix(oAuth2AuthenticationProvider) + "protocol/openid-connect/token");
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
            return new URI(getEndpointPrefix(oAuth2AuthenticationProvider) + "protocol/openid-connect/userinfo");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public String getDefaultScope(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        return "openid";
    }
}
