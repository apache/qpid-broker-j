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
package org.apache.qpid.server.security.auth.manager.oauth2.cloudfoundry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.IdentityResolverException;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2IdentityResolverService;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2Utils;

@PluggableService
public class CloudFoundryOAuth2IdentityResolverService implements OAuth2IdentityResolverService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudFoundryOAuth2IdentityResolverService.class);
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    public static final String TYPE = "CloudFoundryIdentityResolver";

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public void validate(final OAuth2AuthenticationProvider<?> authProvider) throws IllegalConfigurationException
    {
    }

    @Override
    public Principal getUserPrincipal(final OAuth2AuthenticationProvider<?> authenticationProvider,
                                      final String accessToken) throws IOException, IdentityResolverException
    {
        URI checkTokenEndpointURI = authenticationProvider.getIdentityResolverEndpointURI();
        TrustStore trustStore = authenticationProvider.getTrustStore();
        String clientId = authenticationProvider.getClientId();
        String clientSecret = authenticationProvider.getClientSecret();
        URL checkTokenEndpoint = checkTokenEndpointURI.toURL();
        HttpsURLConnection connection;

        LOGGER.debug("About to call identity service '{}'", checkTokenEndpoint);

        connection = (HttpsURLConnection) checkTokenEndpoint.openConnection();
        if (trustStore != null)
        {
            OAuth2Utils.setTrustedCertificates(connection, trustStore);
        }

        connection.setDoOutput(true); // makes sure to use POST
        connection.setRequestProperty("Accept-Charset", UTF8);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + UTF8);
        connection.setRequestProperty("Accept", "application/json");
        String encoded = DatatypeConverter.printBase64Binary((clientId + ":" + clientSecret).getBytes());
        connection.setRequestProperty("Authorization", "Basic " + encoded);

        final Map<String,String> requestParameters = Collections.singletonMap("token", accessToken);

        connection.connect();

        try (OutputStream output = connection.getOutputStream())
        {
            output.write(OAuth2Utils.buildRequestQuery(requestParameters).getBytes(UTF8));
            output.close();
            try (InputStream input = connection.getInputStream())
            {
                int responseCode = connection.getResponseCode();
                LOGGER.debug("Call to identity service '{}' complete, response code : {}", checkTokenEndpoint, responseCode);

                Map<String, String> responseMap = null;
                try
                {
                    responseMap = _objectMapper.readValue(input, Map.class);
                }
                catch (JsonProcessingException e)
                {
                    throw new IOException(String.format("Identity resolver '%s' did not return json", checkTokenEndpoint), e);
                }
                if (responseCode != 200)
                {
                    throw new IdentityResolverException(String.format("Identity resolver '%s' failed, response code %d, error '%s', description '%s'",
                                                                      checkTokenEndpoint,
                                                                      responseCode,
                                                                      responseMap.get("error"),
                                                                      responseMap.get("error_description")));
                }
                final String userName = responseMap.get("user_name");
                if (userName == null)
                {
                    throw new IdentityResolverException(String.format("Identity resolver '%s' failed, response did not include 'user_name'",
                                                                      checkTokenEndpoint));
                }
                return new UsernamePrincipal(userName);
            }
        }
    }
}
