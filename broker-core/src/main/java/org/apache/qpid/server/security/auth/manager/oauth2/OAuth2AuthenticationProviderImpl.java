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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.AbstractAuthenticationManager;

public class OAuth2AuthenticationProviderImpl
        extends AbstractAuthenticationManager<OAuth2AuthenticationProviderImpl>
        implements OAuth2AuthenticationProvider<OAuth2AuthenticationProviderImpl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2AuthenticationProviderImpl.class);
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @ManagedAttributeField
    private URI _authorizationEndpointURI;

    @ManagedAttributeField
    private URI _tokenEndpointURI;

    @ManagedAttributeField
    private URI _identityResolverEndpointURI;

    @ManagedAttributeField
    private boolean _tokenEndpointNeedsAuth;

    @ManagedAttributeField
    private String _clientId;

    @ManagedAttributeField
    private String _clientSecret;

    @ManagedAttributeField
    private TrustStore _trustStore;

    @ManagedAttributeField
    private String _scope;

    @ManagedAttributeField
    private String _identityResolverFactoryType;

    private OAuth2IdentityResolverService _identityResolverService;

    @ManagedObjectFactoryConstructor
    protected OAuth2AuthenticationProviderImpl(final Map<String, Object> attributes,
                                               final Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        String type = getIdentityResolverFactoryType();
        OAuth2IdentityResolverServiceFactory factory = OAuth2IdentityResolverServiceFactory.FACTORIES.get(type);
        _identityResolverService = factory.createIdentityResolverService(this);
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.singletonList(OAuth2SaslServer.MECHANISM);
    }

    @Override
    public SaslServer createSaslServer(final String mechanism,
                                       final String localFQDN,
                                       final Principal externalPrincipal)
            throws SaslException
    {
        if(OAuth2SaslServer.MECHANISM.equals(mechanism))
        {
            return new OAuth2SaslServer();
        }
        else
        {
            throw new SaslException("Unknown mechanism: " + mechanism);
        }
    }

    @Override
    public AuthenticationResult authenticate(final SaslServer server, final byte[] response)
    {
        try
        {
            // Process response from the client
            byte[] challenge = server.evaluateResponse(response != null ? response : new byte[0]);

            if (server.isComplete())
            {
                String accessToken = (String) server.getNegotiatedProperty(OAuth2SaslServer.ACCESS_TOKEN_PROPERTY);
                return authenticateViaAccessToken(accessToken);
            }
            else
            {
                return new AuthenticationResult(challenge, AuthenticationResult.AuthenticationStatus.CONTINUE);
            }
        }
        catch (SaslException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public AuthenticationResult authenticateViaAuthorizationCode(final String authorizationCode, final String redirectUri)
    {
        URL tokenEndpoint;
        HttpsURLConnection connection;
        byte[] body;
        try
        {
            tokenEndpoint = getTokenEndpointURI().toURL();

            LOGGER.debug("About to call token endpoint '{}'", tokenEndpoint);

            connection = (HttpsURLConnection) tokenEndpoint.openConnection();

            if (getTrustStore() != null)
            {
                OAuth2Utils.setTrustedCertificates(connection, getTrustStore());
            }

            connection.setDoOutput(true); // makes sure to use POST
            connection.setRequestProperty("Accept-Charset", UTF8);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + UTF8);
            connection.setRequestProperty("Accept", "application/json");

            if (getTokenEndpointNeedsAuth())
            {
                String encoded = DatatypeConverter.printBase64Binary((getClientId() + ":" + getClientSecret()).getBytes());
                connection.setRequestProperty("Authorization", "Basic " + encoded);
            }

            Map<String, String> requestBody = new HashMap<>();
            requestBody.put("code", authorizationCode);
            requestBody.put("client_id", getClientId());
            requestBody.put("client_secret", getClientSecret());
            requestBody.put("redirect_uri", redirectUri);
            requestBody.put("grant_type", "authorization_code");
            requestBody.put("response_type", "token");
            body = OAuth2Utils.buildRequestQuery(requestBody).getBytes(UTF8);
            connection.connect();
        }
        catch (IOException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }

        try (OutputStream output = connection.getOutputStream())
        {
            output.write(body);
            output.close();

            try (InputStream input = connection.getInputStream())
            {
                final int responseCode = connection.getResponseCode();
                LOGGER.debug("Call to token endpoint '{}' complete, response code : {}", tokenEndpoint, responseCode);

                Map<String, Object> responseMap = _objectMapper.readValue(input, Map.class);
                if (responseCode != 200)
                {
                    IllegalStateException e = new IllegalStateException(String.format("Token endpoint failed, response code %d, error '%s', description '%s'",
                                                                                      responseCode,
                                                                                      responseMap.get("error"),
                                                                                      responseMap.get("error_description")));
                    return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
                }
                return getAuthenticationResult(responseMap);
            }
            catch (JsonProcessingException e)
            {
                IllegalStateException ise = new IllegalStateException(String.format("Token endpoint '%s' did not return json",
                                                                                    tokenEndpoint),
                                                                      e);
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, ise);
            }
        }
        catch (IOException | IdentityResolverException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public AuthenticationResult authenticateViaAccessToken(String accessToken)
    {
        try
        {
            return new AuthenticationResult(new AuthenticatedPrincipal(_identityResolverService.getUserPrincipal(accessToken)));
        }
        catch (IOException | IdentityResolverException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public URI getAuthorizationEndpointURI()
    {
        return _authorizationEndpointURI;
    }

    @Override
    public URI getTokenEndpointURI()
    {
        return _tokenEndpointURI;
    }

    @Override
    public URI getIdentityResolverEndpointURI()
    {
        return _identityResolverEndpointURI;
    }

    @Override
    public boolean getTokenEndpointNeedsAuth()
    {
        return _tokenEndpointNeedsAuth;
    }

    @Override
    public String getIdentityResolverFactoryType()
    {
        return _identityResolverFactoryType;
    }

    @Override
    public String getClientId()
    {
        return _clientId;
    }

    @Override
    public String getClientSecret()
    {
        return _clientSecret;
    }

    @Override
    public TrustStore getTrustStore()
    {
        return _trustStore;
    }

    @Override
    public String getScope()
    {
        return _scope;
    }

    private AuthenticationResult getAuthenticationResult(Map<String, Object> tokenEndpointResponse)
            throws IOException, IdentityResolverException
    {
        final Object accessTokenObject = tokenEndpointResponse.get("access_token");
        if (accessTokenObject == null)
        {
            final IllegalStateException e = new IllegalStateException("Token endpoint response did not include 'access_token'");
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
        String accessToken = String.valueOf(accessTokenObject);

        return new AuthenticationResult(new AuthenticatedPrincipal(_identityResolverService.getUserPrincipal(accessToken)));
    }

}
