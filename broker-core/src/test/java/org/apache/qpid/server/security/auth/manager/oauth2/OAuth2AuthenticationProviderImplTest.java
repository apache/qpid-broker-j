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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.RegisterExtension;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.CachingAuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.cloudfoundry.CloudFoundryOAuth2IdentityResolverService;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.oauth2.OAuth2Negotiator;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.UnitTestBase;

public class OAuth2AuthenticationProviderImplTest extends UnitTestBase
{
    @RegisterExtension
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final String TEST_ENDPOINT_HOST = "localhost";
    private static final String TEST_AUTHORIZATION_ENDPOINT_PATH = "/testauth";
    private static final String TEST_TOKEN_ENDPOINT_PATH = "/testtoken";
    private static final String TEST_IDENTITY_RESOLVER_ENDPOINT_PATH = "/testidresolver";
    private static final String TEST_POST_LOGOUT_PATH = "/testpostlogout";
    static final String UTF8 = StandardCharsets.UTF_8.name();
    static final String TEST_CLIENT_ID = "testClientId";
    static final String TEST_CLIENT_SECRET = "testClientSecret";
    private static final String TEST_IDENTITY_RESOLVER_TYPE = CloudFoundryOAuth2IdentityResolverService.TYPE;
    private static final String TEST_AUTHORIZATION_ENDPOINT_URI_PATTERN = "https://%s:%d%s";
    private static final String TEST_TOKEN_ENDPOINT_URI_PATTERN = "https://%s:%d%s";
    private static final String TEST_AUTHORIZATION_ENDPOINT_NEEDS_AUTH = "true";
    private static final String TEST_IDENTITY_RESOLVER_ENDPOINT_URI_PATTERN = "https://%s:%d%s";
    private static final String TEST_POST_LOGOUT_URI_PATTERN = "https://%s:%d%s";
    private static final String TEST_SCOPE = "testScope";
    private static final String TEST_TRUST_STORE_NAME = null;
    private static final String TEST_VALID_AUTHORIZATION_CODE = "validAuthorizationCode";
    private static final String TEST_INVALID_AUTHORIZATION_CODE = "invalidAuthorizationCode";
    private static final String TEST_VALID_ACCESS_TOKEN = "validAccessToken";
    private static final String TEST_INVALID_ACCESS_TOKEN = "invalidAccessToken";
    private static final String TEST_USER_NAME = "testUser";
    private static final String TEST_REDIRECT_URI = "localhost:23523";

    private OAuth2AuthenticationProvider<?> _authProvider;
    private OAuth2MockEndpointHolder _server;

    @BeforeAll
    public void setUp() throws Exception
    {
        final Path keyStore = TLS_RESOURCE.createSelfSignedKeyStore("CN=localhost");
        _server = new OAuth2MockEndpointHolder(keyStore.toFile().getAbsolutePath(),
                                               TLS_RESOURCE.getSecret(),
                                               TLS_RESOURCE.getKeyStoreType());
        _server.start();

        final Broker<?> broker = BrokerTestHelper.createBrokerMock();
        final Map<String, Object> authProviderAttributes = new HashMap<>();
        authProviderAttributes.put(ConfiguredObject.NAME, "testOAuthProvider");
        authProviderAttributes.put("clientId", TEST_CLIENT_ID);
        authProviderAttributes.put("clientSecret", TEST_CLIENT_SECRET);
        authProviderAttributes.put("identityResolverType", TEST_IDENTITY_RESOLVER_TYPE);
        authProviderAttributes.put("authorizationEndpointURI",
                                   String.format(TEST_AUTHORIZATION_ENDPOINT_URI_PATTERN,
                                                 TEST_ENDPOINT_HOST,
                                                 _server.getPort(),
                                                 TEST_AUTHORIZATION_ENDPOINT_PATH));
        authProviderAttributes.put("tokenEndpointURI",
                                   String.format(TEST_TOKEN_ENDPOINT_URI_PATTERN,
                                                 TEST_ENDPOINT_HOST,
                                                 _server.getPort(),
                                                 TEST_TOKEN_ENDPOINT_PATH));
        authProviderAttributes.put("tokenEndpointNeedsAuth", TEST_AUTHORIZATION_ENDPOINT_NEEDS_AUTH);
        authProviderAttributes.put("identityResolverEndpointURI",
                                   String.format(TEST_IDENTITY_RESOLVER_ENDPOINT_URI_PATTERN,
                                                 TEST_ENDPOINT_HOST,
                                                 _server.getPort(),
                                                 TEST_IDENTITY_RESOLVER_ENDPOINT_PATH));
        authProviderAttributes.put("postLogoutURI",
                                   String.format(TEST_POST_LOGOUT_URI_PATTERN,
                                                 TEST_ENDPOINT_HOST,
                                                 _server.getPort(),
                                                 TEST_POST_LOGOUT_PATH));
        authProviderAttributes.put("scope", TEST_SCOPE);
        authProviderAttributes.put("trustStore", TEST_TRUST_STORE_NAME);

        setTestSystemProperty(CachingAuthenticationProvider.AUTHENTICATION_CACHE_MAX_SIZE, "0");
        _authProvider = new OAuth2AuthenticationProviderImpl(authProviderAttributes, broker);
        _authProvider.open();
        assertEquals(State.ACTIVE, _authProvider.getState(), "Could not successfully open authProvider");

        final TrustManager[] trustingTrustManager = new TrustManager[] {new TrustingTrustManager() };

        final SSLContext sc = SSLContext.getInstance("TLSv1.3");
        sc.init(null, trustingTrustManager, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier(new BlindHostnameVerifier());
    }

    @AfterAll
    public void tearDown() throws Exception
    {
        if (_server != null)
        {
            _server.stop();
        }
    }

    @Test
    public void testGetSecureOnlyMechanisms()
    {
        assertEquals(List.of(OAuth2Negotiator.MECHANISM), _authProvider.getSecureOnlyMechanisms(),
                "OAuth2 should be a secure only mechanism");
    }

    @Test
    public void testAuthenticateViaSasl() throws Exception
    {
        _server.setEndpoints(Map.of(TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, createMockIdentityResolverEndpoint()));
        final SaslNegotiator negotiator = _authProvider.createSaslNegotiator(OAuth2Negotiator.MECHANISM, null, null);
        final AuthenticationResult authenticationResult = negotiator.handleResponse(("auth=Bearer " + TEST_VALID_ACCESS_TOKEN + "\1\1").getBytes(UTF8));

        assertSuccess(authenticationResult);
    }

    @Test
    public void testFailAuthenticateViaSasl() throws Exception
    {
        final OAuth2MockEndpoint mockIdentityResolverEndpoint = createMockIdentityResolverEndpoint();
        mockIdentityResolverEndpoint.putExpectedParameter("token", TEST_INVALID_ACCESS_TOKEN);
        mockIdentityResolverEndpoint.setResponse(400, "{\"error\":\"invalid_token\"}");
        _server.setEndpoints(Map.of(TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, mockIdentityResolverEndpoint));

        final SaslNegotiator negotiator = _authProvider.createSaslNegotiator(OAuth2Negotiator.MECHANISM, null, null);
        final AuthenticationResult authenticationResult = negotiator.handleResponse(("auth=Bearer " + TEST_INVALID_ACCESS_TOKEN + "\1\1").getBytes(UTF8));
        assertFailure(authenticationResult, "invalid_token");
    }

    @Test
    public void testAuthenticateViaAuthorizationCode()
    {
        final Map<String, OAuth2MockEndpoint> mockEndpoints = Map.of(TEST_TOKEN_ENDPOINT_PATH, createMockTokenEndpoint(),
                TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, createMockIdentityResolverEndpoint());
        _server.setEndpoints(mockEndpoints);

        final NamedAddressSpace mockAddressSpace = mock(NamedAddressSpace.class);
        when(mockAddressSpace.getName()).thenReturn("mock");

        final AuthenticationResult authenticationResult =
                _authProvider.authenticateViaAuthorizationCode(TEST_VALID_AUTHORIZATION_CODE, TEST_REDIRECT_URI,
                                                               mockAddressSpace);
        assertSuccess(authenticationResult);
    }

    @Test
    public void testFailAuthenticateViaInvalidAuthorizationCode()
    {
        final OAuth2MockEndpoint mockTokenEndpoint = createMockTokenEndpoint();
        mockTokenEndpoint.putExpectedParameter("code", TEST_INVALID_AUTHORIZATION_CODE);
        mockTokenEndpoint.setResponse(400, "{\"error\":\"invalid_grant\",\"error_description\":\"authorization grant is not valid\"}");
        final Map<String, OAuth2MockEndpoint> mockEndpoints = Map.of(TEST_TOKEN_ENDPOINT_PATH, mockTokenEndpoint,
                TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, createMockIdentityResolverEndpoint());
        _server.setEndpoints(mockEndpoints);

        final NamedAddressSpace mockAddressSpace = mock(NamedAddressSpace.class);
        when(mockAddressSpace.getName()).thenReturn("mock");

        final AuthenticationResult authenticationResult =
                _authProvider.authenticateViaAuthorizationCode(TEST_INVALID_AUTHORIZATION_CODE, TEST_REDIRECT_URI,
                                                               mockAddressSpace);
        assertFailure(authenticationResult, "invalid_grant");
    }

    @Test
    public void testAuthenticateViaAccessToken()
    {
        _server.setEndpoints(Map.of(TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, createMockIdentityResolverEndpoint()));

        final AuthenticationResult authenticationResult = _authProvider.authenticateViaAccessToken(TEST_VALID_ACCESS_TOKEN,
                                                                                             null);
        assertSuccess(authenticationResult);
    }

    @Test
    public void testFailAuthenticateViaInvalidAccessToken()
    {
        final OAuth2MockEndpoint mockIdentityResolverEndpoint = createMockIdentityResolverEndpoint();
        mockIdentityResolverEndpoint.putExpectedParameter("token", TEST_INVALID_ACCESS_TOKEN);
        mockIdentityResolverEndpoint.setResponse(400, "{\"error\":\"invalid_token\"}");
        _server.setEndpoints(Map.of(TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, mockIdentityResolverEndpoint));

        final AuthenticationResult authenticationResult =
                _authProvider.authenticateViaAccessToken(TEST_INVALID_ACCESS_TOKEN, null);
        assertFailure(authenticationResult, "invalid_token");
    }

    private void assertSuccess(final AuthenticationResult authenticationResult)
    {
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authenticationResult.getStatus(),
                "Authentication was not successful: " + authenticationResult.getCause());
        assertEquals(TEST_USER_NAME, authenticationResult.getMainPrincipal().getName(),
                "AuthenticationResult has the wrong Principal");
    }

    private void assertFailure(final AuthenticationResult authenticationResult, final String failureCauseString)
    {
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, authenticationResult.getStatus(),
                "Authentication should not succeed");
        assertTrue(authenticationResult.getCause().toString().contains(failureCauseString),
                authenticationResult.getCause().toString());
        assertNull(authenticationResult.getMainPrincipal(), "AuthenticationResult has the wrong Principal");
    }

    private OAuth2MockEndpoint createMockTokenEndpoint()
    {
        final OAuth2MockEndpoint tokenEndpoint = new OAuth2MockEndpoint();
        tokenEndpoint.putExpectedParameter("grant_type", "authorization_code");
        tokenEndpoint.putExpectedParameter("response_type", "token");
        tokenEndpoint.putExpectedParameter("code", TEST_VALID_AUTHORIZATION_CODE);
        tokenEndpoint.putExpectedParameter("redirect_uri", TEST_REDIRECT_URI);
        tokenEndpoint.setExpectedMethod("POST");
        tokenEndpoint.setNeedsAuth(true);
        tokenEndpoint.setResponse(200, String.format("{\"access_token\":\"%s\"," + "\"token_type\":\"bearer\"," +
                "\"expires_in\":3600}", TEST_VALID_ACCESS_TOKEN));
        return tokenEndpoint;
    }

    private OAuth2MockEndpoint createMockIdentityResolverEndpoint()
    {
        final OAuth2MockEndpoint identityResolverEndpoint = new OAuth2MockEndpoint();
        identityResolverEndpoint.putExpectedParameter("token", TEST_VALID_ACCESS_TOKEN);
        identityResolverEndpoint.setExpectedMethod("POST");
        identityResolverEndpoint.setNeedsAuth(true);
        identityResolverEndpoint.setResponse(200, String.format("{\"user_name\":\"%s\"}", TEST_USER_NAME));
        return identityResolverEndpoint;
    }

    private static final class TrustingTrustManager implements X509TrustManager
    {
        @Override
        public void checkClientTrusted(final X509Certificate[] certs, final String authType)
        {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certs, final String authType)
        {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return null;
        }
    }

    private static final class BlindHostnameVerifier implements HostnameVerifier
    {
        @Override
        public boolean verify(final String arg0, final SSLSession arg1)
        {
            return true;
        }
    }
}
