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
 *
 */

package org.apache.qpid.server.security.auth.sasl.scram;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.util.Strings;
import org.apache.qpid.test.utils.UnitTestBase;

public class ScramNegotiatorTest extends UnitTestBase
{
    private static final String VALID_USER_NAME = "testUser";
    private static final String VALID_USER_PASSWORD = "testPassword";
    private static final String INVALID_USER_PASSWORD = VALID_USER_PASSWORD + "1";
    private static final String INVALID_USER_NAME = VALID_USER_NAME + "1";
    private static final String GS2_HEADER = "n,,";
    private static final Charset ASCII = StandardCharsets.US_ASCII;
    private static final int ITERATION_COUNT = 4096;

    private String _clientFirstMessageBare;
    private String _clientNonce;
    private byte[] _serverSignature;
    private PasswordSource _passwordSource;
    private AuthenticationProvider<?> _authenticationProvider;
    private Broker<?> _broker;

    @BeforeEach
    public void setUp() throws Exception
    {
        _clientNonce = randomUUID().toString();
        _passwordSource = mock(PasswordSource.class);
        when(_passwordSource.getPassword(eq(VALID_USER_NAME))).thenReturn(VALID_USER_PASSWORD.toCharArray());
        _authenticationProvider = mock(AuthenticationProvider.class);
        _broker = BrokerTestHelper.createBrokerMock();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _authenticationProvider.close();
    }

    @Test
    public void testHandleResponseForScramSha256ValidCredentialsAdapterSource() throws Exception
    {
        final ScramSaslServerSource scramSaslServerSource =
                new ScramSaslServerSourceAdapter(ITERATION_COUNT,
                                                 ScramSHA256AuthenticationManager.HMAC_NAME,
                                                 ScramSHA256AuthenticationManager.DIGEST_NAME,
                                                 _passwordSource);
        doSaslNegotiationTestValidCredentials(ScramSHA256AuthenticationManager.MECHANISM,
                                              _authenticationProvider,
                                              scramSaslServerSource);
    }

    @Test
    public void testHandleResponseForScramSha1ValidCredentialsAdapterSource() throws Exception
    {
        final ScramSaslServerSource scramSaslServerSource =
                new ScramSaslServerSourceAdapter(ITERATION_COUNT,
                                                 ScramSHA1AuthenticationManager.HMAC_NAME,
                                                 ScramSHA1AuthenticationManager.DIGEST_NAME,
                                                 _passwordSource);
        doSaslNegotiationTestValidCredentials(ScramSHA1AuthenticationManager.MECHANISM,
                                              _authenticationProvider,
                                              scramSaslServerSource);
    }

    @Test
    public void testHandleResponseForScramSha256InvalidPasswordAdapterSource() throws Exception
    {
        final ScramSaslServerSource scramSaslServerSource =
                new ScramSaslServerSourceAdapter(ITERATION_COUNT,
                                                 ScramSHA256AuthenticationManager.HMAC_NAME,
                                                 ScramSHA256AuthenticationManager.DIGEST_NAME,
                                                 _passwordSource);
        doSaslNegotiationTestInvalidCredentials(VALID_USER_NAME,
                                                INVALID_USER_PASSWORD,
                                                ScramSHA256AuthenticationManager.MECHANISM,
                                                _authenticationProvider,
                                                scramSaslServerSource);
    }

    @Test
    public void testHandleResponseForScramSha1InvalidPasswordAdapterSource() throws Exception
    {
        final ScramSaslServerSource scramSaslServerSource =
                new ScramSaslServerSourceAdapter(ITERATION_COUNT,
                                                 ScramSHA1AuthenticationManager.HMAC_NAME,
                                                 ScramSHA1AuthenticationManager.DIGEST_NAME,
                                                 _passwordSource);
        doSaslNegotiationTestInvalidCredentials(VALID_USER_NAME,
                                                INVALID_USER_PASSWORD,
                                                ScramSHA1AuthenticationManager.MECHANISM,
                                                _authenticationProvider,
                                                scramSaslServerSource);
    }

    @Test
    public void testHandleResponseForScramSha256InvalidUsernameAdapterSource() throws Exception
    {
        final ScramSaslServerSource scramSaslServerSource =
                new ScramSaslServerSourceAdapter(ITERATION_COUNT,
                                                 ScramSHA256AuthenticationManager.HMAC_NAME,
                                                 ScramSHA256AuthenticationManager.DIGEST_NAME,
                                                 _passwordSource);
        doSaslNegotiationTestInvalidCredentials(INVALID_USER_NAME,
                                                VALID_USER_PASSWORD,
                                                ScramSHA256AuthenticationManager.MECHANISM,
                                                _authenticationProvider,
                                                scramSaslServerSource);
    }

    @Test
    public void testHandleResponseForScramSha1InvalidUsernameAdapterSource() throws Exception
    {
        final ScramSaslServerSource scramSaslServerSource =
                new ScramSaslServerSourceAdapter(ITERATION_COUNT,
                                                 ScramSHA1AuthenticationManager.HMAC_NAME,
                                                 ScramSHA1AuthenticationManager.DIGEST_NAME,
                                                 _passwordSource);
        doSaslNegotiationTestInvalidCredentials(INVALID_USER_NAME,
                                                VALID_USER_PASSWORD,
                                                ScramSHA1AuthenticationManager.MECHANISM,
                                                _authenticationProvider,
                                                scramSaslServerSource);
    }

    @Test
    public void testHandleResponseForScramSha256ValidCredentialsAuthenticationProvider() throws Exception
    {
        _authenticationProvider = createTestAuthenticationManager(ScramSHA256AuthenticationManager.PROVIDER_TYPE);
        doSaslNegotiationTestValidCredentials(ScramSHA256AuthenticationManager.MECHANISM, _authenticationProvider,
                                              (ScramSaslServerSource) _authenticationProvider);
    }

    @Test
    public void testHandleResponseForScramSha1ValidCredentialsAuthenticationProvider() throws Exception
    {
        _authenticationProvider = createTestAuthenticationManager(ScramSHA1AuthenticationManager.PROVIDER_TYPE);
        doSaslNegotiationTestValidCredentials(ScramSHA1AuthenticationManager.MECHANISM, _authenticationProvider,
                                              (ScramSaslServerSource) _authenticationProvider);
    }

    @Test
    public void testHandleResponseForScramSha256InvalidPasswordAuthenticationProvider() throws Exception
    {
        _authenticationProvider = createTestAuthenticationManager(ScramSHA256AuthenticationManager.PROVIDER_TYPE);
        doSaslNegotiationTestInvalidCredentials(VALID_USER_NAME,
                                                INVALID_USER_PASSWORD,
                                                "SCRAM-SHA-256",
                                                _authenticationProvider,
                                                (ScramSaslServerSource) _authenticationProvider);
    }

    @Test
    public void testHandleResponseForScramSha1InvalidPasswordAuthenticationProvider() throws Exception
    {
        _authenticationProvider = createTestAuthenticationManager(ScramSHA1AuthenticationManager.PROVIDER_TYPE);
        doSaslNegotiationTestInvalidCredentials(VALID_USER_NAME,
                                                INVALID_USER_PASSWORD,
                                                "SCRAM-SHA-1",
                                                _authenticationProvider,
                                                (ScramSaslServerSource) _authenticationProvider);
    }

    @Test
    public void testHandleResponseForScramSha256InvalidUsernameAuthenticationProvider() throws Exception
    {
        _authenticationProvider = createTestAuthenticationManager(ScramSHA256AuthenticationManager.PROVIDER_TYPE);
        doSaslNegotiationTestInvalidCredentials(INVALID_USER_NAME,
                                                VALID_USER_PASSWORD,
                                                "SCRAM-SHA-256",
                                                _authenticationProvider,
                                                (ScramSaslServerSource) _authenticationProvider);
    }

    @Test
    public void testHandleResponseForScramSha1InvalidUsernameAuthenticationProvider() throws Exception
    {
        _authenticationProvider = createTestAuthenticationManager(ScramSHA1AuthenticationManager.PROVIDER_TYPE);
        doSaslNegotiationTestInvalidCredentials(INVALID_USER_NAME,
                                                VALID_USER_PASSWORD,
                                                "SCRAM-SHA-1",
                                                _authenticationProvider,
                                                (ScramSaslServerSource) _authenticationProvider);
    }

    private void doSaslNegotiationTestValidCredentials(final String mechanism,
                                                       final AuthenticationProvider<?> authenticationProvider,
                                                       final ScramSaslServerSource scramSaslServerSource)
            throws Exception
    {
        final ScramNegotiator scramNegotiator = new ScramNegotiator(authenticationProvider,
                                                              scramSaslServerSource,
                                                              mechanism);

        final byte[] initialResponse = createInitialResponse(VALID_USER_NAME);

        final AuthenticationResult firstResult = scramNegotiator.handleResponse(initialResponse);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, firstResult.getStatus(),
                "Unexpected first result status");

        assertNotNull(firstResult.getChallenge(), "Unexpected first result challenge");

        final byte[] response = calculateClientProof(firstResult.getChallenge(),
                                               scramSaslServerSource.getHmacName(),
                                               scramSaslServerSource.getDigestName(),
                                               VALID_USER_PASSWORD);
        final AuthenticationResult secondResult = scramNegotiator.handleResponse(response);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, secondResult.getStatus(),
                "Unexpected second result status");
        assertNotNull(secondResult.getChallenge(), "Unexpected second result challenge");
        assertEquals(VALID_USER_NAME, secondResult.getMainPrincipal().getName(), "Unexpected second result principal");

        final String serverFinalMessage = new String(secondResult.getChallenge(), ASCII);
        final String[] parts = serverFinalMessage.split(",");
        if (!parts[0].startsWith("v="))
        {
            fail("Server final message did not contain verifier");
        }
        final byte[] serverSignature = Strings.decodeBase64(parts[0].substring(2));
        if (!Arrays.equals(_serverSignature, serverSignature))
        {
            fail("Server signature did not match");
        }

        final AuthenticationResult thirdResult = scramNegotiator.handleResponse(initialResponse);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, thirdResult.getStatus(),
                "Unexpected result status after completion of negotiation");
        assertNull(thirdResult.getMainPrincipal(), "Unexpected principal after completion of negotiation");
    }

    private void doSaslNegotiationTestInvalidCredentials(final String userName,
                                                         final String userPassword,
                                                         final String mechanism,
                                                         final AuthenticationProvider<?> authenticationProvider,
                                                         final ScramSaslServerSource scramSaslServerSource)
            throws Exception
    {
        final ScramNegotiator scramNegotiator = new ScramNegotiator(authenticationProvider, scramSaslServerSource,
                mechanism);

        final byte[] initialResponse = createInitialResponse(userName);
        final AuthenticationResult firstResult = scramNegotiator.handleResponse(initialResponse);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, firstResult.getStatus(),
                "Unexpected first result status");
        assertNotNull(firstResult.getChallenge(), "Unexpected first result challenge");

        final byte[] response = calculateClientProof(firstResult.getChallenge(), scramSaslServerSource.getHmacName(),
                scramSaslServerSource.getDigestName(), userPassword);
        final AuthenticationResult secondResult = scramNegotiator.handleResponse(response);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
        assertNull(secondResult.getChallenge(), "Unexpected second result challenge");
        assertNull(secondResult.getMainPrincipal(), "Unexpected second result principal");
    }


    private byte[] calculateClientProof(final byte[] challenge,
                                        final String hmacName,
                                        final String digestName,
                                        final String userPassword) throws Exception
    {

        final String serverFirstMessage = new String(challenge, ASCII);
        final String[] parts = serverFirstMessage.split(",");
        if (parts.length < 3)
        {
            fail("Server challenge '" + serverFirstMessage + "' cannot be parsed");
        }
        else if (parts[0].startsWith("m="))
        {
            fail("Server requires mandatory extension which is not supported: " + parts[0]);
        }
        else if (!parts[0].startsWith("r="))
        {
            fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find nonce");
        }
        final String nonce = parts[0].substring(2);
        if (!nonce.startsWith(_clientNonce))
        {
            fail("Server challenge did not use correct client nonce");
        }
        if (!parts[1].startsWith("s="))
        {
            fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find salt");
        }
        final byte[] salt = Strings.decodeBase64(parts[1].substring(2));
        if (!parts[2].startsWith("i="))
        {
            fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find iteration count");
        }
        final int _iterationCount = Integer.parseInt(parts[2].substring(2));
        if (_iterationCount <= 0)
        {
            fail("Iteration count " + _iterationCount + " is not a positive integer");
        }
        final byte[] passwordBytes = saslPrep(userPassword).getBytes(StandardCharsets.UTF_8);
        final byte[] saltedPassword = generateSaltedPassword(passwordBytes, hmacName, _iterationCount, salt);

        final String clientFinalMessageWithoutProof =
                "c=" + Base64.getEncoder().encodeToString(GS2_HEADER.getBytes(ASCII)) + ",r=" + nonce;

        final String authMessage = _clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
        final byte[] clientKey = computeHmac(saltedPassword, "Client Key", hmacName);
        final byte[] storedKey = MessageDigest.getInstance(digestName).digest(clientKey);
        final byte[] clientSignature = computeHmac(storedKey, authMessage, hmacName);
        final byte[] clientProof = clientKey.clone();
        for (int i = 0; i < clientProof.length; i++)
        {
            clientProof[i] ^= clientSignature[i];
        }
        final byte[] serverKey = computeHmac(saltedPassword, "Server Key", hmacName);
        _serverSignature = computeHmac(serverKey, authMessage, hmacName);
        final String finalMessageWithProof =
                clientFinalMessageWithoutProof + ",p=" + Base64.getEncoder().encodeToString(clientProof);
        return finalMessageWithProof.getBytes();
    }

    private byte[] computeHmac(final byte[] key, final String string, String hmacName)
            throws Exception
    {
        final Mac mac = createHmac(key, hmacName);
        mac.update(string.getBytes(ASCII));
        return mac.doFinal();
    }

    private byte[] generateSaltedPassword(final byte[] passwordBytes,
                                          final String hmacName,
                                          final int iterationCount,
                                          final byte[] salt) throws Exception
    {
        final Mac mac = createHmac(passwordBytes, hmacName);
        mac.update(salt);
        mac.update(new byte[]{0, 0, 0, 1});
        final byte[] result = mac.doFinal();

        byte[] previous = null;
        for (int i = 1; i < iterationCount; i++)
        {
            mac.update(previous != null ? previous : result);
            previous = mac.doFinal();
            for (int x = 0; x < result.length; x++)
            {
                result[x] ^= previous[x];
            }
        }

        return result;
    }

    private Mac createHmac(final byte[] keyBytes, final String hmacName) throws Exception
    {
        final SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
        Mac mac = Mac.getInstance(hmacName);
        mac.init(key);
        return mac;
    }

    private String saslPrep(String name)
    {
        name = name.replace("=", "=3D");
        name = name.replace(",", "=2C");
        return name;
    }

    private byte[] createInitialResponse(final String userName)
    {
        _clientFirstMessageBare = "n=" + saslPrep(userName) + ",r=" + _clientNonce;
        return (GS2_HEADER + _clientFirstMessageBare).getBytes(ASCII);
    }

    private AuthenticationProvider<?> createTestAuthenticationManager(final String type)
    {
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, getTestName(),
                ConfiguredObject.ID, randomUUID(),
                ConfiguredObject.TYPE, type);
        final ConfiguredObjectFactory objectFactory = _broker.getObjectFactory();
        @SuppressWarnings("unchecked")
        final AuthenticationProvider<?> configuredObject =
                objectFactory.create(AuthenticationProvider.class, attributes, _broker);
        assertEquals(State.ACTIVE, configuredObject.getState(), "Unexpected state");

        final PasswordCredentialManagingAuthenticationProvider<?> authenticationProvider =
                (PasswordCredentialManagingAuthenticationProvider<?>) configuredObject;
        authenticationProvider.createUser(VALID_USER_NAME, VALID_USER_PASSWORD, Map.of());
        return configuredObject;
    }
}
