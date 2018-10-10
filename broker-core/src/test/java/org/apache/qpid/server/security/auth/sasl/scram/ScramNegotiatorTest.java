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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    private static final Charset ASCII = Charset.forName("ASCII");
    private static final int ITERATION_COUNT = 4096;

    private String _clientFirstMessageBare;
    private String _clientNonce;
    private byte[] _serverSignature;
    private PasswordSource _passwordSource;
    private AuthenticationProvider<?> _authenticationProvider;
    private Broker<?> _broker;

    @Before
    public void setUp() throws Exception
    {
        _clientNonce = UUID.randomUUID().toString();
        _passwordSource = mock(PasswordSource.class);
        when(_passwordSource.getPassword(eq(VALID_USER_NAME))).thenReturn(VALID_USER_PASSWORD.toCharArray());
        _authenticationProvider = mock(AuthenticationProvider.class);
        _broker = BrokerTestHelper.createBrokerMock();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
        }
        finally
        {
            _authenticationProvider.close();
        }
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
        ScramNegotiator scramNegotiator = new ScramNegotiator(authenticationProvider,
                                                              scramSaslServerSource,
                                                              mechanism);

        byte[] initialResponse = createInitialResponse(VALID_USER_NAME);

        AuthenticationResult firstResult = scramNegotiator.handleResponse(initialResponse);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            firstResult.getStatus());

        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        byte[] response = calculateClientProof(firstResult.getChallenge(),
                                               scramSaslServerSource.getHmacName(),
                                               scramSaslServerSource.getDigestName(),
                                               VALID_USER_PASSWORD);
        AuthenticationResult secondResult = scramNegotiator.handleResponse(response);
        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            secondResult.getStatus());
        assertNotNull("Unexpected second result challenge", secondResult.getChallenge());
        assertEquals("Unexpected second result principal",
                            VALID_USER_NAME,
                            secondResult.getMainPrincipal().getName());

        String serverFinalMessage = new String(secondResult.getChallenge(), ASCII);
        String[] parts = serverFinalMessage.split(",");
        if (!parts[0].startsWith("v="))
        {
            fail("Server final message did not contain verifier");
        }
        byte[] serverSignature = Strings.decodeBase64(parts[0].substring(2));
        if (!Arrays.equals(_serverSignature, serverSignature))
        {
            fail("Server signature did not match");
        }

        AuthenticationResult thirdResult = scramNegotiator.handleResponse(initialResponse);
        assertEquals("Unexpected result status after completion of negotiation",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            thirdResult.getStatus());
        assertNull("Unexpected principal after completion of negotiation", thirdResult.getMainPrincipal());
    }

    private void doSaslNegotiationTestInvalidCredentials(final String userName,
                                                         final String userPassword,
                                                         final String mechanism,
                                                         final AuthenticationProvider<?> authenticationProvider,
                                                         final ScramSaslServerSource scramSaslServerSource)
            throws Exception
    {
        ScramNegotiator scramNegotiator = new ScramNegotiator(authenticationProvider,
                                                              scramSaslServerSource,
                                                              mechanism);

        byte[] initialResponse = createInitialResponse(userName);
        AuthenticationResult firstResult = scramNegotiator.handleResponse(initialResponse);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            firstResult.getStatus());
        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        byte[] response = calculateClientProof(firstResult.getChallenge(),
                                               scramSaslServerSource.getHmacName(),
                                               scramSaslServerSource.getDigestName(),
                                               userPassword);
        AuthenticationResult secondResult = scramNegotiator.handleResponse(response);
        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
        assertNull("Unexpected second result challenge", secondResult.getChallenge());
        assertNull("Unexpected second result principal", secondResult.getMainPrincipal());
    }


    private byte[] calculateClientProof(final byte[] challenge,
                                        String hmacName,
                                        String digestName,
                                        String userPassword) throws Exception
    {

        String serverFirstMessage = new String(challenge, ASCII);
        String[] parts = serverFirstMessage.split(",");
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
        String nonce = parts[0].substring(2);
        if (!nonce.startsWith(_clientNonce))
        {
            fail("Server challenge did not use correct client nonce");
        }
        if (!parts[1].startsWith("s="))
        {
            fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find salt");
        }
        byte[] salt = Strings.decodeBase64(parts[1].substring(2));
        if (!parts[2].startsWith("i="))
        {
            fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find iteration count");
        }
        int _iterationCount = Integer.parseInt(parts[2].substring(2));
        if (_iterationCount <= 0)
        {
            fail("Iteration count " + _iterationCount + " is not a positive integer");
        }
        byte[] passwordBytes = saslPrep(userPassword).getBytes("UTF-8");
        byte[] saltedPassword = generateSaltedPassword(passwordBytes, hmacName, _iterationCount, salt);

        String clientFinalMessageWithoutProof =
                "c=" + Base64.getEncoder().encodeToString(GS2_HEADER.getBytes(ASCII))
                + ",r=" + nonce;

        String authMessage = _clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
        byte[] clientKey = computeHmac(saltedPassword, "Client Key", hmacName);
        byte[] storedKey = MessageDigest.getInstance(digestName).digest(clientKey);
        byte[] clientSignature = computeHmac(storedKey, authMessage, hmacName);
        byte[] clientProof = clientKey.clone();
        for (int i = 0; i < clientProof.length; i++)
        {
            clientProof[i] ^= clientSignature[i];
        }
        byte[] serverKey = computeHmac(saltedPassword, "Server Key", hmacName);
        _serverSignature = computeHmac(serverKey, authMessage, hmacName);
        String finalMessageWithProof = clientFinalMessageWithoutProof
                                       + ",p=" + Base64.getEncoder().encodeToString(clientProof);
        return finalMessageWithProof.getBytes();
    }

    private byte[] computeHmac(final byte[] key, final String string, String hmacName)
            throws Exception
    {
        Mac mac = createHmac(key, hmacName);
        mac.update(string.getBytes(ASCII));
        return mac.doFinal();
    }

    private byte[] generateSaltedPassword(final byte[] passwordBytes,
                                          String hmacName,
                                          final int iterationCount,
                                          final byte[] salt) throws Exception
    {
        Mac mac = createHmac(passwordBytes, hmacName);
        mac.update(salt);
        mac.update(new byte[]{0, 0, 0, 1});
        byte[] result = mac.doFinal();

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

    private Mac createHmac(final byte[] keyBytes, String hmacName) throws Exception
    {
        SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
        Mac mac = Mac.getInstance(hmacName);
        mac.init(key);
        return mac;
    }

    private String saslPrep(String name) throws SaslException
    {
        name = name.replace("=", "=3D");
        name = name.replace(",", "=2C");
        return name;
    }

    private byte[] createInitialResponse(final String userName) throws SaslException
    {
        _clientFirstMessageBare = "n=" + saslPrep(userName) + ",r=" + _clientNonce;
        return (GS2_HEADER + _clientFirstMessageBare).getBytes(ASCII);
    }

    private AuthenticationProvider createTestAuthenticationManager(String type)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getTestName());
        attributes.put(ConfiguredObject.ID, UUID.randomUUID());
        attributes.put(ConfiguredObject.TYPE, type);
        ConfiguredObjectFactory objectFactory = _broker.getObjectFactory();
        @SuppressWarnings("unchecked")
        AuthenticationProvider<?> configuredObject =
                objectFactory.create(AuthenticationProvider.class, attributes, _broker);
        assertEquals("Unexpected state", State.ACTIVE, configuredObject.getState());

        PasswordCredentialManagingAuthenticationProvider<?> authenticationProvider =
                (PasswordCredentialManagingAuthenticationProvider<?>) configuredObject;
        authenticationProvider.createUser(VALID_USER_NAME,
                                          VALID_USER_PASSWORD,
                                          Collections.<String, String>emptyMap());
        return configuredObject;
    }
}
