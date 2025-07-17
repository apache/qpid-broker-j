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

package org.apache.qpid.server.security.auth.sasl.crammd5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.security.auth.sasl.SaslUtil;
import org.apache.qpid.test.utils.UnitTestBase;

public class CramMd5NegotiatorTest extends UnitTestBase
{
    private static final String TEST_FQDN = "example.com";
    private static final String VALID_USERNAME = "testUser";
    private static final char[] VALID_USERPASSWORD = "testPassword".toCharArray();
    private static final String INVALID_USERPASSWORD = "invalidPassword";
    private static final String INVALID_USERNAME = "invalidUser" ;

    private AbstractCramMd5Negotiator _negotiator;
    private PasswordSource _passwordSource;
    private PasswordCredentialManagingAuthenticationProvider<?> _authenticationProvider;

    @BeforeEach
    public void setUp() throws Exception
    {
        _passwordSource = mock(PasswordSource.class);
        when(_passwordSource.getPassword(eq(VALID_USERNAME))).thenReturn(VALID_USERPASSWORD);
        _authenticationProvider = mock(PasswordCredentialManagingAuthenticationProvider.class);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_negotiator != null)
        {
            _negotiator.dispose();
        }
    }

    @Test
    public void testHandleResponseCramMD5ValidCredentials() throws Exception
    {
        _negotiator = new CramMd5Negotiator(_authenticationProvider, TEST_FQDN, _passwordSource);
        doHandleResponseWithValidCredentials(CramMd5Negotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5InvalidPassword() throws Exception
    {
        _negotiator = new CramMd5Negotiator(_authenticationProvider, TEST_FQDN, _passwordSource);
        doHandleResponseWithInvalidPassword(CramMd5Negotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5InvalidUsername() throws Exception
    {
        _negotiator = new CramMd5Negotiator(_authenticationProvider, TEST_FQDN, _passwordSource);
        doHandleResponseWithInvalidUsername(CramMd5Negotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5HashedValidCredentials() throws Exception
    {
        hashPassword();

        _negotiator = new CramMd5HashedNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithValidCredentials(CramMd5HashedNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5HashedInvalidPassword() throws Exception
    {
        hashPassword();

        _negotiator = new CramMd5HashedNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidPassword(CramMd5HashedNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5HashedInvalidUsername() throws Exception
    {
        hashPassword();

        _negotiator = new CramMd5HashedNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidUsername(CramMd5HashedNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5HexValidCredentials() throws Exception
    {
        hashPassword();

        _negotiator = new CramMd5HexNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithValidCredentials(CramMd5HexNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5HexInvalidPassword() throws Exception
    {
        hashPassword();

        _negotiator = new CramMd5HexNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidPassword(CramMd5HexNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5HexInvalidUsername() throws Exception
    {
        hashPassword();

        _negotiator = new CramMd5HexNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidUsername(CramMd5HexNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5Base64HexValidCredentials() throws Exception
    {
        base64Password();

        _negotiator = new CramMd5Base64HexNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithValidCredentials(CramMd5Base64HexNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5Base64HexInvalidPassword() throws Exception
    {
        base64Password();

        _negotiator = new CramMd5Base64HexNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidPassword(CramMd5Base64HexNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5Base64HexInvalidUsername() throws Exception
    {
        base64Password();

        _negotiator = new CramMd5Base64HexNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidUsername(CramMd5Base64HexNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5Base64HashedValidCredentials() throws Exception
    {
        base64Password();

        _negotiator = new CramMd5Base64HashedNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithValidCredentials(CramMd5Base64HashedNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5Base64HashedInvalidPassword() throws Exception
    {
        base64Password();

        _negotiator = new CramMd5Base64HashedNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidPassword(CramMd5Base64HashedNegotiator.MECHANISM);
    }

    @Test
    public void testHandleResponseCramMD5Base64HashedInvalidUsername() throws Exception
    {
        base64Password();

        _negotiator = new CramMd5Base64HashedNegotiator(_authenticationProvider, TEST_FQDN, _passwordSource);

        doHandleResponseWithInvalidUsername(CramMd5Base64HashedNegotiator.MECHANISM);
    }

    private void doHandleResponseWithValidCredentials(final String mechanism) throws Exception
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, firstResult.getStatus(),
                "Unexpected first result status");


        assertNotNull(firstResult.getChallenge(), "Unexpected first result challenge");

        final byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(mechanism, VALID_USERNAME, new String(VALID_USERPASSWORD), firstResult.getChallenge());

        final AuthenticationResult secondResult = _negotiator.handleResponse(responseBytes);

        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, secondResult.getStatus(),
                "Unexpected second result status");
        assertNull(secondResult.getChallenge(), "Unexpected second result challenge");
        assertEquals(VALID_USERNAME, secondResult.getMainPrincipal().getName(),
                "Unexpected second result main principal");

        verify(_passwordSource).getPassword(eq(VALID_USERNAME));

        final AuthenticationResult thirdResult =  _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, thirdResult.getStatus(),
                "Unexpected third result status");
    }

    private void doHandleResponseWithInvalidPassword(final String mechanism) throws Exception
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, firstResult.getStatus(),
                "Unexpected first result status");
        assertNotNull(firstResult.getChallenge(), "Unexpected first result challenge");

        final byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(mechanism, VALID_USERNAME, INVALID_USERPASSWORD, firstResult.getChallenge());

        final AuthenticationResult secondResult = _negotiator.handleResponse(responseBytes);

        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
        assertNull(secondResult.getChallenge(), "Unexpected second result challenge");
        assertNull(secondResult.getMainPrincipal(), "Unexpected second result main principal");

        verify(_passwordSource).getPassword(eq(VALID_USERNAME));

        final AuthenticationResult thirdResult =  _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, thirdResult.getStatus(),
                "Unexpected third result status");
    }

    private void doHandleResponseWithInvalidUsername(final String mechanism) throws Exception
    {
        final AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, firstResult.getStatus(),
                "Unexpected first result status");
        assertNotNull(firstResult.getChallenge(), "Unexpected first result challenge");

        final byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(mechanism, INVALID_USERNAME, new String(VALID_USERPASSWORD), firstResult.getChallenge());

        final AuthenticationResult secondResult = _negotiator.handleResponse(responseBytes);

        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Unexpected second result status");
        assertNull(secondResult.getChallenge(), "Unexpected second result challenge");
        assertNull(secondResult.getMainPrincipal(), "Unexpected second result main principal");

        verify(_passwordSource).getPassword(eq(INVALID_USERNAME));

        final AuthenticationResult thirdResult =  _negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, thirdResult.getStatus(),
                "Unexpected third result status");
    }

    private void hashPassword()
    {
        final char[] password = hashPassword(VALID_USERPASSWORD);
        when(_passwordSource.getPassword(eq(VALID_USERNAME))).thenReturn(password);
    }

    private void base64Password() throws NoSuchAlgorithmException
    {
        final byte[] data = new String(VALID_USERPASSWORD).getBytes(StandardCharsets.UTF_8);
        final MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(data);
        final char[] password = Base64.getEncoder().encodeToString(md.digest()).toCharArray();
        when(_passwordSource.getPassword(eq(VALID_USERNAME))).thenReturn(password);
    }

    private char[] hashPassword(char[] password)
    {
        byte[] byteArray = new byte[password.length];
        int index = 0;
        for (char c : password)
        {
            byteArray[index++] = (byte) c;
        }

        byte[] md5ByteArray = getMD5(byteArray);

        char[] result = new char[md5ByteArray.length];

        index = 0;
        for (byte c : md5ByteArray)
        {
            result[index++] = (char) c;
        }
        return result;
    }

    private byte[] getMD5(byte[] data)
    {
        MessageDigest md = null;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new ServerScopedRuntimeException("MD5 not supported although Java compliance requires it");
        }

        for (byte b : data)
        {
            md.update(b);
        }

        return md.digest();
    }
}
