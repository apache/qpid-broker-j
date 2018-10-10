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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.database.HashedUser;
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

    @Before
    public void setUp() throws Exception
    {
        _passwordSource = mock(PasswordSource.class);
        when(_passwordSource.getPassword(eq(VALID_USERNAME))).thenReturn(VALID_USERPASSWORD);
        _authenticationProvider = mock(PasswordCredentialManagingAuthenticationProvider.class);
    }

    @After
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
        AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            firstResult.getStatus());


        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(mechanism, VALID_USERNAME, new String(VALID_USERPASSWORD), firstResult.getChallenge());

        AuthenticationResult secondResult = _negotiator.handleResponse(responseBytes);

        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            secondResult.getStatus());
        assertNull("Unexpected second result challenge", secondResult.getChallenge());
        assertEquals("Unexpected second result main principal",
                            VALID_USERNAME,
                            secondResult.getMainPrincipal().getName());

        verify(_passwordSource).getPassword(eq(VALID_USERNAME));

        AuthenticationResult thirdResult =  _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected third result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            thirdResult.getStatus());
    }

    private void doHandleResponseWithInvalidPassword(final String mechanism) throws Exception
    {
        AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            firstResult.getStatus());
        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(mechanism, VALID_USERNAME, INVALID_USERPASSWORD, firstResult.getChallenge());

        AuthenticationResult secondResult = _negotiator.handleResponse(responseBytes);

        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
        assertNull("Unexpected second result challenge", secondResult.getChallenge());
        assertNull("Unexpected second result main principal", secondResult.getMainPrincipal());

        verify(_passwordSource).getPassword(eq(VALID_USERNAME));

        AuthenticationResult thirdResult =  _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected third result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            thirdResult.getStatus());
    }

    private void doHandleResponseWithInvalidUsername(final String mechanism) throws Exception
    {
        AuthenticationResult firstResult = _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected first result status",
                            AuthenticationResult.AuthenticationStatus.CONTINUE,
                            firstResult.getStatus());
        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(mechanism, INVALID_USERNAME, new String(VALID_USERPASSWORD), firstResult.getChallenge());

        AuthenticationResult secondResult = _negotiator.handleResponse(responseBytes);

        assertEquals("Unexpected second result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            secondResult.getStatus());
        assertNull("Unexpected second result challenge", secondResult.getChallenge());
        assertNull("Unexpected second result main principal", secondResult.getMainPrincipal());

        verify(_passwordSource).getPassword(eq(INVALID_USERNAME));

        AuthenticationResult thirdResult =  _negotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected third result status",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            thirdResult.getStatus());
    }

    private void hashPassword()
    {
        HashedUser hashedUser = new HashedUser(VALID_USERNAME, VALID_USERPASSWORD, _authenticationProvider);
        char[] password = hashedUser.getPassword();
        when(_passwordSource.getPassword(eq(VALID_USERNAME))).thenReturn(password);
    }

    private void base64Password() throws NoSuchAlgorithmException
    {
        byte[] data = new String(VALID_USERPASSWORD).getBytes(StandardCharsets.UTF_8);
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(data);
        char[] password = Base64.getEncoder().encodeToString(md.digest()).toCharArray();
        when(_passwordSource.getPassword(eq(VALID_USERNAME))).thenReturn(password);
    }

}
