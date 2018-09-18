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
package org.apache.qpid.tests.http.authentication;

import static javax.servlet.http.HttpServletResponse.SC_EXPECTATION_FAILED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.apache.qpid.server.security.auth.sasl.SaslUtil.generateCramMD5ClientResponse;
import static org.apache.qpid.server.security.auth.sasl.SaslUtil.generatePlainClientResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.tests.http.HttpTestBase;

public class SaslTest extends HttpTestBase
{
    private static final String SASL_SERVICE = "/service/sasl";
    private static final String SET_COOKIE_HEADER = "Set-Cookie";
    private String _userName;
    private String _userPassword;

    @Before
    public void setUp()
    {
        _userName = getBrokerAdmin().getValidUsername();
        _userPassword = getBrokerAdmin().getValidPassword();
    }

    @Test
    public void requestSASLMechanisms() throws Exception
    {
        Map<String, Object> saslData = getHelper().getJsonAsMap(SASL_SERVICE);
        assertNotNull("mechanisms attribute is not found", saslData.get("mechanisms"));

        @SuppressWarnings("unchecked")
        List<String> mechanisms = (List<String>) saslData.get("mechanisms");
        String[] expectedMechanisms = {PlainNegotiator.MECHANISM,
                CramMd5Negotiator.MECHANISM,
                ScramSHA1AuthenticationManager.MECHANISM,
                ScramSHA256AuthenticationManager.MECHANISM};
        for (String mechanism : expectedMechanisms)
        {
            assertTrue(String.format("Mechanism '%s' is not found", mechanism), mechanisms.contains(mechanism));
        }
        assertNull(String.format("Unexpected user was returned: %s", saslData.get("user")), saslData.get("user"));
    }

    @Test
    public void requestUnsupportedSASLMechanism() throws Exception
    {
        HttpURLConnection connection = requestSASLAuthentication("UNSUPPORTED");
        try
        {
            assertEquals("Unexpected response", SC_EXPECTATION_FAILED, connection.getResponseCode());
        }
        finally
        {
            connection.disconnect();
        }
    }

    @Test
    public void plainSASLAuthenticationWithoutInitialResponse() throws Exception
    {
        HttpURLConnection connection = requestSASLAuthentication(PlainNegotiator.MECHANISM);
        try
        {
            assertEquals("Unexpected response", SC_OK, connection.getResponseCode());
            handleChallengeAndSendResponse(connection, _userName, _userPassword, PlainNegotiator.MECHANISM, SC_OK);

            assertAuthenticatedUser(_userName, connection.getHeaderFields().get(SET_COOKIE_HEADER));
        }
        finally
        {
            connection.disconnect();
        }
    }

    @Test
    public void plainSASLAuthenticationWithMalformedInitialResponse() throws Exception
    {
        byte[] responseBytes = "null".getBytes();
        String responseData = Base64.getEncoder().encodeToString(responseBytes);
        String parameters = String.format("mechanism=%s&response=%s", PlainNegotiator.MECHANISM, responseData);

        HttpURLConnection connection = getHelper().openManagementConnection(SASL_SERVICE, "POST");
        try
        {
            try (OutputStream os = connection.getOutputStream())
            {
                os.write(parameters.getBytes());
                os.flush();

                assertEquals("Unexpected response code", SC_UNAUTHORIZED, connection.getResponseCode());

                assertAuthenticatedUser(null, connection.getHeaderFields().get(SET_COOKIE_HEADER));
            }
        }
        finally
        {
            connection.disconnect();
        }
    }

    @Test
    public void plainSASLAuthenticationWithValidCredentials() throws Exception
    {
        List<String> cookies = plainSASLAuthenticationWithInitialResponse(_userName, _userPassword, SC_OK);

        assertAuthenticatedUser(_userName, cookies);
    }

    @Test
    public void plainSASLAuthenticationWithIncorrectPassword() throws Exception
    {
        List<String> cookies = plainSASLAuthenticationWithInitialResponse(_userName, "incorrect", SC_UNAUTHORIZED);

        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void plainSASLAuthenticationWithUnknownUser() throws Exception
    {
        List<String> cookies = plainSASLAuthenticationWithInitialResponse("unknown", _userPassword, SC_UNAUTHORIZED);

        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationForValidCredentials() throws Exception
    {
        List<String> cookies =
                challengeResponseAuthentication(_userName, _userPassword, CramMd5Negotiator.MECHANISM, SC_OK);
        assertAuthenticatedUser(_userName, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationForIncorrectPassword() throws Exception
    {
        List<String> cookies =
                challengeResponseAuthentication(_userName, "incorrect", CramMd5Negotiator.MECHANISM, SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationForNonExistingUser() throws Exception
    {
        List<String> cookies =
                challengeResponseAuthentication("unknown", _userPassword, CramMd5Negotiator.MECHANISM, SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationResponseNotProvided() throws Exception
    {
        HttpURLConnection connection = requestSASLAuthentication(CramMd5Negotiator.MECHANISM);
        try
        {
            Map<String, Object> response = getHelper().readJsonResponseAsMap(connection);
            String challenge = (String) response.get("challenge");
            assertNotNull("Challenge is not found", challenge);

            List<String> cookies = connection.getHeaderFields().get(SET_COOKIE_HEADER);

            String requestParameters = (String.format("id=%s", response.get("id")));
            postResponse(cookies, requestParameters, SC_UNAUTHORIZED);

            assertAuthenticatedUser(null, cookies);
        }
        finally
        {
            connection.disconnect();
        }
    }

    @Test
    public void cramMD5SASLAuthenticationWithMalformedResponse() throws Exception
    {
        HttpURLConnection connection = requestSASLAuthentication(CramMd5Negotiator.MECHANISM);
        try
        {
            Map<String, Object> response = getHelper().readJsonResponseAsMap(connection);
            String challenge = (String) response.get("challenge");
            assertNotNull("Challenge is not found", challenge);

            List<String> cookies = connection.getHeaderFields().get(SET_COOKIE_HEADER);

            String responseData = Base64.getEncoder().encodeToString("null".getBytes());
            String requestParameters = String.format("id=%s&response=%s", response.get("id"), responseData);

            postResponse(cookies, requestParameters, SC_UNAUTHORIZED);

            assertAuthenticatedUser(null, cookies);
        }
        finally
        {
            connection.disconnect();
        }
    }

    @Test
    public void cramMD5SASLAuthenticationWithInvalidId() throws Exception
    {
        HttpURLConnection connection = requestSASLAuthentication(CramMd5Negotiator.MECHANISM);
        try
        {
            Map<String, Object> response = getHelper().readJsonResponseAsMap(connection);
            String challenge = (String) response.get("challenge");
            assertNotNull("Challenge is not found", challenge);

            List<String> cookies = connection.getHeaderFields().get(SET_COOKIE_HEADER);

            byte[] challengeBytes = Base64.getDecoder().decode(challenge);
            byte[] responseBytes =
                    generateClientResponse(CramMd5Negotiator.MECHANISM, _userName, _userPassword, challengeBytes);
            String responseData = Base64.getEncoder().encodeToString(responseBytes);
            String requestParameters = (String.format("id=%s&response=%s", UUID.randomUUID().toString(), responseData));

            postResponse(cookies, requestParameters, SC_EXPECTATION_FAILED);

            assertAuthenticatedUser(null, cookies);
        }
        finally
        {
            connection.disconnect();
        }
    }

    private List<String> plainSASLAuthenticationWithInitialResponse(final String userName,
                                                                    final String userPassword,
                                                                    final int expectedResponseCode) throws Exception
    {
        byte[] responseBytes = generatePlainClientResponse(userName, userPassword);
        String responseData = Base64.getEncoder().encodeToString(responseBytes);
        String parameters = String.format("mechanism=%s&response=%s", PlainNegotiator.MECHANISM, responseData);

        HttpURLConnection connection = getHelper().openManagementConnection(SASL_SERVICE, "POST");
        try
        {
            try (OutputStream os = connection.getOutputStream())
            {
                os.write(parameters.getBytes());
                os.flush();

                assertEquals("Unexpected response code", expectedResponseCode, connection.getResponseCode());
            }
            return connection.getHeaderFields().get(SET_COOKIE_HEADER);
        }
        finally
        {
            connection.disconnect();
        }
    }

    private List<String> challengeResponseAuthentication(final String userName,
                                                         final String userPassword,
                                                         final String mechanism,
                                                         final int expectedResponseCode)
            throws Exception
    {
        HttpURLConnection connection = requestSASLAuthentication(mechanism);
        try
        {
            handleChallengeAndSendResponse(connection, userName, userPassword, mechanism, expectedResponseCode);
            return connection.getHeaderFields().get(SET_COOKIE_HEADER);
        }
        finally
        {
            connection.disconnect();
        }
    }


    private void handleChallengeAndSendResponse(HttpURLConnection requestChallengeConnection,
                                                String userName,
                                                String userPassword,
                                                String mechanism,
                                                final int expectedResponseCode)
            throws Exception
    {
        Map<String, Object> response = getHelper().readJsonResponseAsMap(requestChallengeConnection);
        String challenge = (String) response.get("challenge");
        assertNotNull("Challenge is not found", challenge);

        byte[] challengeBytes = Base64.getDecoder().decode(challenge);
        byte[] responseBytes = generateClientResponse(mechanism, userName, userPassword, challengeBytes);
        String responseData = Base64.getEncoder().encodeToString(responseBytes);
        String requestParameters = (String.format("id=%s&response=%s", response.get("id"), responseData));

        postResponse(requestChallengeConnection.getHeaderFields().get(SET_COOKIE_HEADER),
                     requestParameters,
                     expectedResponseCode);
    }

    private void postResponse(final List<String> cookies,
                              final String requestParameters,
                              final int expectedResponseCode) throws IOException
    {
        HttpURLConnection authenticateConnection = getHelper().openManagementConnection(SASL_SERVICE, "POST");
        try
        {
            applyCookiesToConnection(cookies, authenticateConnection);
            try (OutputStream os = authenticateConnection.getOutputStream())
            {
                os.write(requestParameters.getBytes());
                os.flush();
                assertEquals("Unexpected response code",
                             expectedResponseCode,
                             authenticateConnection.getResponseCode());
            }
        }
        finally
        {
            authenticateConnection.disconnect();
        }
    }

    private byte[] generateClientResponse(String mechanism, String userName, String userPassword, byte[] challengeBytes)
            throws Exception
    {
        byte[] responseBytes;
        if (PlainNegotiator.MECHANISM.equals(mechanism))
        {
            responseBytes = generatePlainClientResponse(_userName, _userPassword);
        }
        else if (CramMd5Negotiator.MECHANISM.equalsIgnoreCase(mechanism))
        {
            responseBytes = generateCramMD5ClientResponse(userName, userPassword, challengeBytes);
        }
        else
        {
            throw new RuntimeException("Not implemented test mechanism " + mechanism);
        }
        return responseBytes;
    }


    private void applyCookiesToConnection(List<String> cookies, HttpURLConnection connection)
    {
        for (String cookie : cookies)
        {
            connection.addRequestProperty("Cookie", cookie.split(";", 2)[0]);
        }
    }

    private HttpURLConnection requestSASLAuthentication(String mechanism) throws IOException
    {
        HttpURLConnection connection = getHelper().openManagementConnection(SASL_SERVICE, "POST");
        OutputStream os = connection.getOutputStream();
        os.write(String.format("mechanism=%s", mechanism).getBytes());
        os.flush();
        return connection;
    }

    private void assertAuthenticatedUser(final String userName, final List<String> cookies) throws IOException
    {
        HttpURLConnection connection = getHelper().openManagementConnection(SASL_SERVICE, "GET");
        try
        {
            applyCookiesToConnection(cookies, connection);
            Map<String, Object> response = getHelper().readJsonResponseAsMap(connection);
            assertEquals("Unexpected user", userName, response.get("user"));
        }
        finally
        {
            connection.disconnect();
        }
    }
}
