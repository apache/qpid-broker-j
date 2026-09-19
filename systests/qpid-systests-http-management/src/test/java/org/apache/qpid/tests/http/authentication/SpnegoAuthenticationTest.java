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

import static jakarta.servlet.http.HttpServletResponse.SC_CREATED;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.apache.qpid.server.test.KerberosUtilities.ACCEPT_SCOPE;
import static org.apache.qpid.server.test.KerberosUtilities.CLIENT_PRINCIPAL_FULL_NAME;
import static org.apache.qpid.server.test.KerberosUtilities.CLIENT_PRINCIPAL_NAME;
import static org.apache.qpid.server.test.KerberosUtilities.HOST_NAME;
import static org.apache.qpid.server.test.KerberosUtilities.REALM;
import static org.apache.qpid.server.test.KerberosUtilities.SERVICE_PRINCIPAL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.auth.manager.KerberosAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.SpnegoAuthenticator;
import org.apache.qpid.server.test.KerberosUtilities;
import org.apache.qpid.test.utils.EmbeddedKdcExtension;
import org.apache.qpid.test.utils.SystemPropertySetter;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class SpnegoAuthenticationTest extends HttpTestBase
{
    private static final String SASL_SERVICE = "/service/sasl";
    private static final KerberosUtilities UTILS = new KerberosUtilities();

    @RegisterExtension
    public static final EmbeddedKdcExtension KDC = new EmbeddedKdcExtension(HOST_NAME, 0, "QpidHttpTestKerberosServer",
            REALM);

    @RegisterExtension
    public static final SystemPropertySetter SYSTEM_PROPERTY_SETTER = new SystemPropertySetter();

    private static File _clientKeyTabFile;

    private final Deque<String> _createdObjects = new ArrayDeque<>();
    private HttpTestHelper _kerberosHelper;

    @BeforeAll
    public static void configureKerberos() throws Exception
    {
        UTILS.prepareConfiguration(HOST_NAME, SYSTEM_PROPERTY_SETTER);
        _clientKeyTabFile = UTILS.prepareKeyTabs(KDC);
    }

    @BeforeEach
    public void configureHttpPort() throws Exception
    {
        final String provider = getTestName() + "-kerberos";
        final String port = getTestName() + "-http";
        final Map<String, String> context = Map.of("qpid.auth.gssapi.spnegoConfigScope", ACCEPT_SCOPE);
        getHelper().submitRequest("authenticationprovider/" + provider, "PUT",
                Map.of(AuthenticationProvider.TYPE, KerberosAuthenticationManager.PROVIDER_TYPE,
                        ConfiguredObject.CONTEXT, context), SC_CREATED);
        _createdObjects.addFirst("authenticationprovider/" + provider);
        getHelper().submitRequest("port/" + port, "PUT",
                Map.of(Port.TYPE, "HTTP", Port.PORT, 0, Port.AUTHENTICATION_PROVIDER, provider,
                        Port.PROTOCOLS, Set.of(Protocol.HTTP), Port.TRANSPORTS, Set.of(Transport.TCP)), SC_CREATED);
        _createdObjects.addFirst("port/" + port);
        final int boundPort = ((Number) getHelper().getJsonAsMap("port/" + port).get("boundPort")).intValue();
        _kerberosHelper = new HttpTestHelper(getBrokerAdmin(), null, boundPort);
        _kerberosHelper.setUserName(null);
    }

    @AfterEach
    public void removeHttpPort() throws Exception
    {
        while (!_createdObjects.isEmpty())
        {
            getHelper().submitRequest(_createdObjects.removeFirst(), "DELETE", SC_OK);
        }
    }

    @Test
    public void testInteractiveAuthenticationRenewsSession() throws Exception
    {
        final String initialCookie = prepareSession();
        final String renewedCookie = authenticate(initialCookie);

        assertNotEquals(initialCookie, renewedCookie, "Authentication must renew the session cookie");
        assertEquals(CLIENT_PRINCIPAL_FULL_NAME, getSessionUser(renewedCookie));
        assertNull(getSessionUser(initialCookie));
    }

    @Test
    public void testInteractiveAuthenticationCreatesSession() throws Exception
    {
        assertEquals(CLIENT_PRINCIPAL_FULL_NAME, getSessionUser(authenticate(null)));
    }

    @Test
    public void testChallengeDoesNotAuthenticateSession() throws Exception
    {
        final String cookie = prepareSession();
        final HttpURLConnection connection = _kerberosHelper
                .openManagementConnection(HttpManagement.DEFAULT_LOGIN_URL, "GET");
        try
        {
            connection.setRequestProperty("Cookie", cookie);
            assertEquals(SC_UNAUTHORIZED, connection.getResponseCode());
            assertEquals(SpnegoAuthenticator.RESPONSE_AUTH_HEADER_VALUE_NEGOTIATE,
                    connection.getHeaderField(SpnegoAuthenticator.RESPONSE_AUTH_HEADER_NAME));
        }
        finally
        {
            connection.disconnect();
        }
        assertNull(getSessionUser(cookie));
    }

    @Test
    public void testManagementSessionsUseCookieTracking() throws Exception
    {
        final String cookie = authenticate(prepareSession());
        final String sessionId = HttpCookie.parse(cookie).get(0).getValue();
        final HttpURLConnection connection = _kerberosHelper
                .openManagementConnection(SASL_SERVICE + ";jsessionid=" + sessionId, "GET");
        try
        {
            assertEquals(SC_OK, connection.getResponseCode());
            assertNull(_kerberosHelper.readJsonResponseAsMap(connection).get("user"));
        }
        finally
        {
            connection.disconnect();
        }
        assertEquals(CLIENT_PRINCIPAL_FULL_NAME, getSessionUser(cookie));
    }

    private String prepareSession() throws Exception
    {
        final HttpURLConnection connection = _kerberosHelper.openManagementConnection(SASL_SERVICE, "GET");
        try
        {
            assertEquals(SC_OK, connection.getResponseCode());
            assertNull(_kerberosHelper.readJsonResponseAsMap(connection).get("user"));
            return getSessionCookie(connection);
        }
        finally
        {
            connection.disconnect();
        }
    }

    private String authenticate(final String cookie) throws Exception
    {
        final byte[] token = UTILS.buildToken(CLIENT_PRINCIPAL_NAME, _clientKeyTabFile, SERVICE_PRINCIPAL_NAME);
        final HttpURLConnection connection = _kerberosHelper
                .openManagementConnection(HttpManagement.DEFAULT_LOGIN_URL, "GET");
        try
        {
            connection.setInstanceFollowRedirects(false);
            if (cookie != null)
            {
                connection.setRequestProperty("Cookie", cookie);
            }
            connection.setRequestProperty(SpnegoAuthenticator.REQUEST_AUTH_HEADER_NAME,
                    SpnegoAuthenticator.RESPONSE_AUTH_HEADER_VALUE_NEGOTIATE + " " +
                    Base64.getEncoder().encodeToString(token));
            assertEquals(SC_OK, connection.getResponseCode());
            return getSessionCookie(connection);
        }
        finally
        {
            connection.disconnect();
        }
    }

    private Object getSessionUser(final String cookie) throws Exception
    {
        final HttpURLConnection connection = _kerberosHelper.openManagementConnection(SASL_SERVICE, "GET");
        try
        {
            connection.setRequestProperty("Cookie", cookie);
            assertEquals(SC_OK, connection.getResponseCode());
            return _kerberosHelper.readJsonResponseAsMap(connection).get("user");
        }
        finally
        {
            connection.disconnect();
        }
    }

    private String getSessionCookie(final HttpURLConnection connection)
    {
        final String cookie = connection.getHeaderField("Set-Cookie");
        assertNotNull(cookie, "Expected a management session cookie");
        return cookie.split(";", 2)[0];
    }
}
