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
package org.apache.qpid.server.security.auth.manager;

import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper.assertOnlyContainsWrapped;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.UnitTestBase;

public class ExternalAuthenticationManagerTest extends UnitTestBase
{
    private ExternalAuthenticationManager<?> _manager;
    private ExternalAuthenticationManager<?> _managerUsingFullDN;
    private SaslSettings _saslSettings;

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> attrs = Map.of(AuthenticationProvider.ID, randomUUID(),
                AuthenticationProvider.NAME, getTestName(),
                "useFullDN",false);
        _manager = new ExternalAuthenticationManagerImpl(attrs, BrokerTestHelper.createBrokerMock());
        _manager.open();
        final Map<String, Object> attrsFullDN = Map.of(AuthenticationProvider.ID, randomUUID(),
                AuthenticationProvider.NAME, getTestName() + "FullDN",
                "useFullDN",true);

        _managerUsingFullDN = new ExternalAuthenticationManagerImpl(attrsFullDN, BrokerTestHelper.createBrokerMock());
        _managerUsingFullDN.open();

        _saslSettings = mock(SaslSettings.class);
        when(_saslSettings.getLocalFQDN()).thenReturn("example.example.com");
    }

    @Test
    public void testGetMechanisms()
    {
        assertEquals(Collections.singletonList("EXTERNAL"), _manager.getMechanisms());
    }

    @Test
    public void testCreateSaslNegotiator()
    {
        createSaslNegotiatorTestImpl(_manager);
    }

    @Test
    public void testAuthenticatePrincipalNull_CausesAuthError()
    {
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus(),
                "Expected authentication to be unsuccessful");

        assertNull(result.getMainPrincipal());
    }

    @Test
    public void testAuthenticatePrincipalNoCn_CausesAuthError()
    {
        final X500Principal principal = new X500Principal("DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus(),
                "Expected authentication to be unsuccessful");
        assertNull(result.getMainPrincipal());
    }

    @Test
    public void testAuthenticatePrincipalEmptyCn_CausesAuthError()
    {
        final X500Principal principal = new X500Principal("CN=, DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus(),
                "Expected authentication to be unsuccessful");
        assertNull(result.getMainPrincipal());
    }

    @Test
    public void testAuthenticatePrincipalCnOnly()
    {
        final X500Principal principal = new X500Principal("CN=person");
        final UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Expected authentication to be successful");
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person", result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticatePrincipalCnAndDc()
    {
        final X500Principal principal = new X500Principal("CN=person, DC=example, DC=com");
        final UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person@example.com", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Expected authentication to be successful");
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person@example.com", result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticatePrincipalCnDc_OtherComponentsIgnored()
    {
        final X500Principal principal = new X500Principal("CN=person, DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        final UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person@example.com", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Expected authentication to be successful");
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person@example.com", result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticatePrincipalCn_OtherComponentsIgnored()
    {
        final X500Principal principal = new X500Principal("CN=person, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        final UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);

        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Expected authentication to be successful");
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person", result.getMainPrincipal().getName());
    }

    @Test
    public void testFullDNMode_CreateSaslNegotiator()
    {
        createSaslNegotiatorTestImpl(_managerUsingFullDN);
    }

    @Test
    public void testFullDNMode_Authenticate()
    {
        final X500Principal principal = new X500Principal("CN=person, DC=example, DC=com");
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        final SaslNegotiator negotiator = _managerUsingFullDN.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Expected authentication to be successful");

        assertOnlyContainsWrapped(principal, result.getPrincipals());
        assertEquals("CN=person,DC=example,DC=com", result.getMainPrincipal().getName());
    }

    private void createSaslNegotiatorTestImpl(final AuthenticationProvider<?> manager)
    {
        SaslNegotiator negotiator = manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        assertNotNull(negotiator, "Could not create SASL negotiator for 'EXTERNAL' mechanism.");

        negotiator = manager.createSaslNegotiator("PLAIN", _saslSettings, null);
        assertNull(negotiator, "Should not be able to create SASL negotiator with incorrect mechanism.");
    }
}
