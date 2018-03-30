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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.x500.X500Principal;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.UnitTestBase;

public class ExternalAuthenticationManagerTest extends UnitTestBase
{
    private ExternalAuthenticationManager _manager;
    private ExternalAuthenticationManager _managerUsingFullDN;
    private SaslSettings _saslSettings;

    @Before
    public void setUp() throws Exception
    {
        Map<String,Object> attrs = new HashMap<>();
        attrs.put(AuthenticationProvider.ID, UUID.randomUUID());
        attrs.put(AuthenticationProvider.NAME, getTestName());
        attrs.put("useFullDN",false);
        _manager = new ExternalAuthenticationManagerImpl(attrs, BrokerTestHelper.createBrokerMock());
        _manager.open();
        HashMap<String, Object> attrsFullDN = new HashMap<>();
        attrsFullDN.put(AuthenticationProvider.ID, UUID.randomUUID());
        attrsFullDN.put(AuthenticationProvider.NAME, getTestName()+"FullDN");
        attrsFullDN.put("useFullDN",true);

        _managerUsingFullDN = new ExternalAuthenticationManagerImpl(attrsFullDN, BrokerTestHelper.createBrokerMock());
        _managerUsingFullDN.open();

        _saslSettings = mock(SaslSettings.class);
        when(_saslSettings.getLocalFQDN()).thenReturn("example.example.com");
    }

    @Test
    public void testGetMechanisms() throws Exception
    {
        assertEquals(Collections.singletonList("EXTERNAL"), _manager.getMechanisms());
    }

    @Test
    public void testCreateSaslNegotiator() throws Exception
    {
        createSaslNegotiatorTestImpl(_manager);
    }

    @Test
    public void testAuthenticatePrincipalNull_CausesAuthError() throws Exception
    {
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be unsuccessful",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            result.getStatus());

        assertNull(result.getMainPrincipal());
    }

    @Test
    public void testAuthenticatePrincipalNoCn_CausesAuthError() throws Exception
    {
        X500Principal principal = new X500Principal("DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be unsuccessful",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            result.getStatus());
        assertNull(result.getMainPrincipal());
    }

    @Test
    public void testAuthenticatePrincipalEmptyCn_CausesAuthError() throws Exception
    {
        X500Principal principal = new X500Principal("CN=, DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be unsuccessful",
                            AuthenticationResult.AuthenticationStatus.ERROR,
                            result.getStatus());
        assertNull(result.getMainPrincipal());
    }

    @Test
    public void testAuthenticatePrincipalCnOnly() throws Exception
    {
        X500Principal principal = new X500Principal("CN=person");
        UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            result.getStatus());
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person", result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticatePrincipalCnAndDc() throws Exception
    {
        X500Principal principal = new X500Principal("CN=person, DC=example, DC=com");
        UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person@example.com", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            result.getStatus());
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person@example.com", result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticatePrincipalCnDc_OtherComponentsIgnored() throws Exception
    {
        X500Principal principal = new X500Principal("CN=person, DC=example, DC=com, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person@example.com", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            result.getStatus());
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person@example.com", result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticatePrincipalCn_OtherComponentsIgnored() throws Exception
    {
        X500Principal principal = new X500Principal("CN=person, O=My Company Ltd, L=Newbury, ST=Berkshire, C=GB");
        UsernamePrincipal expectedPrincipal = new UsernamePrincipal("person", _manager);
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);

        AuthenticationResult result = negotiator.handleResponse(new byte[0]);
        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            result.getStatus());
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals("person", result.getMainPrincipal().getName());
    }

    @Test
    public void testFullDNMode_CreateSaslNegotiator() throws Exception
    {
        createSaslNegotiatorTestImpl(_managerUsingFullDN);
    }

    @Test
    public void testFullDNMode_Authenticate() throws Exception
    {
        X500Principal principal = new X500Principal("CN=person, DC=example, DC=com");
        when(_saslSettings.getExternalPrincipal()).thenReturn(principal);
        SaslNegotiator negotiator = _managerUsingFullDN.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);

        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            result.getStatus());

        assertOnlyContainsWrapped(principal, result.getPrincipals());
        assertEquals("CN=person,DC=example,DC=com", result.getMainPrincipal().getName());
    }

    private void createSaslNegotiatorTestImpl(AuthenticationProvider<?> manager) throws Exception
    {
        SaslNegotiator negotiator = manager.createSaslNegotiator("EXTERNAL", _saslSettings, null);
        assertNotNull("Could not create SASL negotiator for 'EXTERNAL' mechanism.", negotiator);

        negotiator = manager.createSaslNegotiator("PLAIN", _saslSettings, null);
        assertNull("Should not be able to create SASL negotiator with incorrect mechanism.", negotiator);
    }

}
