/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.security.auth.manager;

import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper.assertOnlyContainsWrapped;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.QpidTestCase;

public class AnonymousAuthenticationManagerTest extends QpidTestCase
{
    private AnonymousAuthenticationManager _manager;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        Map<String,Object> attrs = new HashMap<String, Object>();
        attrs.put(AuthenticationProvider.ID, UUID.randomUUID());
        attrs.put(AuthenticationProvider.NAME, getTestName());
        _manager = new AnonymousAuthenticationManager(attrs, BrokerTestHelper.createBrokerMock());

    }

    public void tearDown() throws Exception
    {
        if(_manager != null)
        {
            _manager = null;
        }
        super.tearDown();
    }

    public void testGetMechanisms() throws Exception
    {
        assertEquals(Collections.singletonList("ANONYMOUS"), _manager.getMechanisms());
    }

    public void testCreateSaslNegotiator() throws Exception
    {
        SaslNegotiator negotiator = _manager.createSaslNegotiator("ANONYMOUS", null);
        assertNotNull("Could not create SASL negotiator for mechanism 'ANONYMOUS'", negotiator);

        negotiator = _manager.createSaslNegotiator("PLAIN", null);
        assertNull("Should not be able to create SASL negotiator for mechanism 'PLAIN'", negotiator);
    }

    public void testAuthenticate() throws Exception
    {
        SaslNegotiator negotiator = _manager.createSaslNegotiator("ANONYMOUS", null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);
        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                     AuthenticationResult.AuthenticationStatus.SUCCESS,
                     result.getStatus());

        assertOnlyContainsWrapped(_manager.getAnonymousPrincipal(), result.getPrincipals());
    }


}
