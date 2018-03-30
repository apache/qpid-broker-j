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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.test.utils.UnitTestBase;

public class AnonymousAuthenticationManagerTest extends UnitTestBase
{
    private AnonymousAuthenticationManager _manager;

    @Before
    public void setUp() throws Exception
    {
        Map<String,Object> attrs = new HashMap<String, Object>();
        attrs.put(AuthenticationProvider.ID, UUID.randomUUID());
        attrs.put(AuthenticationProvider.NAME, getTestName());
        _manager = new AnonymousAuthenticationManager(attrs, BrokerTestHelper.createBrokerMock());

    }

    @After
    public void tearDown() throws Exception
    {
        if(_manager != null)
        {
            _manager = null;
        }
    }

    @Test
    public void testGetMechanisms() throws Exception
    {
        assertEquals(Collections.singletonList("ANONYMOUS"), _manager.getMechanisms());
    }

    @Test
    public void testCreateSaslNegotiator() throws Exception
    {
        SaslNegotiator negotiator = _manager.createSaslNegotiator("ANONYMOUS", null, null);
        assertNotNull("Could not create SASL negotiator for mechanism 'ANONYMOUS'", negotiator);

        negotiator = _manager.createSaslNegotiator("PLAIN", null, null);
        assertNull("Should not be able to create SASL negotiator for mechanism 'PLAIN'", negotiator);
    }

    @Test
    public void testAuthenticate() throws Exception
    {
        SaslNegotiator negotiator = _manager.createSaslNegotiator("ANONYMOUS", null, null);
        AuthenticationResult result = negotiator.handleResponse(new byte[0]);
        assertNotNull(result);
        assertEquals("Expected authentication to be successful",
                            AuthenticationResult.AuthenticationStatus.SUCCESS,
                            result.getStatus());


        assertOnlyContainsWrapped(_manager.getAnonymousPrincipal(), result.getPrincipals());
    }


}
