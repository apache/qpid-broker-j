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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.test.utils.UnitTestBase;

public class AnonymousAuthenticationManagerTest extends UnitTestBase
{
    private AnonymousAuthenticationManager _manager;

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> attrs = Map.of(AuthenticationProvider.ID, randomUUID(),
                AuthenticationProvider.NAME, getTestName());
        _manager = new AnonymousAuthenticationManager(attrs, BrokerTestHelper.createBrokerMock());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_manager != null)
        {
            _manager = null;
        }
    }

    @Test
    public void testGetMechanisms()
    {
        assertEquals(List.of("ANONYMOUS"), _manager.getMechanisms());
    }

    @Test
    public void testCreateSaslNegotiator()
    {
        SaslNegotiator negotiator = _manager.createSaslNegotiator("ANONYMOUS", null, null);
        assertNotNull(negotiator, "Could not create SASL negotiator for mechanism 'ANONYMOUS'");

        negotiator = _manager.createSaslNegotiator("PLAIN", null, null);
        assertNull(negotiator, "Should not be able to create SASL negotiator for mechanism 'PLAIN'");
    }

    @Test
    public void testAuthenticate()
    {
        final SaslNegotiator negotiator = _manager.createSaslNegotiator("ANONYMOUS", null, null);
        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);
        assertNotNull(result);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Expected authentication to be successful");
        assertOnlyContainsWrapped(_manager.getAnonymousPrincipal(), result.getPrincipals());
    }
}
