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
package org.apache.qpid.server.security.auth.manager;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.security.auth.login.AccountNotFoundException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.UnitTestBase;

abstract class ManagedAuthenticationManagerTestBase extends UnitTestBase
{
    private static final String TEST_USER_NAME = "admin";
    private static final String TEST_USER_PASSWORD = "admin";

    private ConfigModelPasswordManagingAuthenticationProvider<?> _authManager;
    private Broker<?> _broker;

    @BeforeEach
    public void setUp() throws Exception
    {
        _broker = BrokerTestHelper.createNewBrokerMock();
        final Map<String, Object> attributesMap = Map.of(AuthenticationProvider.NAME, getTestName(),
                AuthenticationProvider.ID, randomUUID());
        _authManager = createAuthManager(attributesMap);
        _authManager.open();
    }

    protected abstract ConfigModelPasswordManagingAuthenticationProvider<?> createAuthManager(final Map<String, Object> attributesMap);

    public Broker<?> getBroker()
    {
        return _broker;
    }

    public ConfigModelPasswordManagingAuthenticationProvider<?> getAuthManager()
    {
        return _authManager;
    }

    @Test
    public void testMechanisms()
    {
        assertFalse(_authManager.getAvailableMechanisms(false).contains("PLAIN"),
                "PLAIN authentication should not be available on an insecure connection");
        assertTrue(_authManager.getAvailableMechanisms(true).contains("PLAIN"),
                "PLAIN authentication should be available on a secure connection");
    }

    @Test
    public void testAddChildAndThenDelete() throws ExecutionException, InterruptedException
    {
        // No children should be present before the test starts
        assertEquals(0, _authManager.getChildren(User.class).size(),
                "No users should be present before the test starts");
        assertEquals(0, _authManager.getUsers().size(), "No users should be present before the test starts");

        final Map<String, Object> childAttrs = new HashMap<>();
        childAttrs.put(User.NAME, getTestName());
        childAttrs.put(User.PASSWORD, "password");
        final User<?> user = _authManager.addChildAsync(User.class, childAttrs).get();
        assertNotNull(user, "User should be created but addChild returned null");
        assertEquals(getTestName(), user.getName());
        if (!isPlain())
        {
            // password shouldn't actually be the given string, but instead hashed value
            assertNotEquals("password", user.getPassword(),
                    "Password shouldn't actually be the given string, but instead hashed value");
        }

        AuthenticationResult authResult = _authManager.authenticate(getTestName(), "password");

        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus(),
                "User should authenticate with given password");

        assertEquals(1, _authManager.getChildren(User.class).size(), "Manager should have exactly one user child");
        assertEquals(1, _authManager.getUsers().size(), "Manager should have exactly one user child");


        user.delete();

        assertEquals(0, _authManager.getChildren(User.class).size(),
                "No users should be present after child deletion");


        authResult = _authManager.authenticate(getTestName(), "password");
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, authResult.getStatus(),
                "User should no longer authenticate with given password");
    }

    @Test
    public void testCreateUser()
    {
        assertEquals(0, _authManager.getChildren(User.class).size(),
                "No users should be present before the test starts");
        assertTrue(_authManager.createUser(getTestName(), "password", Map.of()));
        assertEquals(1, _authManager.getChildren(User.class).size(),
                "Manager should have exactly one user child");
        final User<?> user = _authManager.getChildren(User.class).iterator().next();
        assertEquals(getTestName(), user.getName());
        if (!isPlain())
        {
            // password shouldn't actually be the given string, but instead salt and the hashed value
            assertNotEquals("password", user.getPassword(),
                    "Password shouldn't actually be the given string, but instead salt and the hashed value");
        }
        final Map<String, Object> childAttrs = Map.of(User.NAME, getTestName(),
                User.PASSWORD, "password");

        assertThrows(IllegalArgumentException.class,
                () -> _authManager.addChildAsync(User.class, childAttrs).get(),
                "Should not be able to create a second user with the same name");
        assertDoesNotThrow(() -> _authManager.deleteUser(getTestName()),
                "AccountNotFoundException thrown when none was expected");
        assertThrows(AccountNotFoundException.class,
                () -> _authManager.deleteUser(getTestName()),
                "AccountNotFoundException not thrown when none was expected");
    }

    protected abstract boolean isPlain();

    @Test
    public void testUpdateUser()
    {
        assertTrue(_authManager.createUser(getTestName(), "password", Map.of()));
        assertTrue(_authManager.createUser(getTestName() + "_2", "password", Map.of()));
        assertEquals(2, _authManager.getChildren(User.class).size(),
                "Manager should have exactly two user children");

        AuthenticationResult authResult = _authManager.authenticate(getTestName(), "password");

        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus(),
                "User should authenticate with given password");
        authResult = _authManager.authenticate(getTestName()+"_2", "password");
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus(),
                "User should authenticate with given password");

        for (final User<?> user : _authManager.getChildren(User.class))
        {
            if (user.getName().equals(getTestName()))
            {
                user.setAttributes(Map.of(User.PASSWORD, "newpassword"));
            }
        }

        authResult = _authManager.authenticate(getTestName(), "newpassword");
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus(),
                "User should authenticate with updated password");
        authResult = _authManager.authenticate(getTestName()+"_2", "password");
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus(),
                "User should authenticate with original password");

        authResult = _authManager.authenticate(getTestName(), "password");
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, authResult.getStatus(),
                "User not authenticate with original password");

        for (final User<?> user : _authManager.getChildren(User.class))
        {
            if (user.getName().equals(getTestName()))
            {
                user.setPassword("newerpassword");
            }
        }

        authResult = _authManager.authenticate(getTestName(), "newerpassword");
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus(),
                "User should authenticate with updated password");
    }

    @Test
    public void testGetMechanisms()
    {
        assertFalse(_authManager.getMechanisms().isEmpty(), "Should support at least one mechanism");
    }

    @Test
    public void testAuthenticateValidCredentials()
    {
        _authManager.createUser(TEST_USER_NAME, TEST_USER_PASSWORD, Map.of());
        final AuthenticationResult result = _authManager.authenticate(TEST_USER_NAME, TEST_USER_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus(),
                "Unexpected result status");
        assertEquals(TEST_USER_NAME, result.getMainPrincipal().getName(), "Unexpected result principal");
    }

    @Test
    public void testAuthenticateInvalidCredentials()
    {
        _authManager.createUser(TEST_USER_NAME, TEST_USER_PASSWORD, Map.of());
        final AuthenticationResult result = _authManager.authenticate(TEST_USER_NAME, TEST_USER_PASSWORD + "1");
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus(), "Unexpected result status");
        assertNull(result.getMainPrincipal(), "Unexpected result principal");
    }

    @Test
    public void testAllSaslMechanisms()
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn("testhost.example.com");
        for (final String mechanism : _authManager.getMechanisms())
        {
            final SaslNegotiator negotiator = _authManager.createSaslNegotiator(mechanism, saslSettings, null);
            assertNotNull(negotiator, String.format("Could not create SASL negotiator for mechanism '%s'", mechanism));
        }
    }

    @Test
    public void testUnsupportedSaslMechanisms()
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn("testhost.example.com");
        final SaslNegotiator negotiator = _authManager.createSaslNegotiator("UNSUPPORTED MECHANISM", saslSettings, null);
        assertNull(negotiator, "Should not be able to create SASL negotiator for unsupported mechanism");
    }
}
