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

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.security.auth.login.AccountNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
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


    private Broker _broker;
    private TaskExecutor _executor;

    @Before
    public void setUp() throws Exception
    {
        _executor = new CurrentThreadTaskExecutor();
        _executor.start();
        _broker = BrokerTestHelper.createBrokerMock();
        when(_broker.getTaskExecutor()).thenReturn(_executor);
        when(_broker.getChildExecutor()).thenReturn(_executor);
        final Map<String, Object> attributesMap = new HashMap<String, Object>();
        attributesMap.put(AuthenticationProvider.NAME, getTestName());
        attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
        _authManager = createAuthManager(attributesMap);
        _authManager.open();
    }


    @After
    public void tearDown() throws Exception
    {
        _executor.stop();
    }

    protected abstract ConfigModelPasswordManagingAuthenticationProvider createAuthManager(final Map<String, Object> attributesMap);

    public Broker getBroker()
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
        assertFalse("PLAIN authentication should not be available on an insecure connection", _authManager.getAvailableMechanisms(false).contains("PLAIN"));
        assertTrue("PLAIN authentication should be available on a secure connection", _authManager.getAvailableMechanisms(true).contains("PLAIN"));
    }

    @Test
    public void testAddChildAndThenDelete() throws ExecutionException, InterruptedException
    {
        // No children should be present before the test starts
        assertEquals("No users should be present before the test starts", 0, _authManager.getChildren(User.class).size());
        assertEquals("No users should be present before the test starts", 0, _authManager.getUsers().size());

        final Map<String, Object> childAttrs = new HashMap<String, Object>();

        childAttrs.put(User.NAME, getTestName());
        childAttrs.put(User.PASSWORD, "password");
        User user = _authManager.addChildAsync(User.class, childAttrs).get();
        assertNotNull("User should be created but addChild returned null", user);
        assertEquals(getTestName(), user.getName());
        if(!isPlain())
        {
            // password shouldn't actually be the given string, but instead hashed value
            assertFalse("Password shouldn't actually be the given string, but instead hashed value",
                        "password".equals(user.getPassword()));
        }

        AuthenticationResult authResult =
                _authManager.authenticate(getTestName(), "password");

        assertEquals("User should authenticate with given password", AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus());

        assertEquals("Manager should have exactly one user child",1, _authManager.getChildren(User.class).size());
        assertEquals("Manager should have exactly one user child",1, _authManager.getUsers().size());


        user.delete();

        assertEquals("No users should be present after child deletion", 0, _authManager.getChildren(User.class).size());


        authResult = _authManager.authenticate(getTestName(), "password");
        assertEquals("User should no longer authenticate with given password", AuthenticationResult.AuthenticationStatus.ERROR, authResult.getStatus());

    }

    @Test
    public void testCreateUser() throws ExecutionException, InterruptedException
    {
        assertEquals("No users should be present before the test starts", 0, _authManager.getChildren(User.class).size());
        assertTrue(_authManager.createUser(getTestName(), "password", Collections.<String, String>emptyMap()));
        assertEquals("Manager should have exactly one user child",1, _authManager.getChildren(User.class).size());
        User user = _authManager.getChildren(User.class).iterator().next();
        assertEquals(getTestName(), user.getName());
        if(!isPlain())
        {
            // password shouldn't actually be the given string, but instead salt and the hashed value
            assertFalse("Password shouldn't actually be the given string, but instead salt and the hashed value",
                        "password".equals(user.getPassword()));
        }
        final Map<String, Object> childAttrs = new HashMap<String, Object>();

        childAttrs.put(User.NAME, getTestName());
        childAttrs.put(User.PASSWORD, "password");
        try
        {
            user = _authManager.addChildAsync(User.class, childAttrs).get();
            fail("Should not be able to create a second user with the same name");
        }
        catch(IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            _authManager.deleteUser(getTestName());
        }
        catch (AccountNotFoundException e)
        {
            fail("AccountNotFoundException thrown when none was expected: " + e.getMessage());
        }
        try
        {
            _authManager.deleteUser(getTestName());
            fail("AccountNotFoundException not thrown when was expected");
        }
        catch (AccountNotFoundException e)
        {
            // pass
        }
    }

    protected abstract boolean isPlain();

    @Test
    public void testUpdateUser()
    {
        assertTrue(_authManager.createUser(getTestName(), "password", Collections.<String, String>emptyMap()));
        assertTrue(_authManager.createUser(getTestName()+"_2", "password", Collections.<String, String>emptyMap()));
        assertEquals("Manager should have exactly two user children",2, _authManager.getChildren(User.class).size());

        AuthenticationResult authResult = _authManager.authenticate(getTestName(), "password");

        assertEquals("User should authenticate with given password", AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus());
        authResult = _authManager.authenticate(getTestName()+"_2", "password");
        assertEquals("User should authenticate with given password", AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus());

        for(User user : _authManager.getChildren(User.class))
        {
            if(user.getName().equals(getTestName()))
            {
                user.setAttributes(Collections.singletonMap(User.PASSWORD, "newpassword"));
            }
        }

        authResult = _authManager.authenticate(getTestName(), "newpassword");
        assertEquals("User should authenticate with updated password", AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus());
        authResult = _authManager.authenticate(getTestName()+"_2", "password");
        assertEquals("User should authenticate with original password", AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus());

        authResult = _authManager.authenticate(getTestName(), "password");
        assertEquals("User not authenticate with original password", AuthenticationResult.AuthenticationStatus.ERROR, authResult.getStatus());

        for(User user : _authManager.getChildren(User.class))
        {
            if(user.getName().equals(getTestName()))
            {
                user.setPassword("newerpassword");
            }
        }

        authResult = _authManager.authenticate(getTestName(), "newerpassword");
        assertEquals("User should authenticate with updated password", AuthenticationResult.AuthenticationStatus.SUCCESS, authResult.getStatus());
    }

    @Test
    public void testGetMechanisms() throws Exception
    {
        assertFalse("Should support at least one mechanism", _authManager.getMechanisms().isEmpty());
    }

    @Test
    public void testAuthenticateValidCredentials() throws Exception
    {
        _authManager.createUser(TEST_USER_NAME, TEST_USER_PASSWORD, Collections.<String, String>emptyMap());
        AuthenticationResult result = _authManager.authenticate(TEST_USER_NAME, TEST_USER_PASSWORD);
        assertEquals("Unexpected result status", AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals("Unexpected result principal", TEST_USER_NAME, result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticateInvalidCredentials() throws Exception
    {
        _authManager.createUser(TEST_USER_NAME, TEST_USER_PASSWORD, Collections.<String, String>emptyMap());
        AuthenticationResult result = _authManager.authenticate(TEST_USER_NAME, TEST_USER_PASSWORD + "1");
        assertEquals("Unexpected result status", AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
        assertNull("Unexpected result principal", result.getMainPrincipal());
    }

    @Test
    public void testAllSaslMechanisms() throws Exception
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn("testhost.example.com");
        for (String mechanism : _authManager.getMechanisms())
        {
            final SaslNegotiator negotiator = _authManager.createSaslNegotiator(mechanism, saslSettings, null);
            assertNotNull(String.format("Could not create SASL negotiator for mechanism '%s'", mechanism), negotiator);
        }
    }

    @Test
    public void testUnsupportedSaslMechanisms() throws Exception
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn("testhost.example.com");
        final SaslNegotiator negotiator = _authManager.createSaslNegotiator("UNSUPPORTED MECHANISM", saslSettings, null);
        assertNull("Should not be able to create SASL negotiator for unsupported mechanism", negotiator);
    }

}
