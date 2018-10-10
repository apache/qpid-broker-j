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

import static org.apache.qpid.server.security.auth.AuthenticatedPrincipalTestHelper.assertOnlyContainsWrapped;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.database.PlainPasswordFilePrincipalDatabase;
import org.apache.qpid.server.security.auth.database.PrincipalDatabase;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * Tests the public methods of PrincipalDatabaseAuthenticationManager.
 */
public class PrincipalDatabaseAuthenticationManagerTest extends UnitTestBase
{
    private static final String MOCK_MECH_NAME = "MOCK-MECH-NAME";

    private PrincipalDatabaseAuthenticationManager _manager = null; // Class under test
    private PrincipalDatabase _principalDatabase;
    private String _passwordFileLocation;
    private SaslNegotiator _saslNegotiator = mock(SaslNegotiator.class);

    @Before
    public void setUp() throws Exception
    {
        _passwordFileLocation = TMP_FOLDER + File.separator + PrincipalDatabaseAuthenticationManagerTest.class.getSimpleName() + "-" + getTestName();
        deletePasswordFileIfExists();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_manager != null)
            {
                _manager.close();
            }
        }
        finally
        {
            deletePasswordFileIfExists();
        }
    }

    private void setupMocks() throws Exception
    {
        setUpPrincipalDatabase();

        setupManager(false);

        _manager.initialise();
    }

    private void setUpPrincipalDatabase()
    {
        _principalDatabase = mock(PrincipalDatabase.class);

        when(_principalDatabase.getMechanisms()).thenReturn(Collections.singletonList(MOCK_MECH_NAME));
        when(_principalDatabase.createSaslNegotiator(eq(MOCK_MECH_NAME), any(SaslSettings.class))).thenReturn(
                _saslNegotiator);
    }

    private void setupManager(final boolean recovering)
    {
        Map<String,Object> attrs = new HashMap<String, Object>();
        attrs.put(ConfiguredObject.ID, UUID.randomUUID());
        attrs.put(ConfiguredObject.NAME, getTestName());
        attrs.put("path", _passwordFileLocation);
        _manager = getPrincipalDatabaseAuthenticationManager(attrs);
        if(recovering)
        {
            _manager.open();
        }
        else
        {
            _manager.create();
        }
    }

    @Test
    public void testInitialiseWhenPasswordFileNotFound() throws Exception
    {
        PasswordCredentialManagingAuthenticationProvider mockAuthProvider = mock(PasswordCredentialManagingAuthenticationProvider.class);
        when(mockAuthProvider.getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT)).thenReturn(4096);
        _principalDatabase = new PlainPasswordFilePrincipalDatabase(mockAuthProvider);
        setupManager(true);
        try
        {

            _manager.initialise();
            fail("Initialisiation should fail when users file does not exist");
        }
        catch (IllegalConfigurationException e)
        {
            final boolean condition = e.getCause() instanceof FileNotFoundException;
            assertTrue(condition);
        }
    }

    @Test
    public void testInitialiseWhenPasswordFileExists() throws Exception
    {
        PasswordCredentialManagingAuthenticationProvider mockAuthProvider = mock(PasswordCredentialManagingAuthenticationProvider.class);
        when(mockAuthProvider.getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT)).thenReturn(4096);
        _principalDatabase = new PlainPasswordFilePrincipalDatabase(mockAuthProvider);
        setupManager(true);

        File f = new File(_passwordFileLocation);
        f.createNewFile();
        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(f);
            fos.write("admin:admin".getBytes());
        }
        finally
        {
            if (fos != null)
            {
                fos.close();
            }
        }
        _manager.initialise();
        List<Principal> users = _principalDatabase.getUsers();
        assertEquals("Unexpected uses size", (long) 1, (long) users.size());
        Principal p = _principalDatabase.getUser("admin");
        assertEquals("Unexpected principal name", "admin", p.getName());
    }

    @Test
    public void testSaslMechanismCreation() throws Exception
    {
        setupMocks();

        SaslSettings saslSettings = mock(SaslSettings.class);
        SaslNegotiator saslNegotiator = _manager.createSaslNegotiator(MOCK_MECH_NAME, saslSettings, null);
        assertNotNull(saslNegotiator);
    }

    /**
     * Tests that the authenticate method correctly interprets an
     * authentication success.
     *
     */
    @Test
    public void testSaslAuthenticationSuccess() throws Exception
    {
        setupMocks();
        UsernamePrincipal expectedPrincipal = new UsernamePrincipal("guest", _manager);

        when(_saslNegotiator.handleResponse(any(byte[].class))).thenReturn(new AuthenticationResult(expectedPrincipal));

        AuthenticationResult result = _saslNegotiator.handleResponse("12345".getBytes());

        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus());
    }

    /**
     *
     * Tests that the authenticate method correctly interprets an
     * authentication not complete.
     *
     */
    @Test
    public void testSaslAuthenticationNotCompleted() throws Exception
    {
        setupMocks();

        when(_saslNegotiator.handleResponse(any(byte[].class))).thenReturn(new AuthenticationResult(AuthenticationStatus.CONTINUE));

        AuthenticationResult result = _saslNegotiator.handleResponse("12345".getBytes());
        assertEquals("Principals was not expected size", (long) 0, (long) result.getPrincipals().size());

        assertEquals(AuthenticationStatus.CONTINUE, result.getStatus());
    }

    /**
     *
     * Tests that the authenticate method correctly interprets an
     * authentication error.
     *
     */
    @Test
    public void testSaslAuthenticationError() throws Exception
    {
        setupMocks();

        when(_saslNegotiator.handleResponse(any(byte[].class))).thenReturn(new AuthenticationResult(AuthenticationStatus.ERROR));

        AuthenticationResult result = _saslNegotiator.handleResponse("12345".getBytes());
        assertEquals("Principals was not expected size", (long) 0, (long) result.getPrincipals().size());
        assertEquals(AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testNonSaslAuthenticationSuccess() throws Exception
    {
        setupMocks();

        when(_principalDatabase.verifyPassword("guest", "guest".toCharArray())).thenReturn(true);

        AuthenticationResult result = _manager.authenticate("guest", "guest");

        UsernamePrincipal expectedPrincipal = new UsernamePrincipal("guest", _manager);
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus());
    }

    @Test
    public void testNonSaslAuthenticationErrored() throws Exception
    {
        setupMocks();

        when(_principalDatabase.verifyPassword("guest", "wrongpassword".toCharArray())).thenReturn(false);

        AuthenticationResult result = _manager.authenticate("guest", "wrongpassword");
        assertEquals("Principals was not expected size", (long) 0, (long) result.getPrincipals().size());
        assertEquals(AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testOnCreate() throws Exception
    {
        setupMocks();

        assertTrue("Password file was not created", new File(_passwordFileLocation).exists());
    }

    @Test
    public void testOnDelete() throws Exception
    {
        setupMocks();

        assertTrue("Password file was not created", new File(_passwordFileLocation).exists());

        _manager.delete();
        assertFalse("Password file was not deleted", new File(_passwordFileLocation).exists());
    }

    @Test
    public void testCreateForInvalidPath() throws Exception
    {
        setUpPrincipalDatabase();

        Map<String,Object> attrs = new HashMap<>();
        attrs.put(ConfiguredObject.ID, UUID.randomUUID());
        attrs.put(ConfiguredObject.NAME, getTestName());
        String path = TMP_FOLDER + File.separator + getTestName() + System.nanoTime() + File.separator + "users";
        attrs.put("path", path);

        _manager = getPrincipalDatabaseAuthenticationManager(attrs);
        try
        {
            _manager.create();
            fail("Creation with invalid path should have failed");
        }
        catch(IllegalConfigurationException e)
        {
            assertEquals("Unexpected exception message:" + e.getMessage(),
                                String.format("Cannot create password file at '%s'", path),
                                e.getMessage());

        }
    }

    PrincipalDatabaseAuthenticationManager getPrincipalDatabaseAuthenticationManager(final Map<String, Object> attrs)
    {
        return new PrincipalDatabaseAuthenticationManager(attrs, BrokerTestHelper.createBrokerMock())
        {
            @Override
            protected PrincipalDatabase createDatabase()
            {
                return _principalDatabase;
            }

        };
    }

    private void deletePasswordFileIfExists()
    {
        File passwordFile = new File(_passwordFileLocation);
        if (passwordFile.exists())
        {
            passwordFile.delete();
        }
    }
}
