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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private final SaslNegotiator _saslNegotiator = mock(SaslNegotiator.class);

    private PrincipalDatabaseAuthenticationManager<?> _manager = null; // Class under test
    private PrincipalDatabase _principalDatabase;
    private String _passwordFileLocation;

    @BeforeEach
    public void setUp() throws Exception
    {
        _passwordFileLocation = TMP_FOLDER + File.separator +
                PrincipalDatabaseAuthenticationManagerTest.class.getSimpleName() + "-" + getTestName();
        deletePasswordFileIfExists();
    }

    @AfterEach
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

    private void setupMocks()
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
        final Map<String,Object> attrs = Map.of(ConfiguredObject.ID, randomUUID(),
                ConfiguredObject.NAME, getTestName(),
                "path", _passwordFileLocation);
        _manager = getPrincipalDatabaseAuthenticationManager(attrs);
        if (recovering)
        {
            _manager.open();
        }
        else
        {
            _manager.create();
        }
    }

    @Test
    public void testInitialiseWhenPasswordFileNotFound()
    {
        final PasswordCredentialManagingAuthenticationProvider<?> mockAuthProvider =
                mock(PasswordCredentialManagingAuthenticationProvider.class);
        when(mockAuthProvider.getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT)).thenReturn(4096);
        _principalDatabase = new PlainPasswordFilePrincipalDatabase(mockAuthProvider);
        setupManager(true);

        final IllegalConfigurationException thrown = assertThrows(
                IllegalConfigurationException.class,
                () -> _manager.initialise(),
                "Initialization should fail when users file does not exist");
        assertTrue(thrown.getCause() instanceof FileNotFoundException);
    }

    @Test
    public void testInitialiseWhenPasswordFileExists() throws Exception
    {
        final PasswordCredentialManagingAuthenticationProvider<?> mockAuthProvider =
                mock(PasswordCredentialManagingAuthenticationProvider.class);
        when(mockAuthProvider.getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT)).thenReturn(4096);
        _principalDatabase = new PlainPasswordFilePrincipalDatabase(mockAuthProvider);
        setupManager(true);

        final File file = new File(_passwordFileLocation);
        if (file.createNewFile())
        {
            try (final FileOutputStream fos = new FileOutputStream(file))
            {
                fos.write("admin:admin".getBytes());
            }
        }
        _manager.initialise();
        final List<Principal> users = _principalDatabase.getUsers();
        assertEquals(1, (long) users.size(), "Unexpected uses size");
        final Principal p = _principalDatabase.getUser("admin");
        assertEquals("admin", p.getName(), "Unexpected principal name");
    }

    @Test
    public void testSaslMechanismCreation()
    {
        setupMocks();

        final SaslSettings saslSettings = mock(SaslSettings.class);
        final SaslNegotiator saslNegotiator = _manager.createSaslNegotiator(MOCK_MECH_NAME, saslSettings, null);
        assertNotNull(saslNegotiator);
    }

    /**
     * Tests that the authenticate method correctly interprets an
     * authentication success.
     */
    @Test
    public void testSaslAuthenticationSuccess()
    {
        setupMocks();
        final UsernamePrincipal expectedPrincipal = new UsernamePrincipal("guest", _manager);

        when(_saslNegotiator.handleResponse(any(byte[].class))).thenReturn(new AuthenticationResult(expectedPrincipal));

        final AuthenticationResult result = _saslNegotiator.handleResponse("12345".getBytes());
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus());
    }

    /**
     * Tests that the authenticate method correctly interprets an
     * authentication not complete.
     */
    @Test
    public void testSaslAuthenticationNotCompleted()
    {
        setupMocks();
        when(_saslNegotiator.handleResponse(any(byte[].class))).thenReturn(new AuthenticationResult(AuthenticationStatus.CONTINUE));

        final AuthenticationResult result = _saslNegotiator.handleResponse("12345".getBytes());
        assertEquals(0, (long) result.getPrincipals().size(), "Principals was not expected size");
        assertEquals(AuthenticationStatus.CONTINUE, result.getStatus());
    }

    /**
     * Tests that the authenticate method correctly interprets an
     * authentication error.
     */
    @Test
    public void testSaslAuthenticationError()
    {
        setupMocks();
        when(_saslNegotiator.handleResponse(any(byte[].class))).thenReturn(new AuthenticationResult(AuthenticationStatus.ERROR));

        final AuthenticationResult result = _saslNegotiator.handleResponse("12345".getBytes());
        assertEquals(0, (long) result.getPrincipals().size(), "Principals was not expected size");
        assertEquals(AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testNonSaslAuthenticationSuccess() throws Exception
    {
        setupMocks();
        when(_principalDatabase.verifyPassword("guest", "guest".toCharArray())).thenReturn(true);

        final AuthenticationResult result = _manager.authenticate("guest", "guest");
        final UsernamePrincipal expectedPrincipal = new UsernamePrincipal("guest", _manager);
        assertOnlyContainsWrapped(expectedPrincipal, result.getPrincipals());
        assertEquals(AuthenticationStatus.SUCCESS, result.getStatus());
    }

    @Test
    public void testNonSaslAuthenticationErrored() throws Exception
    {
        setupMocks();
        when(_principalDatabase.verifyPassword("guest", "wrongpassword".toCharArray())).thenReturn(false);

        final AuthenticationResult result = _manager.authenticate("guest", "wrongpassword");
        assertEquals(0, (long) result.getPrincipals().size(), "Principals was not expected size");
        assertEquals(AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testOnCreate()
    {
        setupMocks();
        assertTrue(new File(_passwordFileLocation).exists(), "Password file was not created");
    }

    @Test
    public void testOnDelete()
    {
        setupMocks();
        assertTrue(new File(_passwordFileLocation).exists(), "Password file was not created");

        _manager.delete();
        assertFalse(new File(_passwordFileLocation).exists(), "Password file was not deleted");
    }

    @Test
    public void testCreateForInvalidPath()
    {
        setUpPrincipalDatabase();

        final String path = TMP_FOLDER + File.separator + getTestName() + System.nanoTime() + File.separator + "users";
        final Map<String,Object> attrs = Map.of(ConfiguredObject.ID, randomUUID(),
                ConfiguredObject.NAME, getTestName(),
                "path", path);

        _manager = getPrincipalDatabaseAuthenticationManager(attrs);

        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> _manager.create(), "Creation with invalid path should have failed");
        assertEquals(String.format("Cannot create password file at '%s'", path), thrown.getMessage(),
                "Unexpected exception message:" + thrown.getMessage());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    final PrincipalDatabaseAuthenticationManager<?> getPrincipalDatabaseAuthenticationManager(final Map<String, Object> attrs)
    {
        return new PrincipalDatabaseAuthenticationManager(attrs, BrokerTestHelper.createNewBrokerMock())
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
        final File passwordFile = new File(_passwordFileLocation);
        if (passwordFile.exists())
        {
            passwordFile.delete();
        }
    }
}
