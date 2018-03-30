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
package org.apache.qpid.server.security.auth.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.security.auth.login.AccountNotFoundException;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.AbstractScramAuthenticationManager;
public class PlainPasswordFilePrincipalDatabaseTest extends AbstractPasswordFilePrincipalDatabaseTest
{


    private Principal _principal = new UsernamePrincipal(TEST_USERNAME, null);
    private PlainPasswordFilePrincipalDatabase _database;
    private List<File> _testPwdFiles = new ArrayList<File>();

    @Before
    public void setUp() throws Exception
    {
        final PasswordCredentialManagingAuthenticationProvider
                mockAuthenticationProvider = mock(PasswordCredentialManagingAuthenticationProvider.class);
        when(mockAuthenticationProvider.getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT)).thenReturn(4096);
        _database = new PlainPasswordFilePrincipalDatabase(mockAuthenticationProvider);
        _testPwdFiles.clear();
    }

    @Override
    protected AbstractPasswordFilePrincipalDatabase getDatabase()
    {
        return _database;
    }

    // ******* Test Methods ********** //

    @Test
    public void testCreatePrincipal()
    {
        File testFile = createPasswordFile(1, 0);

        loadPasswordFile(testFile);

        final String CREATED_PASSWORD = "guest";
        final String CREATED_USERNAME = "createdUser";

        Principal principal = new Principal()
        {
            @Override
            public String getName()
            {
                return CREATED_USERNAME;
            }
        };

        assertTrue("New user not created.", _database.createPrincipal(principal, CREATED_PASSWORD.toCharArray()));


        loadPasswordFile(testFile);

        assertNotNull("Created User was not saved", _database.getUser(CREATED_USERNAME));

        assertFalse("Duplicate user created.",
                           _database.createPrincipal(principal, CREATED_PASSWORD.toCharArray()));


        testFile.delete();
    }

    @Test
    public void testCreatePrincipalIsSavedToFile()
    {

        File testFile = createPasswordFile(1, 0);

        loadPasswordFile(testFile);

        Principal principal = new Principal()
        {
            @Override
            public String getName()
            {
                return TEST_USERNAME;
            }
        };

        _database.createPrincipal(principal, TEST_PASSWORD_CHARS);

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(testFile));

            assertTrue("File has no content", reader.ready());

            assertEquals("Comment line has been corrupted.", TEST_COMMENT, reader.readLine());

            assertTrue("File is missing user data.", reader.ready());

            String userLine = reader.readLine();

            String[] result = Pattern.compile(":").split(userLine);

            assertEquals("User line not complete '" + userLine + "'", (long) 2, (long) result.length);

            assertEquals("Username not correct,", TEST_USERNAME, result[0]);
            assertEquals("Password not correct,", TEST_PASSWORD, result[1]);

            assertFalse("File has more content", reader.ready());
        }
        catch (IOException e)
        {
            fail("Unable to validate file contents due to:" + e.getMessage());
        }
        testFile.delete();
    }
    
    @Test
    public void testDeletePrincipal()
    {
        File testFile = createPasswordFile(1, 1);

        loadPasswordFile(testFile);

        Principal user = _database.getUser(TEST_USERNAME + "0");
        assertNotNull("Generated user not present.", user);

        try
        {
            _database.deletePrincipal(user);
        }
        catch (AccountNotFoundException e)
        {
            fail("User should be present" + e.getMessage());
        }

        try
        {
            _database.deletePrincipal(user);
            fail("User should not be present");
        }
        catch (AccountNotFoundException e)
        {
            //pass
        }

        loadPasswordFile(testFile);

        try
        {
            _database.deletePrincipal(user);
            fail("User should not be present");
        }
        catch (AccountNotFoundException e)
        {
            //pass
        }

        assertNull("Deleted user still present.", _database.getUser(TEST_USERNAME + "0"));

        testFile.delete();
    }

    @Test
    public void testGetUsers()
    {
        int USER_COUNT = 10;
        File testFile = createPasswordFile(1, USER_COUNT);

        loadPasswordFile(testFile);

        Principal user = _database.getUser("MISSING_USERNAME");
        assertNull("Missing user present.", user);

        List<Principal> users = _database.getUsers();

        assertNotNull("Users list is null.", users);

        assertEquals((long) USER_COUNT, (long) users.size());

        boolean[] verify = new boolean[USER_COUNT];
        for (int i = 0; i < USER_COUNT; i++)
        {
            Principal principal = users.get(i);

            assertNotNull("Generated user not present.", principal);

            String name = principal.getName();

            int id = Integer.parseInt(name.substring(TEST_USERNAME.length()));

            assertFalse("Duplicated username retrieve", verify[id]);
            verify[id] = true;
        }

        for (int i = 0; i < USER_COUNT; i++)
        {
            assertTrue("User " + i + " missing", verify[i]);
        }

        testFile.delete();
    }

    @Test
    public void testUpdatePasswordIsSavedToFile()
    {

        File testFile = createPasswordFile(1, 1);

        loadPasswordFile(testFile);

        Principal testUser = _database.getUser(TEST_USERNAME + "0");

        assertNotNull(testUser);

        String NEW_PASSWORD = "NewPassword";
        try
        {
            _database.updatePassword(testUser, NEW_PASSWORD.toCharArray());
        }
        catch (AccountNotFoundException e)
        {
            fail(e.toString());
        }

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(testFile));

            assertTrue("File has no content", reader.ready());

            assertEquals("Comment line has been corrupted.", TEST_COMMENT, reader.readLine());

            assertTrue("File is missing user data.", reader.ready());

            String userLine = reader.readLine();

            String[] result = Pattern.compile(":").split(userLine);

            assertEquals("User line not complete '" + userLine + "'", (long) 2, (long) result.length);

            assertEquals("Username not correct,", TEST_USERNAME + "0", result[0]);
            assertEquals("New Password not correct,", NEW_PASSWORD, result[1]);

            assertFalse("File has more content", reader.ready());
        }
        catch (IOException e)
        {
            fail("Unable to validate file contents due to:" + e.getMessage());
        }
        testFile.delete();
    }

    @Test
    public void testSetPasswordFileWithMissingFile()
    {
        try
        {
            _database.open(new File("DoesntExist"));
        }
        catch (FileNotFoundException fnfe)
        {
            assertTrue(fnfe.getMessage(), fnfe.getMessage().startsWith("Cannot find password file"));
        }
        catch (IOException e)
        {
            fail("Password File was not created." + e.getMessage());
        }

    }

    @Test
    public void testSetPasswordFileWithReadOnlyFile()
    {

        File testFile = createPasswordFile(0, 0);

        testFile.setReadOnly();

        try
        {
            _database.open(testFile);
        }
        catch (FileNotFoundException fnfe)
        {
            assertTrue(fnfe.getMessage().startsWith("Cannot read password file "));
        }
        catch (IOException e)
        {
            fail("Password File was not created." + e.getMessage());
        }

        testFile.delete();
    }
    
    @Test
    public void testCreateUserPrincipal() throws IOException
    {
        Principal newPrincipal = createUserPrincipal();
        assertNotNull(newPrincipal);
        assertEquals(_principal.getName(), newPrincipal.getName());
    }

    private Principal createUserPrincipal()
    {
        File testFile = createPasswordFile(0, 0);
        loadPasswordFile(testFile);

        _database.createPrincipal(_principal, TEST_PASSWORD_CHARS);
        return _database.getUser(TEST_USERNAME);
    }

    @Test
    public void testVerifyPassword() throws IOException, AccountNotFoundException
    {
        createUserPrincipal();
        assertFalse(_database.verifyPassword(TEST_USERNAME, new char[]{}));
        assertFalse(_database.verifyPassword(TEST_USERNAME, "massword".toCharArray()));
        assertTrue(_database.verifyPassword(TEST_USERNAME, TEST_PASSWORD_CHARS));

        try
        {
            _database.verifyPassword("made.up.username", TEST_PASSWORD_CHARS);
            fail("Should not have been able to verify this non-existant users password.");
        }
        catch (AccountNotFoundException e)
        {
            // pass
        }
    }
    
    @Test
    public void testUpdatePassword() throws IOException, AccountNotFoundException
    {
        createUserPrincipal();
        char[] newPwd = "newpassword".toCharArray();
        _database.updatePassword(_principal, newPwd);
        assertFalse(_database.verifyPassword(TEST_USERNAME, TEST_PASSWORD_CHARS));
        assertTrue(_database.verifyPassword(TEST_USERNAME, newPwd));
    }
}
