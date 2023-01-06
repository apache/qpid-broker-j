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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.AccountNotFoundException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.auth.UsernamePrincipal;

public class Base64MD5PasswordFilePrincipalDatabaseTest extends AbstractPasswordFilePrincipalDatabaseTest
{
    private static final String PASSWORD = "guest";
    private static final String PASSWORD_B64MD5HASHED = "CE4DQ6BIb/BVMN9scFyLtA==";
    private static final char[] PASSWORD_MD5_CHARS;
    private static final String PRINCIPAL_USERNAME = "testUserPrincipal";
    private static final Principal PRINCIPAL = new UsernamePrincipal(PRINCIPAL_USERNAME, null);
    private Base64MD5PasswordFilePrincipalDatabase _database;
    private File _pwdFile;

    static
    {
        final byte[] decoded = Base64.getDecoder().decode(PASSWORD_B64MD5HASHED);
        PASSWORD_MD5_CHARS = new char[decoded.length];
        for (int i = 0; i < decoded.length; i++)
        {
            PASSWORD_MD5_CHARS[i] = (char) decoded[i];
        }
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        _database = new Base64MD5PasswordFilePrincipalDatabase(null);
        _pwdFile = File.createTempFile(this.getClass().getName(), "pwd");
        _pwdFile.deleteOnExit();
        _database.open(_pwdFile);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            //clean up the created default password file and any backup
            final File oldPwdFile = new File(_pwdFile.getAbsolutePath() + ".old");
            if (oldPwdFile.exists())
            {
                oldPwdFile.delete();
            }
            _pwdFile.delete();
        }
        finally
        {
            super.tearDown();
        }
    }

    @Override
    protected AbstractPasswordFilePrincipalDatabase<?> getDatabase()
    {
        return _database;
    }

    /** **** Test Methods ************** */

    @Test
    public void testCreatePrincipal()
    {
        final File testFile = createPasswordFile(1, 0);
        loadPasswordFile(testFile);

        final Principal principal = () -> TEST_USERNAME;

        assertTrue(_database.createPrincipal(principal, PASSWORD.toCharArray()), "New user not created.");

        final PasswordCallback callback = new PasswordCallback("prompt",false);
        assertDoesNotThrow(() -> _database.setPassword(principal, callback),
                "User account did not exist");

        final char[] password = callback.getPassword();
        assertArrayEquals(PASSWORD_MD5_CHARS, password, "Password returned was incorrect.");

        loadPasswordFile(testFile);

        assertDoesNotThrow(() -> _database.setPassword(principal, callback),
                "User account did not exist");

        assertArrayEquals(PASSWORD_MD5_CHARS, callback.getPassword(), "Password returned was incorrect.");
        assertNotNull(_database.getUser(TEST_USERNAME), "Created User was not saved");
        assertFalse(_database.createPrincipal(principal, PASSWORD.toCharArray()), "Duplicate user created.");
    }
    
    @Test
    public void testCreatePrincipalIsSavedToFile()
    {
        final File testFile = createPasswordFile(1, 0);
        loadPasswordFile(testFile);
        
        final String CREATED_PASSWORD = "guest";
        final String CREATED_B64MD5HASHED_PASSWORD = "CE4DQ6BIb/BVMN9scFyLtA==";
        final String CREATED_USERNAME = "createdUser";

        final Principal principal = () -> CREATED_USERNAME;

        _database.createPrincipal(principal, CREATED_PASSWORD.toCharArray());

        try (final BufferedReader reader = new BufferedReader(new FileReader(testFile)))
        {
            assertTrue(reader.ready(), "File has no content");
            assertEquals(TEST_COMMENT, reader.readLine(), "Comment line has been corrupted.");
            assertTrue(reader.ready(), "File is missing user data.");

            final String userLine = reader.readLine();
            final String[] result = Pattern.compile(":").split(userLine);

            assertEquals(2, (long) result.length, "User line not complete '" + userLine + "'");
            assertEquals(CREATED_USERNAME, result[0], "Username not correct,");
            assertEquals(CREATED_B64MD5HASHED_PASSWORD, result[1], "Password not correct,");
            assertFalse(reader.ready(), "File has more content");
        }
        catch (IOException e)
        {
            fail("Unable to validate file contents due to:" + e.getMessage());
        }
    }
    

    @Test
    public void testDeletePrincipal()
    {
        final File testFile = createPasswordFile(1, 1);
        loadPasswordFile(testFile);

        final Principal user = _database.getUser(TEST_USERNAME + "0");
        assertNotNull(user, "Generated user not present.");

        assertDoesNotThrow(() -> _database.deletePrincipal(user), "User should be present");
        assertThrows(AccountNotFoundException.class, () -> _database.deletePrincipal(user),
                "User should not be present");

        loadPasswordFile(testFile);

        assertThrows(AccountNotFoundException.class, () -> _database.deletePrincipal(user),
                "User should not be present");
        assertNull(_database.getUser(TEST_USERNAME + "0"), "Deleted user still present.");
    }

    @Test
    public void testGetUsers()
    {
        final int USER_COUNT = 10;
        final File testFile = createPasswordFile(1, USER_COUNT);
        loadPasswordFile(testFile);

        final Principal user = _database.getUser("MISSING_USERNAME");
        assertNull(user, "Missing user present.");

        final List<Principal> users = _database.getUsers();

        assertNotNull(users, "Users list is null.");

        assertEquals(USER_COUNT, (long) users.size());

        final boolean[] verify = new boolean[USER_COUNT];
        for (int i = 0; i < USER_COUNT; i++)
        {
            final Principal principal = users.get(i);

            assertNotNull(principal, "Generated user not present.");

            final String name = principal.getName();

            final int id = Integer.parseInt(name.substring(TEST_USERNAME.length()));

            assertFalse(verify[id], "Duplicated username retrieve");
            verify[id] = true;
        }

        for (int i = 0; i < USER_COUNT; i++)
        {
            assertTrue(verify[i], "User " + i + " missing");
        }
    }

    @Test
    public void testUpdatePasswordIsSavedToFile()
    {
        final File testFile = createPasswordFile(1, 1);
        loadPasswordFile(testFile);

        final Principal testUser = _database.getUser(TEST_USERNAME + "0");

        assertNotNull(testUser);

        final String NEW_PASSWORD = "guest";
        final String NEW_PASSWORD_HASH = "CE4DQ6BIb/BVMN9scFyLtA==";
        assertDoesNotThrow(() -> _database.updatePassword(testUser, NEW_PASSWORD.toCharArray()));

        try (final BufferedReader reader = new BufferedReader(new FileReader(testFile)))
        {
            assertTrue(reader.ready(), "File has no content");
            assertEquals(TEST_COMMENT, reader.readLine(), "Comment line has been corrupted.");
            assertTrue(reader.ready(), "File is missing user data.");

            final String userLine = reader.readLine();
            final String[] result = Pattern.compile(":").split(userLine);

            assertEquals(2, (long) result.length, "User line not complete '" + userLine + "'");
            assertEquals(TEST_USERNAME + "0", result[0], "Username not correct,");
            assertEquals(NEW_PASSWORD_HASH, result[1], "New Password not correct,");
            assertFalse(reader.ready(), "File has more content");
        }
        catch (IOException e)
        {
            fail("Unable to validate file contents due to:" + e.getMessage());
        }
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
            assertTrue(fnfe.getMessage().startsWith("Cannot find password file"), fnfe.getMessage());
        }
        catch (IOException e)
        {
            fail("Password File was not created." + e.getMessage());
        }
    }

    @Test
    public void testSetPasswordFileWithReadOnlyFile()
    {
        final File testFile = createPasswordFile(0, 0);
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
    }
    
    @Test
    public void testCreateUserPrincipal()
    {
        _database.createPrincipal(PRINCIPAL, PASSWORD.toCharArray());
        final Principal newPrincipal = _database.getUser(PRINCIPAL_USERNAME);
        assertNotNull(newPrincipal);
        assertEquals(PRINCIPAL.getName(), newPrincipal.getName());
    }
    
    @Test
    public void testVerifyPassword() throws AccountNotFoundException
    {
        testCreateUserPrincipal();
        //assertFalse(_pwdDB.verifyPassword(_username, null));
        assertFalse(_database.verifyPassword(PRINCIPAL_USERNAME, new char[]{}));
        assertFalse(_database.verifyPassword(PRINCIPAL_USERNAME, (PASSWORD + "z").toCharArray()));
        assertTrue(_database.verifyPassword(PRINCIPAL_USERNAME, PASSWORD.toCharArray()));

        assertThrows(AccountNotFoundException.class,
                () -> _database.verifyPassword("made.up.username", PASSWORD.toCharArray()),
                "Should not have been able to verify this nonexistent users password.");
    }
    
    @Test
    public void testUpdatePassword() throws AccountNotFoundException
    {
        testCreateUserPrincipal();
        final char[] newPwd = "newpassword".toCharArray();
        _database.updatePassword(PRINCIPAL, newPwd);
        assertFalse(_database.verifyPassword(PRINCIPAL_USERNAME, PASSWORD.toCharArray()));
        assertTrue(_database.verifyPassword(PRINCIPAL_USERNAME, newPwd));
    }
}
