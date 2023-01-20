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

package org.apache.qpid.server.security.auth.database;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public abstract class AbstractPasswordFilePrincipalDatabaseTest extends UnitTestBase
{
    protected static final String TEST_COMMENT = "# Test Comment";
    protected static final String TEST_USERNAME = "testUser";
    protected static final String TEST_PASSWORD = "testPassword";
    protected static final char[] TEST_PASSWORD_CHARS = TEST_PASSWORD.toCharArray();

    private final List<File> _testPwdFiles = new ArrayList<>();
    private final Principal _principal = new UsernamePrincipal(TEST_USERNAME, null);

    protected abstract AbstractPasswordFilePrincipalDatabase<?> getDatabase();

    @AfterEach
    public void tearDown() throws Exception
    {
        //clean up any additional files and their backups
        for (final File f : _testPwdFiles)
        {
            final File oldPwdFile = new File(f.getAbsolutePath() + ".old");
            if (oldPwdFile.exists())
            {
                oldPwdFile.delete();
            }

            f.delete();
        }
    }

    protected File createPasswordFile(final int commentLines, final int users)
    {
        try
        {
            final File testFile = File.createTempFile(getTestName(), "tmp");
            testFile.deleteOnExit();

            try (final BufferedWriter writer = new BufferedWriter(new FileWriter(testFile)))
            {

                for (int i = 0; i < commentLines; i++)
                {
                    writer.write(TEST_COMMENT);
                    writer.newLine();
                }

                for (int i = 0; i < users; i++)
                {
                    writer.write(TEST_USERNAME + i + ":Password");
                    writer.newLine();
                }

                writer.flush();
            }

            _testPwdFiles.add(testFile);

            return testFile;

        }
        catch (IOException e)
        {
            fail("Unable to create test password file." + e.getMessage());
        }

        return null;
    }


    protected void loadPasswordFile(final File file)
    {
        assertDoesNotThrow(() -> getDatabase().open(file), "Password File was not created");
    }


    @Test
    public void testRejectUsernameWithColon()
    {
        final String usernameWithColon = "user:name";
        final Principal principal = new UsernamePrincipal(usernameWithColon, null);
        final File testFile = createPasswordFile(0, 0);
        loadPasswordFile(testFile);

        assertThrows(IllegalArgumentException.class,
                () -> getDatabase().createPrincipal(principal, TEST_PASSWORD_CHARS),
                "Username with colon should be rejected");
    }

    @Test
    public void testRejectPasswordWithColon()
    {
        final String username = "username";
        final String passwordWithColon = "pass:word";
        final Principal principal = new UsernamePrincipal(username, null);
        final File testFile = createPasswordFile(0, 0);
        loadPasswordFile(testFile);

        assertThrows(IllegalArgumentException.class,
                () -> getDatabase().createPrincipal(principal, passwordWithColon.toCharArray()),
                "Password with colon should be rejected");

        getDatabase().createPrincipal(_principal, TEST_PASSWORD_CHARS);

        assertThrows(
                IllegalArgumentException.class,
                () -> getDatabase().updatePassword(_principal, passwordWithColon.toCharArray()),
                "Password with colon should be rejected");
    }

}
