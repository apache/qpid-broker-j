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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/*
    Note User is mainly tested by Base64MD5PFPDTest this is just to catch the extra methods
 */
public class HashedUserTest extends UnitTestBase
{
    private final String USERNAME = "username";
    private final String PASSWORD = "password";

    @Test
    public void testToLongArrayConstructor()
    {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new HashedUser(new String[]{USERNAME, PASSWORD, USERNAME}, null),
                "Error expected");
        assertEquals("User Data should be length 2, username, password", thrown.getMessage());
    }

    @Test
    public void testArrayConstructor()
    {
        final String b64EncodedPassword = "cGFzc3dvcmQ=";
        final HashedUser user = new HashedUser(new String[]{USERNAME, b64EncodedPassword}, null);
        assertEquals(USERNAME, user.getName(), "Username incorrect");
        int index = 0;

        char[] hash = b64EncodedPassword.toCharArray();

        try
        {
            for (byte c : user.getEncodedPassword())
            {
                assertEquals(hash[index], (long) (char) c, "Password incorrect");
                index++;
            }
        }
        catch (Exception e)
        {
            fail(e.getMessage());
        }

        hash = PASSWORD.toCharArray();

        index=0;
        for (char c : user.getPassword())
        {
            assertEquals(hash[index], (long) c, "Password incorrect");
            index++;
        }
    }
}

