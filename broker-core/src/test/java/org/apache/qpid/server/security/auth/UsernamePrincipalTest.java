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
package org.apache.qpid.server.security.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Tests the UsernamePrincipal.
 */
public class UsernamePrincipalTest extends UnitTestBase
{
    @Test
    public void testEqualitySameObject()
    {
        final UsernamePrincipal principal = new UsernamePrincipal("string", null);
        assertEquals(principal, principal);
    }

    @Test
    public void testEqualitySameName()
    {
        final String string = "string";
        final UsernamePrincipal principal1 = new UsernamePrincipal(string, null);
        final UsernamePrincipal principal2 = new UsernamePrincipal(string, null);
        assertEquals(principal1, principal2);
    }

    @Test
    public void testEqualityEqualName()
    {
        final UsernamePrincipal principal1 = new UsernamePrincipal("string", null);
        final UsernamePrincipal principal2 = new UsernamePrincipal("string", null);
        assertEquals(principal1, principal2);
    }

    @Test
    public void testInequalityDifferentUserPrincipals()
    {
        final UsernamePrincipal principal1 = new UsernamePrincipal("string1", null);
        final UsernamePrincipal principal2 = new UsernamePrincipal("string2", null);
        assertNotEquals(principal1, principal2);
    }

    @Test
    public void testInequalityNonUserPrincipal()
    {
        final UsernamePrincipal principal = new UsernamePrincipal("string", null);
        assertNotEquals(principal, "string");
    }

    @Test
    public void testInequalityNull()
    {
        final UsernamePrincipal principal = new UsernamePrincipal("string", null);
        assertNotEquals(null, principal);
    }
}
