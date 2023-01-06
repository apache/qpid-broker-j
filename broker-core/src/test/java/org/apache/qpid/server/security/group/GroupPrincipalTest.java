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
package org.apache.qpid.server.security.group;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class GroupPrincipalTest extends UnitTestBase
{
    @Test
    public void testGetName()
    {
        final GroupPrincipal principal = new GroupPrincipal("group", null);
        assertEquals("group", principal.getName());
    }

    @Test
    public void testAddRejected()
    {
        final GroupPrincipal principal = new GroupPrincipal("group", null);
        final UsernamePrincipal user = new UsernamePrincipal("name", null);

        assertThrows(UnsupportedOperationException.class,
                () -> principal.addMember(user),
                "Exception not thrown");
    }

    @Test
    public void testEqualitySameName()
    {
        final String string = "string";
        final GroupPrincipal principal1 = new GroupPrincipal(string, null);
        final GroupPrincipal principal2 = new GroupPrincipal(string, null);
        assertEquals(principal1, principal2);
    }

    @Test
    public void testEqualityEqualName()
    {
        final GroupPrincipal principal1 = new GroupPrincipal(new String("string"), null);
        final GroupPrincipal principal2 = new GroupPrincipal(new String("string"), null);
        assertEquals(principal1, principal2);
    }

    @Test
    public void testInequalityDifferentGroupPrincipals()
    {
        final GroupPrincipal principal1 = new GroupPrincipal("string1", null);
        final GroupPrincipal principal2 = new GroupPrincipal("string2", null);
        assertNotEquals(principal1, principal2);
    }

    @Test
    public void testInequalityNonGroupPrincipal()
    {
        final GroupPrincipal principal = new GroupPrincipal("string", null);
        assertNotEquals(principal, new UsernamePrincipal("string", null));
    }

    @Test
    public void testInequalityNull()
    {
        final GroupPrincipal principal = new GroupPrincipal("string", null);
        assertNotEquals(null, principal);
    }
}
