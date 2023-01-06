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
package org.apache.qpid.server.security.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.Principal;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class AuthenticatedPrincipalTest extends UnitTestBase
{
    private final AuthenticatedPrincipal _authenticatedPrincipal =
            new AuthenticatedPrincipal(new UsernamePrincipal("name", null));

    @Test
    public void testGetAuthenticatedPrincipalFromSubject()
    {
        final Subject subject = createSubjectContainingAuthenticatedPrincipal();
        final AuthenticatedPrincipal actual = AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(subject);
        assertSame(_authenticatedPrincipal, actual);
    }

    @Test
    public void testAuthenticatedPrincipalNotInSubject()
    {
        assertThrows(IllegalArgumentException.class,
                () -> AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(new Subject()),
                "Exception not thrown");
    }

    @Test
    public void testGetOptionalAuthenticatedPrincipalFromSubject()
    {
        final Subject subject = createSubjectContainingAuthenticatedPrincipal();
        final AuthenticatedPrincipal actual = AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subject);

        assertSame(_authenticatedPrincipal, actual);
    }

    @Test
    public void testGetOptionalAuthenticatedPrincipalFromSubjectReturnsNullIfMissing()
    {
        final Subject subjectWithNoPrincipals = new Subject();
        assertNull(AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subjectWithNoPrincipals));

        final Subject subjectWithoutAuthenticatedPrincipal = new Subject();
        subjectWithoutAuthenticatedPrincipal.getPrincipals().add(new UsernamePrincipal("name1", null));
        assertNull(AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subjectWithoutAuthenticatedPrincipal),
                "Should return null for a subject containing a principal that isn't an AuthenticatedPrincipal");

    }

    @Test
    public void testTooManyAuthenticatedPrincipalsInSubject()
    {
        final Subject subject = new Subject();
        subject.getPrincipals().add(new AuthenticatedPrincipal(new UsernamePrincipal("name1", null)));
        subject.getPrincipals().add(new AuthenticatedPrincipal(new UsernamePrincipal("name2", null)));

        assertThrows(IllegalArgumentException.class,
                () -> AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(subject),
                "Exception not thrown");
    }

    private Subject createSubjectContainingAuthenticatedPrincipal()
    {
        final Principal other = () -> "otherprincipal";
        final Subject subject = new Subject();
        subject.getPrincipals().add(_authenticatedPrincipal);
        subject.getPrincipals().add(other);
        return subject;
    }

    @Test
    public void testEqualsAndHashcode()
    {
        final AuthenticatedPrincipal user1principal1 = new AuthenticatedPrincipal(new UsernamePrincipal("user1", null));
        final AuthenticatedPrincipal user1principal2 = new AuthenticatedPrincipal(new UsernamePrincipal("user1", null));

        assertEquals(user1principal1, user1principal1);
        assertEquals(user1principal1, user1principal2);
        assertEquals(user1principal2, user1principal1);
        assertEquals(user1principal1.hashCode(), (long) user1principal2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeWithSameWrappedObject()
    {
        final UsernamePrincipal wrappedPrincipal = new UsernamePrincipal("user1", null);
        final AuthenticatedPrincipal user1principal1 = new AuthenticatedPrincipal(wrappedPrincipal);
        final AuthenticatedPrincipal user1principal2 = new AuthenticatedPrincipal(wrappedPrincipal);

        assertEquals(user1principal1, user1principal1);
        assertEquals(user1principal1, user1principal2);
        assertEquals(user1principal2, user1principal1);
        assertEquals(user1principal1.hashCode(), (long) user1principal2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentUsernames()
    {
        final AuthenticatedPrincipal user1principal1 = new AuthenticatedPrincipal(new UsernamePrincipal("user1", null));
        final AuthenticatedPrincipal user1principal2 = new AuthenticatedPrincipal(new UsernamePrincipal("user2", null));

        assertNotEquals(user1principal1, user1principal2);
        assertNotEquals(user1principal2, user1principal1);
    }

    @Test
    public void testEqualsWithDissimilarObjects()
    {
        final UsernamePrincipal wrappedPrincipal = new UsernamePrincipal("user1", null);
        final AuthenticatedPrincipal authenticatedPrincipal = new AuthenticatedPrincipal(wrappedPrincipal);

        assertNotEquals(authenticatedPrincipal, wrappedPrincipal);
        assertNotEquals(wrappedPrincipal, authenticatedPrincipal);
    }
}
