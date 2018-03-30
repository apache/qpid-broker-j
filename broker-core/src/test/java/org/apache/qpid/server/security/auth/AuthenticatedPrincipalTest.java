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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.Principal;

import javax.security.auth.Subject;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class AuthenticatedPrincipalTest extends UnitTestBase
{

    private AuthenticatedPrincipal _authenticatedPrincipal = new AuthenticatedPrincipal(new UsernamePrincipal("name",
                                                                                                              null));

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
        try
        {
            AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(new Subject());
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException iae)
        {
            // PASS
        }
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
        Subject subjectWithNoPrincipals = new Subject();
        assertNull(AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subjectWithNoPrincipals));

        Subject subjectWithoutAuthenticatedPrincipal = new Subject();
        subjectWithoutAuthenticatedPrincipal.getPrincipals().add(new UsernamePrincipal("name1", null));
        assertNull("Should return null for a subject containing a principal that isn't an AuthenticatedPrincipal",

                          AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subjectWithoutAuthenticatedPrincipal));

    }

    @Test
    public void testTooManyAuthenticatedPrincipalsInSubject()
    {
        final Subject subject = new Subject();
        subject.getPrincipals().add(new AuthenticatedPrincipal(new UsernamePrincipal("name1", null)));
        subject.getPrincipals().add(new AuthenticatedPrincipal(new UsernamePrincipal("name2", null)));

        try
        {
            AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(subject);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException iae)
        {
            // PASS
        }
    }

    private Subject createSubjectContainingAuthenticatedPrincipal()
    {
        final Principal other = new Principal()
        {
            @Override
            public String getName()
            {
                return "otherprincipal";
            }
        };

        final Subject subject = new Subject();
        subject.getPrincipals().add(_authenticatedPrincipal);
        subject.getPrincipals().add(other);
        return subject;
    }

    @Test
    public void testEqualsAndHashcode()
    {
        AuthenticatedPrincipal user1principal1 = new AuthenticatedPrincipal(new UsernamePrincipal("user1", null));
        AuthenticatedPrincipal user1principal2 = new AuthenticatedPrincipal(new UsernamePrincipal("user1", null));

        assertTrue(user1principal1.equals(user1principal1));
        assertTrue(user1principal1.equals(user1principal2));
        assertTrue(user1principal2.equals(user1principal1));

        assertEquals((long) user1principal1.hashCode(), (long) user1principal2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeWithSameWrappedObject()
    {
        UsernamePrincipal wrappedPrincipal = new UsernamePrincipal("user1", null);
        AuthenticatedPrincipal user1principal1 = new AuthenticatedPrincipal(wrappedPrincipal);
        AuthenticatedPrincipal user1principal2 = new AuthenticatedPrincipal(wrappedPrincipal);

        assertTrue(user1principal1.equals(user1principal1));
        assertTrue(user1principal1.equals(user1principal2));
        assertTrue(user1principal2.equals(user1principal1));

        assertEquals((long) user1principal1.hashCode(), (long) user1principal2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentUsernames()
    {
        AuthenticatedPrincipal user1principal1 = new AuthenticatedPrincipal(new UsernamePrincipal("user1", null));
        AuthenticatedPrincipal user1principal2 = new AuthenticatedPrincipal(new UsernamePrincipal("user2", null));

        assertFalse(user1principal1.equals(user1principal2));
        assertFalse(user1principal2.equals(user1principal1));
    }

    @Test
    public void testEqualsWithDissimilarObjects()
    {
        UsernamePrincipal wrappedPrincipal = new UsernamePrincipal("user1", null);
        AuthenticatedPrincipal authenticatedPrincipal = new AuthenticatedPrincipal(wrappedPrincipal);

        assertFalse(authenticatedPrincipal.equals(wrappedPrincipal));
        assertFalse(wrappedPrincipal.equals(authenticatedPrincipal));
    }
}
