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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.security.group.GroupPrincipal;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestPrincipalUtils
{
    public static final String TEST_AUTH_PROVIDER_TYPE = "TestAuthProviderType";
    public static final String TEST_AUTH_PROVIDER_NAME = "testAuthProvider";
    private static final AuthenticationProvider TEST_AUTH_PROVIDER = mock(AuthenticationProvider.class);

    static
    {
        when(TEST_AUTH_PROVIDER.getType()).thenReturn(TEST_AUTH_PROVIDER_TYPE);
        when(TEST_AUTH_PROVIDER.getName()).thenReturn(TEST_AUTH_PROVIDER_NAME);
    }

    /**
     * Creates a test subject, with exactly one {@link AuthenticatedPrincipal} and zero or more GroupPrincipals.
     */
    public static Subject createTestSubject(final String username, final String... groups)
    {
        final Set<Principal> principals = new HashSet<Principal>(1 + groups.length);
        principals.add(new AuthenticatedPrincipal(new UsernamePrincipal(username, TEST_AUTH_PROVIDER)));
        for (String group : groups)
        {
            principals.add(new GroupPrincipal(group, TEST_AUTH_PROVIDER));
        }

        return new Subject(false, principals, Collections.EMPTY_SET, Collections.EMPTY_SET);
    }

    public static String getTestPrincipalSerialization(final String principalName)
    {
        return String.format("%s@%s('%s')", principalName, TEST_AUTH_PROVIDER_TYPE, TEST_AUTH_PROVIDER_NAME);
    }
}
