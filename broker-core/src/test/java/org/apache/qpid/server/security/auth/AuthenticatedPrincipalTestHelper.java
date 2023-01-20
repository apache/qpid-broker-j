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

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class for testing that sets of principals contain {@link AuthenticatedPrincipal}'s that wrap
 * expected {@link Principal}'s.
 */
public class AuthenticatedPrincipalTestHelper
{
    public static void assertOnlyContainsWrapped(final Principal wrappedPrincipal, final Set<Principal> principals)
    {
        assertOnlyContainsWrappedAndSecondaryPrincipals(wrappedPrincipal, Set.of(), principals);
    }

    public static void assertOnlyContainsWrappedAndSecondaryPrincipals(final Principal expectedWrappedPrincipal,
                                                                       final Set<Principal> expectedSecondaryPrincipals,
                                                                       final Set<Principal> actualPrincipals)
    {
        assertEquals(1 + expectedSecondaryPrincipals.size(), actualPrincipals.size(),
                "Principal set should contain one principal " + "but the principal set is: " + actualPrincipals);

        final Set<Principal> expectedSet = new HashSet<>(expectedSecondaryPrincipals);
        expectedSet.add(new AuthenticatedPrincipal(expectedWrappedPrincipal));
        assertEquals(expectedSet, actualPrincipals);
    }
}
