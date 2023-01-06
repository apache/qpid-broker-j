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
 *
 */

package org.apache.qpid.server.security.auth.sasl.anonymous;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.test.utils.UnitTestBase;

public class AnonymousNegotiatorTest extends UnitTestBase
{
    @Test
    public void testHandleResponse()
    {
        final AuthenticationResult result = mock(AuthenticationResult.class);
        final AnonymousNegotiator negotiator = new AnonymousNegotiator(result);
        final Object actual = negotiator.handleResponse(new byte[0]);
        assertEquals(result, actual, "Unexpected result");

        final AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus(),
                "Only first call to handleResponse should be successful");
    }
}