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

import static org.mockito.Mockito.mock;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.test.utils.QpidTestCase;

public class AnonymousNegotiatorTest extends QpidTestCase
{
    public void testHandleResponse() throws Exception
    {
        final AuthenticationResult result = mock(AuthenticationResult.class);
        AnonymousNegotiator negotiator = new AnonymousNegotiator(result);
        assertEquals("Unexpected result", result, negotiator.handleResponse(new byte[0]));

        AuthenticationResult secondResult = negotiator.handleResponse(new byte[0]);
        assertEquals("Only first call to handleResponse should be successful",
                     AuthenticationResult.AuthenticationStatus.ERROR, secondResult.getStatus());
    }
}