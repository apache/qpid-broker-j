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

package org.apache.qpid.server.security.auth.manager;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

public class AuthenticationResultCacherTest extends UnitTestBase
{
    private AuthenticationResultCacher _authenticationResultCacher;
    private final AuthenticationResult _successfulAuthenticationResult =
            new AuthenticationResult(new AuthenticatedPrincipal(new UsernamePrincipal("TestUser", null)));
    private int _loadCallCount;
    private Subject _subject;
    private AMQPConnection _connection;
    private Callable<AuthenticationResult> _loader;

    @Before
    public void setUp() throws Exception
    {
        _connection = mock(AMQPConnection.class);
        when(_connection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("example.com", 9999));
        _subject = new Subject(true,
                               Collections.singleton(new ConnectionPrincipal(_connection)),
                               Collections.emptySet(),
                               Collections.emptySet());
        _authenticationResultCacher = new AuthenticationResultCacher(10, 10 * 60L, 2);

        _loadCallCount = 0;
        _loader = new Callable<AuthenticationResult>()
        {
            @Override
            public AuthenticationResult call() throws Exception
            {
                _loadCallCount += 1;
                return _successfulAuthenticationResult;
            }
        };
    }

    @Test
    public void testCacheHit() throws Exception
    {
        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                AuthenticationResult result;
                result = _authenticationResultCacher.getOrLoad(new String[]{"credentials"}, _loader);
                assertEquals("Unexpected AuthenticationResult", _successfulAuthenticationResult, result);
                assertEquals("Unexpected number of loads before cache hit", (long) 1, (long) _loadCallCount);
                result = _authenticationResultCacher.getOrLoad(new String[]{"credentials"}, _loader);
                assertEquals("Unexpected AuthenticationResult", _successfulAuthenticationResult, result);
                assertEquals("Unexpected number of loads after cache hit", (long) 1, (long) _loadCallCount);
                return null;
            }
        });
    }

    @Test
    public void testCacheMissDifferentCredentials() throws Exception
    {
        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                AuthenticationResult result;
                result = _authenticationResultCacher.getOrLoad(new String[]{"credentials"}, _loader);
                assertEquals("Unexpected AuthenticationResult", _successfulAuthenticationResult, result);
                assertEquals("Unexpected number of loads before cache hit", (long) 1, (long) _loadCallCount);
                result = _authenticationResultCacher.getOrLoad(new String[]{"other credentials"}, _loader);
                assertEquals("Unexpected AuthenticationResult", _successfulAuthenticationResult, result);
                assertEquals("Unexpected number of loads before cache hit", (long) 2, (long) _loadCallCount);
                return null;
            }
        });
    }


    @Test
    public void testCacheMissDifferentRemoteAddressHosts() throws Exception
    {
        final String credentials = "credentials";
        assertGetOrLoad(credentials, _successfulAuthenticationResult, 1);
        when(_connection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("example2.com", 8888));
        assertGetOrLoad(credentials, _successfulAuthenticationResult, 2);
    }

    @Test
    public void testCacheHitDifferentRemoteAddressPorts() throws Exception
    {
        final int expectedHitCount = 1;
        final AuthenticationResult expectedResult = _successfulAuthenticationResult;
        final String credentials = "credentials";

        assertGetOrLoad(credentials, expectedResult, expectedHitCount);
        when(_connection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("example.com", 8888));
        assertGetOrLoad(credentials, expectedResult, expectedHitCount);
    }

    private void assertGetOrLoad(final String credentials,
                                 final AuthenticationResult expectedResult,
                                 final int expectedHitCount)
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () -> {
            AuthenticationResult result;
            result = _authenticationResultCacher.getOrLoad(new String[]{credentials}, _loader);
            assertEquals("Unexpected AuthenticationResult", expectedResult, result);
            assertEquals("Unexpected number of loads before cache hit", (long)expectedHitCount, (long) _loadCallCount);
            return null;
        });
    }
}
