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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    private AMQPConnection<?> _connection;
    private Callable<AuthenticationResult> _loader;

    @BeforeEach
    public void setUp() throws Exception
    {
        _connection = mock(AMQPConnection.class);
        when(_connection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("example.com", 9999));
        _subject = new Subject(true, Set.of(new ConnectionPrincipal(_connection)), Set.of(), Set.of());
        _authenticationResultCacher = new AuthenticationResultCacher(10, 10 * 60L, 2);

        _loadCallCount = 0;
        _loader = () ->
        {
            _loadCallCount += 1;
            return _successfulAuthenticationResult;
        };
    }

    @Test
    public void testCacheHit()
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            AuthenticationResult result;
            result = _authenticationResultCacher.getOrLoad(new String[]{"credentials"}, _loader);
            assertEquals(_successfulAuthenticationResult, result, "Unexpected AuthenticationResult");
            assertEquals(1, (long) _loadCallCount, "Unexpected number of loads before cache hit");
            result = _authenticationResultCacher.getOrLoad(new String[]{"credentials"}, _loader);
            assertEquals(_successfulAuthenticationResult, result, "Unexpected AuthenticationResult");
            assertEquals(1, (long) _loadCallCount, "Unexpected number of loads after cache hit");
            return null;
        });
    }

    @Test
    public void testCacheMissDifferentCredentials()
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            AuthenticationResult result;
            result = _authenticationResultCacher.getOrLoad(new String[]{"credentials"}, _loader);
            assertEquals(_successfulAuthenticationResult, result, "Unexpected AuthenticationResult");
            assertEquals(1, (long) _loadCallCount, "Unexpected number of loads before cache hit");
            result = _authenticationResultCacher.getOrLoad(new String[]{"other credentials"}, _loader);
            assertEquals(_successfulAuthenticationResult, result, "Unexpected AuthenticationResult");
            assertEquals(2, (long) _loadCallCount, "Unexpected number of loads before cache hit");
            return null;
        });
    }

    @Test
    public void testCacheMissDifferentRemoteAddressHosts()
    {
        final String credentials = "credentials";
        assertGetOrLoad(credentials, _successfulAuthenticationResult, 1);
        when(_connection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("example2.com", 8888));
        assertGetOrLoad(credentials, _successfulAuthenticationResult, 2);
    }

    @Test
    public void testCacheHitDifferentRemoteAddressPorts()
    {
        final int expectedHitCount = 1;
        final AuthenticationResult expectedResult = _successfulAuthenticationResult;
        final String credentials = "credentials";

        assertGetOrLoad(credentials, expectedResult, expectedHitCount);
        when(_connection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("example.com", 8888));
        assertGetOrLoad(credentials, expectedResult, expectedHitCount);
    }

    @Test
    public void testCacheHitNoSubject()
    {
        final String credentials = "credentials";
        final AuthenticationResult result1 = _authenticationResultCacher.getOrLoad(new String[]{credentials}, _loader);
        assertEquals(_successfulAuthenticationResult, result1, "Unexpected AuthenticationResult");
        assertEquals(1, _loadCallCount, "Unexpected number of loads before cache hit");

        final AuthenticationResult result2 = _authenticationResultCacher.getOrLoad(new String[]{credentials}, _loader);
        assertEquals(_successfulAuthenticationResult, result2, "Unexpected AuthenticationResult");
        assertEquals(1, _loadCallCount, "Unexpected number of loads before cache hit");
    }

    private void assertGetOrLoad(final String credentials,
                                 final AuthenticationResult expectedResult,
                                 final int expectedHitCount)
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () -> {
            AuthenticationResult result;
            result = _authenticationResultCacher.getOrLoad(new String[]{credentials}, _loader);
            assertEquals(expectedResult, result, "Unexpected AuthenticationResult");
            assertEquals(expectedHitCount, (long) _loadCallCount, "Unexpected number of loads before cache hit");
            return null;
        });
    }
}
