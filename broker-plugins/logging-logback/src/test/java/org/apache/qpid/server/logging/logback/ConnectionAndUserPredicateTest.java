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
package org.apache.qpid.server.logging.logback;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.Set;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConnectionAndUserPredicateTest extends UnitTestBase
{
    private static final String TEST_USER = "testUser@foo('bar')";
    private ConnectionAndUserPredicate _predicate;
    private Subject _subject;

    @BeforeEach
    public void setUp() throws Exception
    {
        _predicate = new ConnectionAndUserPredicate();
        _subject = new Subject(false,
                               Set.of(new AuthenticatedPrincipal(new GenericPrincipal(TEST_USER))),
                               Set.of(),
                               Set.of());
    }


    @Test
    public void testEvaluateUsername()
    {
        _predicate.setUsernamePattern("testUser.*");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertTrue(_predicate.evaluate(mock(ILoggingEvent.class)),"predicate unexpectedly did not match");
            return null;
        });
        _predicate.setUsernamePattern("nonmatching.*");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertFalse(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly matched");
            return null;
        });
    }

    @Test
    public void testEvaluateRemoteContainerIdAndUsername()
    {
        final AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getRemoteAddressString()).thenReturn("foo:1234");
        when(connection.getRemoteContainerName()).thenReturn("TestClientId");
        _subject.getPrincipals().add(new ConnectionPrincipal(connection));
        _predicate.setRemoteContainerIdPattern(".*Client.*");
        _predicate.setUsernamePattern("testUser.*");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertTrue(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly did not match");
            return null;
        });
        _predicate.setRemoteContainerIdPattern(".*noMatchingClient.*");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertFalse(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly matched");
            return null;
        });
        _predicate.setRemoteContainerIdPattern(".*Client.*");
        _predicate.setUsernamePattern("noMatchingUsername.*");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertFalse(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly matched");
            return null;
        });
    }

    @Test
    public void testEvaluateConnectionNameForAmqp()
    {
        final AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getRemoteAddressString()).thenReturn("foo:1234");
        when(connection.getRemoteContainerName()).thenReturn(null);
        _subject.getPrincipals().add(new ConnectionPrincipal(connection));
        _predicate.setConnectionNamePattern(".*:1234");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertTrue(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly did not match");
            return null;
        });
        _predicate.setConnectionNamePattern(".*:4321");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertFalse(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly matched");
            return null;
        });
    }

    @Test
    public void testEvaluateConnectionNameForHttp()
    {
        final ManagementConnectionPrincipal principal = mock(ManagementConnectionPrincipal.class);
        when(principal.getName()).thenReturn("foo:1234");
        _subject.getPrincipals().add(principal);
        _predicate.setConnectionNamePattern(".*:1234");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertTrue(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly did not match");
            return null;
        });
        _predicate.setConnectionNamePattern(".*:4321");
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            assertFalse(_predicate.evaluate(mock(ILoggingEvent.class)), "predicate unexpectedly matched");
            return null;
        });
    }
}
