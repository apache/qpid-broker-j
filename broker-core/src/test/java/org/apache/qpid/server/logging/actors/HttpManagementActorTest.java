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
package org.apache.qpid.server.logging.actors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.security.auth.SocketConnectionMetaData;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;

public class HttpManagementActorTest extends BaseActorTestCase
{
    public static final LogMessage EMPTY_MESSAGE = new LogMessage()
    {
        @Override
        public String getLogHierarchy()
        {
            return "";
        }

        @Override
        public String toString()
        {
            return "";
        }
    };

    private static final String IP = "127.0.0.1";
    private static final int PORT = 1;
    private static final String TEST_USER = "guest";
    private static final String SESSION_ID = "testSession";

    private static final String FORMAT = "[mng:%s(%s@/" + IP + ":" + PORT + ")] ";
    private static final Object NA = "N/A";

    private ManagementConnectionPrincipal _connectionPrincipal;

    @BeforeEach
    public void setUp() throws Exception
    {
        super.setUp();
        _connectionPrincipal = new ManagementConnectionPrincipal()
        {
            @Override
            public String getType()
            {
                return "HTTP";
            }

            @Override
            public String getSessionId()
            {
                return SESSION_ID;
            }

            @Override
            public SocketAddress getRemoteAddress()
            {
                return new InetSocketAddress(IP, PORT);
            }

            @Override
            public SocketConnectionMetaData getConnectionMetaData()
            {
                return null;
            }

            @Override
            public String getName()
            {
                return getRemoteAddress().toString();
            }
        };
    }

    @Test
    public void testSubjectPrincipalNameAppearance()
    {
        final Subject subject = TestPrincipalUtils.createTestSubject(TEST_USER);
        subject.getPrincipals().add(_connectionPrincipal);
        final String message = Subject.doAs(subject, (PrivilegedAction<String>) this::sendTestLogMessage);

        assertNotNull(message, "Test log message is not created!");

        final List<Object> logs = getRawLogger().getLogMessages();
        assertEquals(1, (long) logs.size(), "Message log size not as expected.");

        final String logMessage = logs.get(0).toString();
        assertTrue(logMessage.contains(message), "Message was not found in log message");
        assertTrue(logMessage.startsWith(String.format(FORMAT, SESSION_ID, TEST_USER)),
                "Message does not contain expected value: " + logMessage);
    }

    /** It's necessary to test successive calls because HttpManagementActor caches
     *  its log message based on principal name */
    @Test
    public void testGetLogMessageCaching()
    {
        assertLogMessageWithoutPrincipal();
        assertLogMessageWithPrincipal("my_principal");
        assertLogMessageWithPrincipal("my_principal2");
        assertLogMessageWithoutPrincipal();
    }

    private void assertLogMessageWithoutPrincipal()
    {
        getRawLogger().getLogMessages().clear();
        final Subject subject = new Subject(false, Set.of(_connectionPrincipal), Set.of(), Set.of());
        Subject.doAs(subject, (PrivilegedAction<Object>) () ->
        {
            getEventLogger().message(EMPTY_MESSAGE);
            final List<Object> logs = getRawLogger().getLogMessages();
            assertEquals(1, (long) logs.size(), "Message log size not as expected.");

            final String logMessage = logs.get(0).toString();
            assertEquals(String.format(FORMAT, SESSION_ID, NA), logMessage, "Unexpected log message");
            return null;
        });
    }

    private void assertLogMessageWithPrincipal(final String principalName)
    {
        getRawLogger().getLogMessages().clear();

        final Subject subject = TestPrincipalUtils.createTestSubject(principalName);
        subject.getPrincipals().add(_connectionPrincipal);
        final String message = Subject.doAs(subject, (PrivilegedAction<String>) () ->
        {
            getEventLogger().message(EMPTY_MESSAGE);
            final List<Object> logs = getRawLogger().getLogMessages();
            assertEquals(1, (long) logs.size(), "Message log size not as expected.");
            return logs.get(0).toString();
        });

        assertTrue(message.startsWith(String.format(FORMAT, SESSION_ID, principalName)), "Unexpected log message");
    }
}
