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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

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

    @Before
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
        Subject subject = TestPrincipalUtils.createTestSubject(TEST_USER);

        subject.getPrincipals().add(_connectionPrincipal);

        final String message = Subject.doAs(subject, new PrivilegedAction<String>()
        {
            @Override
            public String run()
            {
                return sendTestLogMessage();
            }
        });

        assertNotNull("Test log message is not created!", message);

        List<Object> logs = getRawLogger().getLogMessages();
        assertEquals("Message log size not as expected.", (long) 1, (long) logs.size());

        String logMessage = logs.get(0).toString();
        assertTrue("Message was not found in log message", logMessage.contains(message));
        assertTrue("Message does not contain expected value: " + logMessage,
                          logMessage.startsWith(String.format(FORMAT, SESSION_ID, TEST_USER)));
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
        Subject subject = new Subject(false,
                                      Collections.singleton(_connectionPrincipal),
                                      Collections.emptySet(),
                                      Collections.emptySet());
        Subject.doAs(subject, new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                getEventLogger().message(EMPTY_MESSAGE);
                List<Object> logs = getRawLogger().getLogMessages();
                assertEquals("Message log size not as expected.", (long) 1, (long) logs.size());

                String logMessage = logs.get(0).toString();
                assertEquals("Unexpected log message", String.format(FORMAT, SESSION_ID, NA), logMessage);

                return null;
            }
        });
    }

    private void assertLogMessageWithPrincipal(String principalName)
    {
        getRawLogger().getLogMessages().clear();

        Subject subject = TestPrincipalUtils.createTestSubject(principalName);
        subject.getPrincipals().add(_connectionPrincipal);
        final String message = Subject.doAs(subject, new PrivilegedAction<String>()
        {
            @Override
            public String run()
            {
                getEventLogger().message(EMPTY_MESSAGE);
                List<Object> logs = getRawLogger().getLogMessages();
                assertEquals("Message log size not as expected.", (long) 1, (long) logs.size());

                String logMessage = logs.get(0).toString();

                return logMessage;
            }
        });

        assertTrue("Unexpected log message",
                          message.startsWith(String.format(FORMAT, SESSION_ID, principalName)));
    }
}
