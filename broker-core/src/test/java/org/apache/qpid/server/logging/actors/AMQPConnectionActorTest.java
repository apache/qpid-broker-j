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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.logging.LogMessage;

import javax.security.auth.Subject;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test : AMQPConnectionActorTest
 * Validate the AMQPConnectionActor class.
 *
 * The test creates a new AMQPActor and then logs a message using it.
 *
 * The test then verifies that the logged message was the only one created and
 * that the message contains the required message.
 */
public class AMQPConnectionActorTest extends BaseConnectionActorTestCase
{
    @BeforeEach
    public void setUp()
    {
        //Prevent logger creation
    }

    /**
     * Test the AMQPActor logging as a Connection level.
     *
     * The test sends a message then verifies that it entered the logs.
     *
     * The log message should be fully replaced (no '{n}' values) and should
     * not contain any channel identification.
     */
    @Test
    public void testConnection() throws Exception
    {
        super.setUp();

        // ignore all the startup log messages
        getRawLogger().clearLogMessages();
        final String message = sendLogMessage();

        final List<Object> logs = getRawLogger().getLogMessages();

        assertEquals(1, (long) logs.size(), "Message log size not as expected.");

        // Verify that the logged message is present in the output
        assertTrue(logs.get(0).toString().contains(message), "Message was not found in log message");

        // Verify that the message has the correct type
        assertTrue(logs.get(0).toString().contains("[con:"), "Message does not contain the [con: prefix");

        // Verify that all the values were presented to the MessageFormatter
        // so we will not end up with '{n}' entries in the log.
        assertFalse(logs.get(0).toString().contains("{"), "Verify that the string does not contain any '{'.");

        // Verify that the logged message does not contains the 'ch:' marker
        assertFalse(logs.get(0).toString().contains("/ch:"), "Message was logged with a channel identifier." + logs.get(0));
    }

    @Test
    public void testConnectionLoggingOff() throws Exception
    {
        setStatusUpdatesEnabled(false);
        super.setUp();
        sendLogMessage();
        final List<Object> logs = getRawLogger().getLogMessages();
        assertEquals(0, (long) logs.size(), "Message log size not as expected.");
    }

    private String sendLogMessage()
    {
        final String message = "test logging";
        final Subject subject = new Subject(false, Set.of(new ConnectionPrincipal(getConnection())), Set.of(), Set.of());
        Subject.doAs(subject, (PrivilegedAction<Object>) () ->
        {
            getEventLogger().message(() -> "[AMQPActorTest]", new LogMessage()
            {
                @Override
                public String toString()
                {
                    return message;
                }

                @Override
                public String getLogHierarchy()
                {
                    return "test.hierarchy";
                }
            });
            return null;
        });
        return message;
    }
}
