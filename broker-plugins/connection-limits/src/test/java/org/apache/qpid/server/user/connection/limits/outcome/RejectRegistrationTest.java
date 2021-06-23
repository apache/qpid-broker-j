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
package org.apache.qpid.server.user.connection.limits.outcome;

import java.time.Duration;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.user.connection.limits.logging.ConnectionLimitEventLogger;
import org.apache.qpid.test.utils.UnitTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RejectRegistrationTest extends UnitTestBase
{
    private org.apache.qpid.server.logging.EventLogger _eventLogger;
    private ConnectionLimitEventLogger _logger;

    @Before
    public void setUp()
    {
        _eventLogger = Mockito.mock(org.apache.qpid.server.logging.EventLogger.class);
        _logger = new ConnectionLimitEventLogger("RejectLogger", () -> _eventLogger);
    }

    @Test
    public void testFromConnectionCount()
    {
        final RejectRegistration resource = RejectRegistration.breakingConnectionCount("user", 10, "amqp");
        assertNotNull(resource);
        assertEquals("User user breaks connection count limit 10 on port amqp", resource.logMessage(_logger));

        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        Mockito.verify(_eventLogger).message(captor.capture());
        final LogMessage message = captor.getValue();
        assertEquals("RL-1002 : Rejected : Opening connection by user : Limiter 'RejectLogger': User user breaks connection count limit 10 on port amqp", message.toString());
    }

    @Test
    public void testFromConnectionFrequency()
    {
        final RejectRegistration resource = RejectRegistration.breakingConnectionFrequency("user", 10, Duration.ofMinutes(1L), "amqp");
        assertNotNull(resource);
        assertEquals("User user breaks connection frequency limit 10 per 60 s on port amqp", resource.logMessage(_logger));

        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        Mockito.verify(_eventLogger).message(captor.capture());
        final LogMessage message = captor.getValue();
        assertEquals("RL-1002 : Rejected : Opening connection by user : Limiter 'RejectLogger': User user breaks connection frequency limit 10 per 60 s on port amqp", message.toString());
    }

    @Test
    public void testBlockedUser()
    {
        final RejectRegistration resource = RejectRegistration.blockedUser("user", "amqp");
        assertNotNull(resource);
        assertEquals("User user is blocked on port amqp", resource.logMessage(_logger));

        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        Mockito.verify(_eventLogger).message(captor.capture());
        final LogMessage message = captor.getValue();
        assertEquals("RL-1002 : Rejected : Opening connection by user : Limiter 'RejectLogger': User user is blocked on port amqp", message.toString());
    }
}