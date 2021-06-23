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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.user.connection.limits.logging.ConnectionLimitEventLogger;
import org.apache.qpid.server.user.connection.limits.logging.FullConnectionLimitEventLogger;
import org.apache.qpid.test.utils.UnitTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AcceptRegistrationTest extends UnitTestBase
{
    private org.apache.qpid.server.logging.EventLogger _eventLogger;
    private FullConnectionLimitEventLogger _logger;
    private ConnectionLimitEventLogger _rejectLogger;
    private ConnectionSlot _slot;

    @Before
    public void setUp()
    {
        _eventLogger = Mockito.mock(org.apache.qpid.server.logging.EventLogger.class);
        final EventLoggerProvider provider = () -> _eventLogger;
        _logger = new FullConnectionLimitEventLogger("AllLogger", provider);
        _rejectLogger = new ConnectionLimitEventLogger("RejectLogger", provider);
        _slot = Mockito.mock(ConnectionSlot.class);
    }

    @Test
    public void testLogMessage()
    {
        final AcceptRegistration result = AcceptRegistration.newInstance(_slot, "user", 7, "amqp");
        assertNotNull(result);
        assertEquals("User user with connection count 7 on amqp port", result.getMessage());

        assertEquals(_slot, result.logMessage(_rejectLogger));
        Mockito.verify(_eventLogger, Mockito.never()).message(Mockito.any(LogMessage.class));

        assertEquals(_slot, result.logMessage(_logger));
        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        Mockito.verify(_eventLogger).message(captor.capture());
        final LogMessage message = captor.getValue();
        assertEquals("RL-1001 : Accepted : Opening connection by user : Limiter 'AllLogger': User user with connection count 7 on amqp port",
                message.toString());
    }

    @Test
    public void testFree()
    {
        final AcceptRegistration result = AcceptRegistration.newInstance(_slot, "user", 17, "amqps");
        assertNotNull(result);
        result.free();

        Mockito.verify(_slot).free();
    }
}