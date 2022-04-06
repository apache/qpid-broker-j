/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.access.plugins;

import junit.framework.TestCase;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class RuleOutcomeTest extends TestCase
{
    private EventLogger _logger;
    private EventLoggerProvider _provider;

    @Override
    public void setUp()
    {
        _logger = Mockito.mock(EventLogger.class);
        _provider =() ->_logger;
    }

    @Test
    public void testLogResult()
    {
        assertEquals(Result.ALLOWED, RuleOutcome.ALLOW.logResult(_provider, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties()));
        assertEquals(Result.DENIED, RuleOutcome.DENY.logResult(_provider, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties()));
        Mockito.verify(_logger, Mockito.never()).message(Mockito.any(LogMessage.class));
    }

    @Test
    public void testLogDeniedResult() {
        assertEquals(Result.DENIED, RuleOutcome.DENY_LOG.logResult(_provider, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties()));

        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        Mockito.verify(_logger, Mockito.times(1)).message(captor.capture());

        final LogMessage message = captor.getValue();
        assertNotNull(message);
        assertTrue(message.toString().contains("Denied"));
        assertTrue(message.toString().contains(LegacyOperation.ACCESS.toString()));
        assertTrue(message.toString().contains(ObjectType.VIRTUALHOST.toString()));
    }

    @Test
    public void testLogAllowResult() {
        assertEquals(Result.ALLOWED, RuleOutcome.ALLOW_LOG.logResult(_provider, LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, new ObjectProperties()));

        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        Mockito.verify(_logger, Mockito.times(1)).message(captor.capture());

        final LogMessage message = captor.getValue();
        assertNotNull(message);
        assertTrue(message.toString().contains("Allowed"));
        assertTrue(message.toString().contains(LegacyOperation.ACCESS.toString()));
        assertTrue(message.toString().contains(ObjectType.VIRTUALHOST.toString()));
    }
}
