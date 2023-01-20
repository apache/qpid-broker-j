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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class LoggerNameAndLevelFilterTest extends UnitTestBase
{
    @Test
    public void testDecideForWildcardLoggerName()
    {
        final LoggerNameAndLevelFilter filter =
                new LoggerNameAndLevelFilter("org.apache.qpid.server.*", Level.INFO);

        final ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching logger name and log level");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for non matching logger name but matching log level");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for matching logger name but non matching log level");
    }

    @Test
    public void testDecideForEmptyLoggerName()
    {
        final LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("", Level.INFO);

        final ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and arbitrary logger name");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and arbitrary logger namel");

        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for non matching log level");
    }

    @Test
    public void testDecideForRootLoggerName()
    {
        final LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter(Logger.ROOT_LOGGER_NAME, Level.INFO);

        final ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and arbitrary logger name");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and arbitrary logger name");

        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for non matching log level");
    }

    @Test
    public void testDecideForNullLoggerName()
    {
        final LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter(null, Level.INFO);

        final ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and arbitrary logger name");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and arbitrary logger name");

        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for non matching log level");
    }

    @Test
    public void testDecideForNonWildCardLoggerName()
    {
        final LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("org.apache.qpid", Level.INFO);

        final ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid");
        assertEquals(FilterReply.ACCEPT, filter.decide(event),
                "Unexpected reply for matching log level and same logger name");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for matching log level and not same logger name");

        when(event.getLevel()).thenReturn(Level.DEBUG);
        when(event.getLoggerName()).thenReturn("org.apache.qpid");
        assertEquals(FilterReply.DENY, filter.decide(event),
                "Unexpected reply for non matching log leve and same logger namel");
    }

    @Test
    public void testDecideForTurnedOffLogger()
    {
        final LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("org.apache.qpid", Level.OFF);

        final ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.WARN);
        when(event.getLoggerName()).thenReturn("org.apache.qpid");
        assertEquals(FilterReply.DENY, filter.decide(event),
                "Unexpected reply for matching log level and same logger name");

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals(FilterReply.NEUTRAL, filter.decide(event),
                "Unexpected reply for matching log level and not same logger name");
    }
}
