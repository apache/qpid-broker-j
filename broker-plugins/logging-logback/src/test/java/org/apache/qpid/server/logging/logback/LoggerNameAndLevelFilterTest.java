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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class LoggerNameAndLevelFilterTest extends UnitTestBase
{

    @Test
    public void testDecideForWildcardLoggerName() throws Exception
    {
        LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("org.apache.qpid.server.*", Level.INFO);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals("Unexpected reply for matching logger name and log level",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals("Unexpected reply for non matching logger name but matching log level",
                            FilterReply.NEUTRAL,
                            filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals("Unexpected reply for matching logger name but non matching log level",
                            FilterReply.NEUTRAL,
                            filter.decide(event));
    }

    @Test
    public void testDecideForEmptyLoggerName() throws Exception
    {
        LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("", Level.INFO);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals("Unexpected reply for matching log level and arbitrary logger name",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals("Unexpected reply for matching log level and arbitrary logger namel",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals("Unexpected reply for non matching log level", FilterReply.NEUTRAL, filter.decide(event));
    }

    @Test
    public void testDecideForRootLoggerName() throws Exception
    {
        LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter(Logger.ROOT_LOGGER_NAME, Level.INFO);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals("Unexpected reply for matching log level and arbitrary logger name",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals("Unexpected reply for matching log level and arbitrary logger name",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals("Unexpected reply for non matching log level", FilterReply.NEUTRAL, filter.decide(event));
    }

    @Test
    public void testDecideForNullLoggerName() throws Exception
    {
        LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter(null, Level.INFO);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid.server.foo");
        assertEquals("Unexpected reply for matching log level and arbitrary logger name",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals("Unexpected reply for matching log level and arbitrary logger name",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLevel()).thenReturn(Level.DEBUG);
        assertEquals("Unexpected reply for non matching log level", FilterReply.NEUTRAL, filter.decide(event));
    }

    @Test
    public void testDecideForNonWildCardLoggerName() throws Exception
    {
        LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("org.apache.qpid", Level.INFO);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("org.apache.qpid");
        assertEquals("Unexpected reply for matching log level and same logger name",
                            FilterReply.ACCEPT,
                            filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals("Unexpected reply for matching log level and not same logger name",
                            FilterReply.NEUTRAL,
                            filter.decide(event));

        when(event.getLevel()).thenReturn(Level.DEBUG);
        when(event.getLoggerName()).thenReturn("org.apache.qpid");
        assertEquals("Unexpected reply for non matching log leve and same logger namel",
                            FilterReply.DENY,
                            filter.decide(event));
    }

    @Test
    public void testDecideForTurnedOffLogger() throws Exception
    {
        LoggerNameAndLevelFilter filter = new LoggerNameAndLevelFilter("org.apache.qpid", Level.OFF);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.WARN);
        when(event.getLoggerName()).thenReturn("org.apache.qpid");
        assertEquals("Unexpected reply for matching log level and same logger name",
                     FilterReply.DENY,
                     filter.decide(event));

        when(event.getLoggerName()).thenReturn("org.apache.qpid.foo");
        assertEquals("Unexpected reply for matching log level and not same logger name",
                     FilterReply.NEUTRAL,
                     filter.decide(event));
    }
}
