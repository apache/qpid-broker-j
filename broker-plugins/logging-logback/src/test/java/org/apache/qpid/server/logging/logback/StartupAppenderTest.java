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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class StartupAppenderTest extends UnitTestBase
{
    private StartupAppender _startupAppender;

    @Before
    public void setUp() throws Exception
    {
        _startupAppender = createAndStartStartupAppender();
    }

    @Test
    public void testLogToConsole() throws Exception
    {
        ILoggingEvent event1 = createMockLoggingEvent("org.apache.qpid.Test", Level.WARN, "Test1", "Test-Thread-1");
        _startupAppender.doAppend(event1);
        ILoggingEvent event2 = createMockLoggingEvent("non.qpid.Test",Level.DEBUG, "Test2",  "Test-Thread-2");
        _startupAppender.doAppend(event2);
        ILoggingEvent event3 = createMockLoggingEvent("non.qpid.Test", Level.INFO, "Test3", "Test-Thread-3");
        _startupAppender.doAppend(event3);
        ILoggingEvent event4 = createMockLoggingEvent("org.apache.qpid.Test", Level.DEBUG, "Test4", "Test-Thread-4");
        _startupAppender.doAppend(event4);

        List<String> lines = logToConsoleAndCollectSystemOutputLines();

        assertEquals("Unexpected number of log events", (long) 2, (long) lines.size());
        assertTrue(lines.get(0).contains("Test1"));
        assertTrue(lines.get(1).contains("Test3"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReplayAccumulatedEvents()
    {
        ILoggingEvent event1 = createMockLoggingEvent("org.apache.qpid.Test", Level.DEBUG, "Test1", "Test-Thread-1");
        _startupAppender.doAppend(event1);
        ILoggingEvent event2 = createMockLoggingEvent("non.qpid.Test", Level.INFO, "Test2", "Test-Thread-2");
        _startupAppender.doAppend(event2);

        Appender mockAppender = mock(Appender.class);
        _startupAppender.replayAccumulatedEvents(mockAppender);

        verify(mockAppender).doAppend(event1);
        verify(mockAppender).doAppend(event2);

    }

    @Test
    public void testLogToConsoleWithOverriddenLogLevel() throws Exception
    {
        setTestSystemProperty(StartupAppender.PROPERTY_STARTUP_FAILOVER_CONSOLE_LOG_LEVEL, "DEBUG");

        _startupAppender = createAndStartStartupAppender();

        ILoggingEvent event1 = createMockLoggingEvent("org.apache.qpid.Test", Level.WARN, "Test1", "Test-Thread-1");
        _startupAppender.doAppend(event1);
        ILoggingEvent event2 = createMockLoggingEvent("non.qpid.Test",Level.DEBUG, "Test2",  "Test-Thread-2");
        _startupAppender.doAppend(event2);
        ILoggingEvent event3 = createMockLoggingEvent("non.qpid.Test", Level.INFO, "Test3", "Test-Thread-3");
        _startupAppender.doAppend(event3);
        ILoggingEvent event4 = createMockLoggingEvent("org.apache.qpid.Test", Level.DEBUG, "Test4", "Test-Thread-4");
        _startupAppender.doAppend(event4);

        List<String> lines = logToConsoleAndCollectSystemOutputLines();

        assertEquals("Unexpected number of log events", (long) 4, (long) lines.size());
        assertTrue(lines.get(0).contains("Test1"));
        assertTrue(lines.get(1).contains("Test2"));
        assertTrue(lines.get(2).contains("Test3"));
        assertTrue(lines.get(3).contains("Test4"));
    }

    private StartupAppender createAndStartStartupAppender()
    {
        StartupAppender startupAppender = new StartupAppender();
        startupAppender.setContext(new LoggerContext());
        startupAppender.start();
        return startupAppender;
    }

    private List<String> logToConsoleAndCollectSystemOutputLines() throws IOException
    {
        List<String> lines;
        try(ByteArrayOutputStream out = new ByteArrayOutputStream())
        {

            PrintStream originalOutput = System.out;
            try
            {
                System.setOut(new PrintStream(out));
                _startupAppender.logToConsole();
            }
            finally
            {
                System.setOut(originalOutput);
            }

            lines = getLogLines(out.toByteArray());
        }
        return lines;
    }

    private ILoggingEvent createMockLoggingEvent(String loggerName, Level logLevel, String logMessage, String threadName)
    {
        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLoggerName()).thenReturn(loggerName);
        when(event.getLevel()).thenReturn(logLevel);
        when(event.getFormattedMessage()).thenReturn(logMessage);
        when(event.getThreadName()).thenReturn(threadName);
        when(event.getTimeStamp()).thenReturn(System.currentTimeMillis());
        return event;
    }

    private List<String> getLogLines(byte[] data) throws IOException
    {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data))))
        {
            for (;;)
            {
                String line = reader.readLine();
                if (line == null)
                {
                    break;
                }
                lines.add(line);
            }
        }
        return lines;
    }
}
