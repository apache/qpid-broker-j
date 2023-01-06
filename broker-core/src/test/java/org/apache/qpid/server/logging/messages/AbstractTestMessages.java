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
package org.apache.qpid.server.logging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.UnitTestMessageLogger;
import org.apache.qpid.server.logging.subjects.TestBlankSubject;
import org.apache.qpid.test.utils.UnitTestBase;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractTestMessages extends UnitTestBase
{
    protected LogMessage _logMessage = null;
    protected UnitTestMessageLogger _logger;
    protected LogSubject _logSubject = new TestBlankSubject();
    private EventLogger _eventLogger;

    @BeforeEach
    public void setUp() throws Exception
    {
        _logger = new UnitTestMessageLogger();
        _eventLogger = new EventLogger(_logger);
    }

    protected List<Object> getLog()
    {
        return _logger.getLogMessages();
    }

    protected void clearLog()
    {
        _logger.clearLogMessages();
    }

    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    protected List<Object> performLog()
    {
        if (_logMessage == null)
        {
            throw new NullPointerException("LogMessage has not been set");
        }
        _eventLogger.message(_logSubject, _logMessage);

        return _logger.getLogMessages();
    }

    protected void validateLogMessage(final List<Object> logs, final String tag, final String[] expected)
    {
        validateLogMessage(logs, tag, false, expected);
    }

    protected void validateLogMessageNoSubject(final List<Object> logs, final String tag, final String[] expected)
    {
        validateLogMessage(logs, tag, null, false, expected);
    }

    /**
     * Validate that only a single log message occurred and that the message
     * section starts with the specified tag
     *
     * @param logs     the logs generated during test run
     * @param tag      the tag to check for
     * @param expected the expected log messages
     * @param useStringForNull replace a null String reference with "null"
     */
    protected void validateLogMessage(final List<Object> logs,
                                      final String tag,
                                      final boolean useStringForNull,
                                      final String[] expected)
    {
        validateLogMessage(logs, tag, _logSubject, useStringForNull, expected);
    }

    protected void validateLogMessage(
            final List<Object> logs,
            final String tag,
            final LogSubject subject,
            final boolean useStringForNull,
            final String[] expected)
    {
        assertEquals(1, logs.size(), "Log has incorrect message count");

        // We trim() here as we don't care about extra white space at the end of the log message
        // but we do care about the ability to easily check we don't have unexpected text at
        // the end.
        final String log = String.valueOf(logs.get(0)).trim();

        // Simple switch to print out all the logged messages 
        // System.err.println(log);

        int msgIndex;
        String message;
        if (subject != null)
        {
            final String subjectString = subject.toLogString();
            msgIndex = log.indexOf(subjectString)+ subjectString.length();
            assertTrue(msgIndex != -1, "Unable to locate Subject:" + log);
        }
        else
        {
            msgIndex = log.indexOf(tag);
        }
        message = log.substring(msgIndex);

        assertTrue(message.startsWith(tag), "Message does not start with tag:" + tag + ":" + message);

        // Test that the expected items occur in order.
        int index = 0;
        for (String text : expected)
        {
            if (useStringForNull && text == null)
            {
                text = "null";
            }
            index = message.indexOf(text, index);
            assertTrue(index != -1, "Message does not contain expected (" + text + ") text :" + message);
            index = index + text.length();
        }

        // Check there is nothing left on the log message
        assertEquals(log.length(), msgIndex + index, "Message has more text. '" +
                log.substring(msgIndex + index) + "'");
    }
}
