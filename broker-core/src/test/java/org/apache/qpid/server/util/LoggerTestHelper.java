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

package org.apache.qpid.server.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LoggerTestHelper
{
    private final static ch.qos.logback.classic.Logger ROOT_LOGGER = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));

    public static ListAppender createAndRegisterAppender(String appenderName)
    {
        ListAppender appender = new ListAppender();
        appender.setName(appenderName);
        ROOT_LOGGER.addAppender(appender);
        appender.start();
        return appender;
    }

    public static void deleteAndUnregisterAppender(Appender appender)
    {
        appender.stop();
        ROOT_LOGGER.detachAppender(appender);
    }

    public static void deleteAndUnregisterAppender(String appenderName)
    {
        Appender appender = ROOT_LOGGER.getAppender(appenderName);
        if (appender != null)
        {
            deleteAndUnregisterAppender(appender);
        }
    }

    public static void assertLoggedEvent(ListAppender appender, boolean exists, String message, String loggerName, Level level)
    {
        List<ILoggingEvent> events;
        synchronized(appender)
        {
            events = new ArrayList<ILoggingEvent>(appender.list);
        }

        boolean logged = false;
        for (ILoggingEvent event: events)
        {
            if (event.getFormattedMessage().equals(message) && event.getLoggerName().equals(loggerName) && event.getLevel() == level)
            {
                logged = true;
                break;
            }
        }
        assertEquals("Event " + message + " from logger " + loggerName + " of log level " + level
                + " is " + (exists ? "not" : "") + " found", exists, logged);
    }

}
