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

import java.util.ArrayList;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class StartupAppender  extends AppenderBase<ILoggingEvent>
{
    public static final String PROPERTY_STARTUP_FAILOVER_CONSOLE_LOG_LEVEL = "qpid.startup_failover_console_log_level";
    private List<ILoggingEvent> _accumulatedLoggingEvents = new ArrayList<>();
    private Level _consoleAppenderAcceptLogLevel = Level.INFO;

    public StartupAppender()
    {
        super();
        setName(StartupAppender.class.getName());
        String overriddenLogLevel = System.getProperty(PROPERTY_STARTUP_FAILOVER_CONSOLE_LOG_LEVEL);
        if (overriddenLogLevel != null)
        {
            _consoleAppenderAcceptLogLevel = Level.valueOf(overriddenLogLevel);
        }
    }

    @Override
    protected synchronized void append(ILoggingEvent e)
    {
        _accumulatedLoggingEvents.add(e);
    }

    public synchronized void replayAccumulatedEvents(Appender<ILoggingEvent> appender)
    {
        for (ILoggingEvent event: _accumulatedLoggingEvents)
        {
            appender.doAppend(event);
        }
    }

    public void logToConsole()
    {
        Context context = getContext();
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
        consoleAppender.setContext(context);
        PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
        patternLayoutEncoder.setContext(context);

        // added MDC variable 'qpid.log.prefix' for test purposes
        patternLayoutEncoder.setPattern("%X{qpid.log.prefix}%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
        patternLayoutEncoder.start();

        consoleAppender.addFilter(new Filter<ILoggingEvent>()
        {
            @Override
            public FilterReply decide(final ILoggingEvent event)
            {
                return event.getLevel().isGreaterOrEqual(_consoleAppenderAcceptLogLevel) ? FilterReply.ACCEPT : FilterReply.DENY;
            }
        });

        consoleAppender.setEncoder(patternLayoutEncoder);
        consoleAppender.start();
        replayAccumulatedEvents(consoleAppender);
        consoleAppender.stop();
    }

    @Override
    public void stop()
    {
        super.stop();
        synchronized (this)
        {
            _accumulatedLoggingEvents.clear();
        }
    }
}
