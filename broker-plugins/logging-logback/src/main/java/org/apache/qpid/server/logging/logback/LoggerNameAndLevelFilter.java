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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class LoggerNameAndLevelFilter extends Filter<ILoggingEvent> implements EffectiveLevelFilter
{
    private final Filter<ILoggingEvent> _filter;
    private final String _loggerName;
    private volatile Level _level;

    public LoggerNameAndLevelFilter(String loggerName, Level level)
    {
        _level = level;
        _loggerName = loggerName;
        _filter = createFilter(loggerName);
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        return _filter.decide(event);
    }

    public void setLevel(Level level)
    {
        _level = level;
    }

    @Override
    public Level getLevel()
    {
        return _level;
    }

    public String getLoggerName()
    {
        return _loggerName;
    }

    protected Filter<ILoggingEvent> createFilter(final String loggerName)
    {
        if(loggerName == null || "".equals(loggerName) || Logger.ROOT_LOGGER_NAME.equals(loggerName))
        {
            return new Filter<ILoggingEvent>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent event)
                {
                    return getWildCardLoggerFilterReply(event.getLevel());
                }
            };
        }
        else if(loggerName.endsWith(".*"))
        {
            final String prefixName = loggerName.substring(0,loggerName.length()-2);
            return new Filter<ILoggingEvent>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent event)
                {
                    if (event.getLoggerName().startsWith(prefixName))
                    {
                        return getWildCardLoggerFilterReply(event.getLevel());
                    }
                    return FilterReply.NEUTRAL;
                }
            };
        }
        else
        {
            return new Filter<ILoggingEvent>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent event)
                {
                    if (event.getLoggerName().equals(loggerName))
                    {
                        return getExactLoggerFilterReply(event.getLevel());
                    }
                    return FilterReply.NEUTRAL;
                }
            };
        }
    }

    private FilterReply getWildCardLoggerFilterReply(final Level eventLevel)
    {
        if (eventLevel.isGreaterOrEqual(_level))
        {
            return FilterReply.ACCEPT;
        }
        else
        {
            return FilterReply.NEUTRAL;
        }
    }

    private FilterReply getExactLoggerFilterReply(final Level eventLevel)
    {
        return  eventLevel.isGreaterOrEqual(_level) ? FilterReply.ACCEPT : FilterReply.DENY;
    }

    @Override
    public Level getEffectiveLevel(final Logger logger)
    {
        if((_loggerName == null || "".equals(_loggerName) || Logger.ROOT_LOGGER_NAME.equals(_loggerName))
          || (_loggerName.endsWith(".*") && logger.getName().startsWith(_loggerName.substring(0,_loggerName.length()-2)))
          || _loggerName.equals(logger.getName()))
        {
            return _level;
        }
        else
        {
            return Level.OFF;
        }
    }
}
