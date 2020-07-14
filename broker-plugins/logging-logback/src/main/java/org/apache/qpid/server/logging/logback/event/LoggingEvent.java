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
package org.apache.qpid.server.logging.logback.event;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import org.apache.qpid.server.logging.CallerDataFilter;
import org.apache.qpid.server.util.ArrayUtils;
import org.slf4j.Marker;

import java.util.Map;
import java.util.Objects;

public final class LoggingEvent implements ILoggingEvent
{
    private static final CallerDataFilter FILTER = new CallerDataFilter();

    private final ILoggingEvent _event;

    private StackTraceElement[] _callerData = null;

    public static ILoggingEvent wrap(ILoggingEvent event)
    {
        return event != null ? new LoggingEvent(event) : null;
    }

    private LoggingEvent(ILoggingEvent event)
    {
        _event = Objects.requireNonNull(event);
    }

    @Override
    public String getThreadName()
    {
        return _event.getThreadName();
    }

    @Override
    public Level getLevel()
    {
        return _event.getLevel();
    }

    @Override
    public String getMessage()
    {
        return _event.getMessage();
    }

    @Override
    public Object[] getArgumentArray()
    {
        return _event.getArgumentArray();
    }

    @Override
    public String getFormattedMessage()
    {
        return _event.getFormattedMessage();
    }

    @Override
    public String getLoggerName()
    {
        return _event.getLoggerName();
    }

    @Override
    public LoggerContextVO getLoggerContextVO()
    {
        return _event.getLoggerContextVO();
    }

    @Override
    public IThrowableProxy getThrowableProxy()
    {
        return _event.getThrowableProxy();
    }

    @Override
    public StackTraceElement[] getCallerData()
    {
        if (_callerData == null)
        {
            _callerData = FILTER.filter(_event.getCallerData());
        }
        return _callerData != null ? _callerData.clone() : null;
    }

    @Override
    public boolean hasCallerData()
    {
        return !ArrayUtils.isEmpty(getCallerData());
    }

    @Override
    public Marker getMarker()
    {
        return _event.getMarker();
    }

    @Override
    public Map<String, String> getMDCPropertyMap()
    {
        return _event.getMDCPropertyMap();
    }

    /**
     * @deprecated getMDCPropertyMap method should be used instead.
     */
    @Deprecated
    @Override
    public Map<String, String> getMdc()
    {
        return _event.getMdc();
    }

    @Override
    public long getTimeStamp()
    {
        return _event.getTimeStamp();
    }

    @Override
    public void prepareForDeferredProcessing()
    {
        _event.prepareForDeferredProcessing();
    }
}
