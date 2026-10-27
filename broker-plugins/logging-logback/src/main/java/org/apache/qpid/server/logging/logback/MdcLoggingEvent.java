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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import org.slf4j.Marker;
import org.slf4j.event.KeyValuePair;

final class MdcLoggingEvent implements ILoggingEvent
{
    private final ILoggingEvent _event;
    private final Map<String, String> _mdc;

    MdcLoggingEvent(final ILoggingEvent event, final Map<String, String> additions)
    {
        _event = Objects.requireNonNull(event, "Event must not be null");
        final Map<String, String> mdc = new HashMap<>(event.getMDCPropertyMap());
        mdc.putAll(Objects.requireNonNull(additions, "MDC additions must not be null"));
        _mdc = Collections.unmodifiableMap(mdc);
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
        return _event.getCallerData();
    }

    @Override
    public boolean hasCallerData()
    {
        return _event.hasCallerData();
    }

    @Override
    public List<Marker> getMarkerList()
    {
        return _event.getMarkerList();
    }

    @Override
    public Map<String, String> getMDCPropertyMap()
    {
        return _mdc;
    }

    @Deprecated
    @Override
    public Map<String, String> getMdc()
    {
        return _mdc;
    }

    @Override
    public long getTimeStamp()
    {
        return _event.getTimeStamp();
    }

    @Override
    public Instant getInstant()
    {
        return _event.getInstant();
    }

    @Override
    public int getNanoseconds()
    {
        return _event.getNanoseconds();
    }

    @Override
    public long getSequenceNumber()
    {
        return _event.getSequenceNumber();
    }

    @Override
    public List<KeyValuePair> getKeyValuePairs()
    {
        return _event.getKeyValuePairs();
    }

    @Override
    public void prepareForDeferredProcessing()
    {
        _event.prepareForDeferredProcessing();
    }
}
