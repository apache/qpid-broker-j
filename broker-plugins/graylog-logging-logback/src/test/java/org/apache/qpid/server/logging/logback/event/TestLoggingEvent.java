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
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.slf4j.Marker;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class TestLoggingEvent implements ILoggingEvent
{
    private final String _threadName = Thread.currentThread().getName();

    private final Object[] _argumentArray = {"A", "B"};

    private final LoggerContextVO _loggerContext = new LoggerContextVO(new LoggerContext());

    private final long _timeStamp = System.currentTimeMillis();

    private final Marker _marker = new Marker()
    {
        @Override
        public String getName()
        {
            return Marker.ANY_MARKER;
        }

        @Override
        public void add(Marker marker)
        {
        }

        @Override
        public boolean remove(Marker marker)
        {
            return false;
        }

        @Override
        public boolean hasChildren()
        {
            return false;
        }

        @Override
        public boolean hasReferences()
        {
            return false;
        }

        @Override
        public Iterator<Marker> iterator()
        {
            return Collections.emptyIterator();
        }

        @Override
        public boolean contains(Marker marker)
        {
            return false;
        }

        @Override
        public boolean contains(String s)
        {
            return false;
        }
    };

    private final Map<String, String> _mdcMap = Collections.singletonMap("A", "B");

    private boolean preparedForDeferredProcessing = false;

    private final IThrowableProxy _throwableProxy = new IThrowableProxy()
    {
        @Override
        public String getMessage()
        {
            return "message";
        }

        @Override
        public String getClassName()
        {
            return "className";
        }

        @Override
        public StackTraceElementProxy[] getStackTraceElementProxyArray()
        {
            return new StackTraceElementProxy[0];
        }

        @Override
        public int getCommonFrames()
        {
            return 0;
        }

        @Override
        public IThrowableProxy getCause()
        {
            return null;
        }

        @Override
        public IThrowableProxy[] getSuppressed()
        {
            return new IThrowableProxy[0];
        }
    };

    private StackTraceElement[] _callerData;

    @Override
    public String getThreadName()
    {
        return _threadName;
    }

    @Override
    public Level getLevel()
    {
        return Level.INFO;
    }

    @Override
    public String getMessage()
    {
        return "Error message";
    }

    @Override
    public Object[] getArgumentArray()
    {
        return _argumentArray;
    }

    @Override
    public String getFormattedMessage()
    {
        return "Formatted message";
    }

    @Override
    public String getLoggerName()
    {
        return "Logger";
    }

    @Override
    public LoggerContextVO getLoggerContextVO()
    {
        return _loggerContext;
    }

    @Override
    public IThrowableProxy getThrowableProxy()
    {
        return _throwableProxy;
    }

    @Override
    public StackTraceElement[] getCallerData()
    {
        return _callerData;
    }

    @Override
    public boolean hasCallerData()
    {
        return _callerData != null;
    }

    @Override
    public Marker getMarker()
    {
        return _marker;
    }

    @Override
    public Map<String, String> getMDCPropertyMap()
    {
        return _mdcMap;
    }

    /**
     * @deprecated getMDCPropertyMap method should be used instead.
     */
    @Deprecated
    @Override
    public Map<String, String> getMdc()
    {
        return _mdcMap;
    }

    @Override
    public long getTimeStamp()
    {
        return _timeStamp;
    }

    @Override
    public void prepareForDeferredProcessing()
    {
        preparedForDeferredProcessing = true;
    }

    public boolean isPreparedForDeferredProcessing()
    {
        return preparedForDeferredProcessing;
    }

    public TestLoggingEvent withCallerData(StackTraceElement[] data)
    {
        _callerData = data;
        return this;
    }
}
