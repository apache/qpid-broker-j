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

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LoggingEventTest extends UnitTestBase
{
    public static class LocalTestLogger implements MessageLogger
    {
        private StackTraceElement[] _stackTrace = null;

        @Override
        public boolean isEnabled()
        {
            fillStackTrace();
            return false;
        }

        @Override
        public boolean isMessageEnabled(String logHierarchy)
        {
            fillStackTrace();
            return false;
        }

        @Override
        public void message(LogMessage message)
        {
            fillStackTrace();
        }

        @Override
        public void message(LogSubject subject, LogMessage message)
        {
            fillStackTrace();
        }

        private void fillStackTrace()
        {
            _stackTrace = Thread.currentThread().getStackTrace();
        }

        public TestLoggingEvent event()
        {
            return new TestLoggingEvent().withCallerData(_stackTrace);
        }
    }

    @Test
    public void testWrap_NullAsInput()
    {
        assertNull(LoggingEvent.wrap(null));
    }

    @Test
    public void testWrap()
    {
        assertNotNull(LoggingEvent.wrap(new TestLoggingEvent()));
    }

    @Test
    public void testGetThreadName()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getThreadName(), wrapper.getThreadName());
    }

    @Test
    public void testGetLevel()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getLevel(), wrapper.getLevel());
    }

    @Test
    public void testGetMessage()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getMessage(), wrapper.getMessage());
    }

    @Test
    public void testGetArgumentArray()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertTrue(Arrays.deepEquals(event.getArgumentArray(), wrapper.getArgumentArray()));
    }

    @Test
    public void testGetFormattedMessage()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getFormattedMessage(), wrapper.getFormattedMessage());
    }

    @Test
    public void testGetLoggerName()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getLoggerName(), wrapper.getLoggerName());
    }

    @Test
    public void testGetLoggerContextVO()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getLoggerContextVO(), wrapper.getLoggerContextVO());
    }

    @Test
    public void testGetThrowableProxy()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getThrowableProxy(), wrapper.getThrowableProxy());
    }

    @Test
    public void testGetCallerData_NullAsInput()
    {
        TestLoggingEvent event = new TestLoggingEvent().withCallerData(null);
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        StackTraceElement[] data = wrapper.getCallerData();
        assertNotNull(data);
        assertEquals(0, data.length);

        data = wrapper.getCallerData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    public void testGetCallerData_EmptyInput()
    {
        TestLoggingEvent event = new TestLoggingEvent().withCallerData(new StackTraceElement[0]);
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        StackTraceElement[] callerData = wrapper.getCallerData();
        assertNotNull(callerData);
        assertEquals(0, callerData.length);

        callerData = wrapper.getCallerData();
        assertNotNull(callerData);
        assertEquals(0, callerData.length);
    }

    @Test
    public void testGetCallerData_AllData()
    {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        TestLoggingEvent event = new TestLoggingEvent().withCallerData(stackTrace);
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        StackTraceElement[] callerData = wrapper.getCallerData();
        assertNotNull(callerData);
        assertTrue(Arrays.deepEquals(stackTrace, callerData));

        callerData = wrapper.getCallerData();
        assertNotNull(callerData);
        assertTrue(Arrays.deepEquals(stackTrace, callerData));
    }

    @Test
    public void testGetCallerData_FilteredData()
    {
        LocalTestLogger logger = new LocalTestLogger();
        logger.message(() -> "Class");

        ILoggingEvent wrapper = LoggingEvent.wrap(logger.event());
        assertNotNull(wrapper);

        StackTraceElement[] callerData = wrapper.getCallerData();
        assertNotNull(callerData);

        final String name = LocalTestLogger.class.getName();
        assertFalse(Arrays.stream(callerData).anyMatch(e -> e.getClassName().contains(name)));
    }

    @Test
    public void testHasCallerData_NegativeResult()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        event.withCallerData(null);
        assertFalse(wrapper.hasCallerData());

        event.withCallerData(new StackTraceElement[0]);
        assertFalse(wrapper.hasCallerData());
    }

    @Test
    public void testHasCallerData_PositiveResult()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        event.withCallerData(Thread.currentThread().getStackTrace());
        assertTrue(wrapper.hasCallerData());
    }

    @Test
    public void testGetMarker()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getMarker(), wrapper.getMarker());
    }

    @Test
    public void tesGetMDCPropertyMap()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        Map<String, String> originalMap = event.getMDCPropertyMap();
        Map<String, String> map = wrapper.getMDCPropertyMap();

        assertEquals(originalMap.keySet(), map.keySet());
        for (Map.Entry<String, String> entry : originalMap.entrySet())
        {
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    public void testGetMdc()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);

        Map<String, String> originalMap = event.getMdc();
        Map<String, String> map = wrapper.getMdc();

        assertEquals(originalMap.keySet(), map.keySet());
        for (Map.Entry<String, String> entry : originalMap.entrySet())
        {
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    public void testGetTimeStamp()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertEquals(event.getTimeStamp(), wrapper.getTimeStamp());
    }

    @Test
    public void testPrepareForDeferredProcessing()
    {
        TestLoggingEvent event = new TestLoggingEvent();
        ILoggingEvent wrapper = LoggingEvent.wrap(event);
        assertNotNull(wrapper);
        assertFalse(event.isPreparedForDeferredProcessing());
        wrapper.prepareForDeferredProcessing();
        assertTrue(event.isPreparedForDeferredProcessing());
    }
}
