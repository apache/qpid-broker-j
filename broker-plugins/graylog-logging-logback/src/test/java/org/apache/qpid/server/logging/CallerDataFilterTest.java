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

package org.apache.qpid.server.logging;

import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CallerDataFilterTest extends UnitTestBase
{
    public static class CallerDataTestLogger implements MessageLogger
    {
        private StackTraceElement[] _stackTraceElements;

        @Override
        public boolean isEnabled()
        {
            catchStackTrace();
            return false;
        }

        @Override
        public boolean isMessageEnabled(String logHierarchy)
        {
            catchStackTrace();
            return false;
        }

        @Override
        public void message(LogMessage message)
        {
            catchStackTrace();
        }

        @Override
        public void message(LogSubject subject, LogMessage message)
        {
            catchStackTrace();
        }

        private void catchStackTrace()
        {
            _stackTraceElements = Thread.currentThread().getStackTrace();
        }

        public StackTraceElement[] getStackTrace()
        {
            return _stackTraceElements;
        }
    }

    private CallerDataFilter _filter;
    private CallerDataTestLogger _logger;

    @Before
    public void setUp()
    {
        _filter = new CallerDataFilter();
        _logger = new CallerDataTestLogger();
    }

    @Test
    public void testFilter_nullAsInput()
    {
        StackTraceElement[] result = _filter.filter(null);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    public void testFilter_emptyInput()
    {
        StackTraceElement[] result = _filter.filter(new StackTraceElement[0]);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    public void testFilter_withoutLogger()
    {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StackTraceElement[] result = _filter.filter(stackTrace);
        assertNotNull(result);

        assertTrue(Arrays.deepEquals(stackTrace, result));
    }

    @Test
    public void testFilter_withLogger()
    {
        _logger.message(() -> "ClassName");
        StackTraceElement[] result = _filter.filter(_logger.getStackTrace());
        assertNotNull(result);

        final String loggerName = _logger.getClass().getName();
        assertFalse(Arrays.stream(result).anyMatch(e -> e.getClassName().contains(loggerName)));
    }

    @Test
    public void testFilter_withLogger_InvalidMethod()
    {
        _logger.isEnabled();
        StackTraceElement[] stackTrace = _logger.getStackTrace();
        StackTraceElement[] result = _filter.filter(_logger.getStackTrace());
        assertNotNull(result);

        assertTrue(Arrays.deepEquals(stackTrace, result));
    }

    @Test
    public void testFilter_withLoggerOnly()
    {
        _logger.message(() -> "ClassName");
        final String loggerName = _logger.getClass().getName();
        StackTraceElement[] stackTrace = Arrays.stream(_logger.getStackTrace())
                .filter(e -> e.getClassName().contains(loggerName))
                .toArray(StackTraceElement[]::new);

        StackTraceElement[] result = _filter.filter(stackTrace);
        assertNotNull(result);

        assertTrue(Arrays.deepEquals(stackTrace, result));
    }

    @Test
    public void testFilter_withUnknownClass()
    {
        StackTraceElement element1 = new StackTraceElement("unknown_class_xyz", "message", "file", 7);
        StackTraceElement element2 = new StackTraceElement("unknown_class_xyz", "message", "file", 17);

        final StackTraceElement[] stackTrace = {element1, element2};
        StackTraceElement[] result = _filter.filter(stackTrace);
        assertNotNull(result);

        assertTrue(Arrays.deepEquals(stackTrace, result));
    }
}
