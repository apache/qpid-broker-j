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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class RecordEventAppenderTest extends UnitTestBase
{
    private LoggerContext _loggerContext;
    private Logger _logger;
    private RecordEventAppender _appender;
    private ListAppender<ILoggingEvent> _events;

    @BeforeEach
    void setUp()
    {
        _loggerContext = new LoggerContext();
        _loggerContext.setMDCAdapter(new LogbackMDCAdapter());
        _loggerContext.start();
        _logger = _loggerContext.getLogger(getTestClassName());
        _logger.setLevel(Level.INFO);
        _logger.setAdditive(false);
        _appender = new RecordEventAppender(2);
        _appender.setContext(_loggerContext);
        _appender.start();
        _logger.addAppender(_appender);

        _events = new ListAppender<>();
        _events.setContext(_loggerContext);
        _events.start();
        _logger.addAppender(_events);
    }

    @AfterEach
    void tearDown()
    {
        _loggerContext.stop();
    }

    @Test
    void testRecordsPreserveProducerThreadAndFormattedMessage() throws Exception
    {
        logOnProducerThread();
        final List<LogRecord> records = new ArrayList<>();
        new LogRecorder(_appender).forEach(records::add);

        assertEquals(2, records.size());
        assertEquals(1, records.get(0).getId());
        assertEquals("producer-thread", records.get(0).getThreadName());
        assertEquals("value=before", records.get(0).getMessage());
        assertEquals(2, records.get(1).getId());
        assertEquals("producer-thread", records.get(1).getThreadName());
        assertEquals("after clearing MDC", records.get(1).getMessage());
    }

    @Test
    void testRecordsCaptureMdcWithoutCallerData() throws Exception
    {
        logOnProducerThread();
        _loggerContext.getMDCAdapter().put("trace", "reader-mdc");

        assertEquals(2, _events.list.size());
        assertEquals(Map.of("trace", "producer-mdc"), _events.list.get(0).getMDCPropertyMap());
        assertEquals(Map.of(), _events.list.get(1).getMDCPropertyMap());
        assertFalse(_events.list.get(0).hasCallerData());
        assertFalse(_events.list.get(1).hasCallerData());
    }

    private void logOnProducerThread() throws Exception
    {
        final FutureTask<Void> task = new FutureTask<>(() ->
        {
            _loggerContext.getMDCAdapter().put("trace", "producer-mdc");
            try
            {
                final StringBuilder argument = new StringBuilder("before");
                _logger.info("value={}", argument);
                argument.replace(0, argument.length(), "after");
                _loggerContext.getMDCAdapter().clear();
                _logger.info("after clearing MDC");
            }
            finally
            {
                _loggerContext.getMDCAdapter().clear();
            }
        }, null);
        final Thread producer = new Thread(task, "producer-thread");
        producer.start();
        try
        {
            task.get(5, TimeUnit.SECONDS);
        }
        finally
        {
            producer.join(TimeUnit.SECONDS.toMillis(5));
        }
        assertFalse(producer.isAlive());
    }
}
