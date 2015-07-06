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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RollingPolicy;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import ch.qos.logback.core.rolling.helper.CompressionMode;
import org.apache.qpid.server.logging.logback.RollingPolicyDecorator;
import org.apache.qpid.test.utils.QpidTestCase;

public class RollingFileAppenderFactoryTest extends QpidTestCase
{
    public static final String FILE_NAME = "TEST";
    public static final String LAYOUT = "%d %-5p [%t] \\(%c{2}\\) # %m%n";
    public static final String MAX_FILE_SIZE = "100mb";
    public static final int MAX_HISTORY = 10;

    private FileLoggerSettings _settings;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _settings = mock(FileLoggerSettings.class);
        when(_settings.getFileName()).thenReturn(FILE_NAME);
        when(_settings.getLayout()).thenReturn(LAYOUT);
        when(_settings.getMaxFileSize()).thenReturn(MAX_FILE_SIZE);
        when(_settings.isCompressOldFiles()).thenReturn(Boolean.TRUE);
        when(_settings.isRollDaily()).thenReturn(Boolean.TRUE);
        when(_settings.isRollOnRestart()).thenReturn(Boolean.TRUE);
        when(_settings.getMaxHistory()).thenReturn(MAX_HISTORY);
        when(_settings.getExecutorService()).thenReturn(mock(ScheduledExecutorService.class));
    }

    public void testCreateRollingFileAppenderDailyRolling()
    {
        RollingFileAppender<ILoggingEvent> appender = RollingFileAppenderFactory.configureRollingFileAppender(_settings,
                                                                                           mock(Context.class),
                                                                                           new RollingFileAppender<ILoggingEvent>());

        assertEquals("Unexpected appender file name", FILE_NAME, appender.getFile());

        RollingPolicy rollingPolicy = appender.getRollingPolicy();
        assertTrue("Unexpected rolling policy", rollingPolicy instanceof RollingPolicyDecorator);
        rollingPolicy = ((RollingPolicyDecorator)rollingPolicy).getDecorated();
        assertTrue("Unexpected decorated rolling policy", rollingPolicy instanceof TimeBasedRollingPolicy);
        assertEquals("Unexpected max history", MAX_HISTORY, ((TimeBasedRollingPolicy) rollingPolicy).getMaxHistory());
        assertEquals("Unexpected file name pattern", FILE_NAME + ".%d{yyyy-MM-dd}.%i.gz", ((TimeBasedRollingPolicy) rollingPolicy).getFileNamePattern());
        assertEquals("Unexpected compression mode", CompressionMode.GZ, rollingPolicy.getCompressionMode());

        TriggeringPolicy triggeringPolicy = ((TimeBasedRollingPolicy) rollingPolicy).getTimeBasedFileNamingAndTriggeringPolicy();
        assertTrue("Unexpected triggering policy", triggeringPolicy instanceof RollingFileAppenderFactory.DailyTriggeringPolicy);
        assertEquals("Unexpected triggering policy", MAX_FILE_SIZE, ((RollingFileAppenderFactory.DailyTriggeringPolicy) triggeringPolicy).getMaxFileSize());
        assertEquals("Unexpected layout", LAYOUT, ((PatternLayoutEncoder)appender.getEncoder()).getPattern());
    }

    public void testCreateRollingFileAppenderNonDailyRolling()
    {
        when(_settings.isRollDaily()).thenReturn(Boolean.FALSE);
        when(_settings.isCompressOldFiles()).thenReturn(Boolean.FALSE);

        RollingFileAppender<ILoggingEvent> appender = RollingFileAppenderFactory.configureRollingFileAppender(_settings,
                                                                                           mock(Context.class),
                                                                                           new RollingFileAppender<ILoggingEvent>());

        assertEquals("Unexpected appender file name", FILE_NAME, appender.getFile());

        RollingPolicy rollingPolicy = appender.getRollingPolicy();
        assertTrue("Unexpected rolling policy", rollingPolicy instanceof RollingPolicyDecorator);
        rollingPolicy = ((RollingPolicyDecorator)rollingPolicy).getDecorated();
        assertTrue("Unexpected decorated rolling policy", rollingPolicy instanceof RollingFileAppenderFactory.SimpleRollingPolicy);
        assertEquals("Unexpected max history", MAX_HISTORY, ((RollingFileAppenderFactory.SimpleRollingPolicy) rollingPolicy).getMaxIndex());
        assertEquals("Unexpected file name pattern", FILE_NAME + ".%i", ((RollingFileAppenderFactory.SimpleRollingPolicy) rollingPolicy).getFileNamePattern());
        assertEquals("Unexpected compression mode", CompressionMode.NONE, rollingPolicy.getCompressionMode());

        TriggeringPolicy triggeringPolicy = appender.getTriggeringPolicy();
        assertEquals("Unexpected triggering policy", MAX_FILE_SIZE, ((RollingFileAppenderFactory.SizeTriggeringPolicy) triggeringPolicy).getMaxFileSize());

        Encoder encoder = appender.getEncoder();
        assertTrue("Unexpected encoder", encoder instanceof PatternLayoutEncoder);
        assertEquals("Unexpected layout pattern", LAYOUT, ((PatternLayoutEncoder)encoder).getPattern());

    }

}
