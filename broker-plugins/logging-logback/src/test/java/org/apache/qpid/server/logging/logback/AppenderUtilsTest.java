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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RollingPolicy;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import ch.qos.logback.core.rolling.helper.CompressionMode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;

public class AppenderUtilsTest extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AppenderUtilsTest.class);
    public static final String LAYOUT = "%d %-5p [%t] \\(%c{2}\\) # %m%n";
    public static final int MAX_FILE_SIZE = 101;
    public static final int MAX_HISTORY = 13;

    private FileLoggerSettings _settings;
    private File _testLogFile;
    private String _testLogFileName;

    @BeforeEach
    public void setUp() throws Exception
    {
        _testLogFile = File.createTempFile(getTestName(), ".log");
        _testLogFileName = _testLogFile.getAbsolutePath();
        _settings = mock(FileLoggerSettings.class);
        when(_settings.getFileName()).thenReturn(_testLogFileName);
        when(_settings.getLayout()).thenReturn(LAYOUT);
        when(_settings.getMaxFileSize()).thenReturn(MAX_FILE_SIZE);
        when(_settings.isCompressOldFiles()).thenReturn(Boolean.TRUE);
        when(_settings.isRollDaily()).thenReturn(Boolean.TRUE);
        when(_settings.isRollOnRestart()).thenReturn(Boolean.TRUE);
        when(_settings.getMaxHistory()).thenReturn(MAX_HISTORY);
        when(_settings.getExecutorService()).thenReturn(mock(ScheduledExecutorService.class));
    }

    @AfterEach
    public void tearDown()
    {
        _testLogFile.delete();
    }

    @Test
    public void testCreateRollingFileAppenderDailyRolling()
    {
        final RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        AppenderUtils.configureRollingFileAppender(_settings, new LoggerContext(), appender);

        assertEquals(_testLogFileName, appender.getFile(), "Unexpected appender file name");

        RollingPolicy rollingPolicy = appender.getRollingPolicy();
        final boolean condition2 = rollingPolicy instanceof RollingPolicyDecorator;
        assertTrue(condition2, "Unexpected rolling policy");
        rollingPolicy = ((RollingPolicyDecorator)rollingPolicy).getDecorated();
        final boolean condition1 = rollingPolicy instanceof TimeBasedRollingPolicy;
        assertTrue(condition1, "Unexpected decorated rolling policy");
        assertEquals(MAX_HISTORY, (long) ((TimeBasedRollingPolicy<?>) rollingPolicy).getMaxHistory(),
                "Unexpected max history");

        assertEquals(_testLogFileName + ".%d{yyyy-MM-dd}.%i.gz",
                ((TimeBasedRollingPolicy<?>) rollingPolicy).getFileNamePattern(),
                "Unexpected file name pattern");

        assertEquals(CompressionMode.GZ, rollingPolicy.getCompressionMode(), "Unexpected compression mode");

        final TriggeringPolicy<?> triggeringPolicy = ((TimeBasedRollingPolicy<?>) rollingPolicy).getTimeBasedFileNamingAndTriggeringPolicy();
        final boolean condition = triggeringPolicy instanceof AppenderUtils.DailyTriggeringPolicy;
        assertTrue(condition, "Unexpected triggering policy");
        assertEquals(MAX_FILE_SIZE + " MB",
                ((AppenderUtils.DailyTriggeringPolicy) triggeringPolicy).getMaxFileSize().toString(),
                "Unexpected triggering policy");
        assertEquals(LAYOUT, ((PatternLayoutEncoder) appender.getEncoder()).getPattern(), "Unexpected layout");
    }

    @Test
    public void testCreateRollingFileAppenderNonDailyRolling()
    {
        when(_settings.isRollDaily()).thenReturn(Boolean.FALSE);
        when(_settings.isCompressOldFiles()).thenReturn(Boolean.FALSE);

        final RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        AppenderUtils.configureRollingFileAppender(_settings, new LoggerContext(), appender);

        assertEquals(_testLogFileName, appender.getFile(), "Unexpected appender file name");

        RollingPolicy rollingPolicy = appender.getRollingPolicy();
        final boolean condition2 = rollingPolicy instanceof RollingPolicyDecorator;
        assertTrue(condition2, "Unexpected rolling policy");
        rollingPolicy = ((RollingPolicyDecorator)rollingPolicy).getDecorated();
        final boolean condition1 = rollingPolicy instanceof AppenderUtils.SimpleRollingPolicy;
        assertTrue(condition1, "Unexpected decorated rolling policy");
        assertEquals(MAX_HISTORY, (long) ((AppenderUtils.SimpleRollingPolicy) rollingPolicy).getMaxIndex(),
                "Unexpected max history");
        assertEquals(_testLogFileName + ".%i",
                ((AppenderUtils.SimpleRollingPolicy) rollingPolicy).getFileNamePattern(),
                "Unexpected file name pattern");
        assertEquals(CompressionMode.NONE, rollingPolicy.getCompressionMode(), "Unexpected compression mode");

        final TriggeringPolicy<?> triggeringPolicy = appender.getTriggeringPolicy();
        assertEquals(MAX_FILE_SIZE + " MB",
                ((AppenderUtils.SizeTriggeringPolicy) triggeringPolicy).getMaxFileSize().toString(),
                "Unexpected triggering policy");

        final Encoder<?> encoder = appender.getEncoder();
        final boolean condition = encoder instanceof PatternLayoutEncoder;
        assertTrue(condition, "Unexpected encoder");
        assertEquals(LAYOUT, ((PatternLayoutEncoder)encoder).getPattern(), "Unexpected layout pattern");
    }

    @Test
    public void testMaxFileSizeLimit()
    {
        assertThrows(IllegalConfigurationException.class,
                () -> AppenderUtils.validateMaxFileSize(0),
                "Exception not thrown");
    }

    @Test
    public void testUnwritableLogFileTarget() throws Exception
    {
        final File unwriteableFile = File.createTempFile(getTestName(), null);

        try
        {
            assertTrue(unwriteableFile.setWritable(false), "could not set log target permissions for test");
            doValidateLogTarget(unwriteableFile);
        }
        finally
        {
            unwriteableFile.delete();
        }
    }

    @Test
    public void testUnwritableLogDirectoryTarget() throws Exception
    {
        final Path unwriteableLogTargetPath = Files.createTempDirectory(getTestName());
        final File unwriteableLogTarget = unwriteableLogTargetPath.toFile();

        try
        {
            if (unwriteableLogTarget.setWritable(false))
            {
                doValidateLogTarget(new File(unwriteableLogTarget.getAbsolutePath(), "nonExistingFile.log"));
            }
            else
            {
                LOGGER.warn("could not set permissions on temporary directory - test skipped");
            }
        }
        finally
        {
            unwriteableLogTarget.delete();
        }
    }

    @Test
    public void testDirectoryLogTarget() throws Exception
    {
        final Path unwriteableLogTargetPath = Files.createTempDirectory(getTestName());
        final File unwriteableLogTarget = unwriteableLogTargetPath.toFile();

        try
        {
            doValidateLogTarget(unwriteableLogTargetPath.toFile());
        }
        finally
        {
            unwriteableLogTarget.delete();
        }
    }

    private void doValidateLogTarget(File target)
    {
        assertThrows(IllegalConfigurationException.class,
                () -> AppenderUtils.validateLogFilePermissions(target),
                "Exception not thrown");
    }
}
