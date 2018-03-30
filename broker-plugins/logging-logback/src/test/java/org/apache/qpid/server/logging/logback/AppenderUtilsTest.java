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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

    @Before
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

    @After
    public void tearDown() throws Exception
    {
        _testLogFile.delete();
    }

    @Test
    public void testCreateRollingFileAppenderDailyRolling()
    {
        final RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        AppenderUtils.configureRollingFileAppender(_settings, new LoggerContext(), appender);

        assertEquals("Unexpected appender file name", _testLogFileName, appender.getFile());

        RollingPolicy rollingPolicy = appender.getRollingPolicy();
        final boolean condition2 = rollingPolicy instanceof RollingPolicyDecorator;
        assertTrue("Unexpected rolling policy", condition2);
        rollingPolicy = ((RollingPolicyDecorator)rollingPolicy).getDecorated();
        final boolean condition1 = rollingPolicy instanceof TimeBasedRollingPolicy;
        assertTrue("Unexpected decorated rolling policy", condition1);
        assertEquals("Unexpected max history",
                            (long) MAX_HISTORY,
                            (long) ((TimeBasedRollingPolicy) rollingPolicy).getMaxHistory());

        assertEquals("Unexpected file name pattern",
                            _testLogFileName + ".%d{yyyy-MM-dd}.%i.gz",
                            ((TimeBasedRollingPolicy) rollingPolicy).getFileNamePattern());

        assertEquals("Unexpected compression mode", CompressionMode.GZ, rollingPolicy.getCompressionMode());

        TriggeringPolicy triggeringPolicy = ((TimeBasedRollingPolicy) rollingPolicy).getTimeBasedFileNamingAndTriggeringPolicy();
        final boolean condition = triggeringPolicy instanceof AppenderUtils.DailyTriggeringPolicy;
        assertTrue("Unexpected triggering policy", condition);
        assertEquals("Unexpected triggering policy",
                            String.valueOf(MAX_FILE_SIZE) + " MB",
                            ((AppenderUtils.DailyTriggeringPolicy) triggeringPolicy).getMaxFileSize().toString());
        assertEquals("Unexpected layout", LAYOUT, ((PatternLayoutEncoder) appender.getEncoder()).getPattern());
    }

    @Test
    public void testCreateRollingFileAppenderNonDailyRolling()
    {
        when(_settings.isRollDaily()).thenReturn(Boolean.FALSE);
        when(_settings.isCompressOldFiles()).thenReturn(Boolean.FALSE);

        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        AppenderUtils.configureRollingFileAppender(_settings, new LoggerContext(), appender);

        assertEquals("Unexpected appender file name", _testLogFileName, appender.getFile());

        RollingPolicy rollingPolicy = appender.getRollingPolicy();
        final boolean condition2 = rollingPolicy instanceof RollingPolicyDecorator;
        assertTrue("Unexpected rolling policy", condition2);
        rollingPolicy = ((RollingPolicyDecorator)rollingPolicy).getDecorated();
        final boolean condition1 = rollingPolicy instanceof AppenderUtils.SimpleRollingPolicy;
        assertTrue("Unexpected decorated rolling policy", condition1);
        assertEquals("Unexpected max history",
                            (long) MAX_HISTORY,
                            (long) ((AppenderUtils.SimpleRollingPolicy) rollingPolicy).getMaxIndex());
        assertEquals("Unexpected file name pattern",
                            _testLogFileName + ".%i",
                            ((AppenderUtils.SimpleRollingPolicy) rollingPolicy).getFileNamePattern());
        assertEquals("Unexpected compression mode", CompressionMode.NONE, rollingPolicy.getCompressionMode());

        TriggeringPolicy triggeringPolicy = appender.getTriggeringPolicy();
        assertEquals("Unexpected triggering policy",
                            String.valueOf(MAX_FILE_SIZE) + " MB",
                            ((AppenderUtils.SizeTriggeringPolicy) triggeringPolicy).getMaxFileSize().toString());

        Encoder encoder = appender.getEncoder();
        final boolean condition = encoder instanceof PatternLayoutEncoder;
        assertTrue("Unexpected encoder", condition);
        assertEquals("Unexpected layout pattern", LAYOUT, ((PatternLayoutEncoder)encoder).getPattern());
    }

    @Test
    public void testMaxFileSizeLimit() throws Exception
    {
        try
        {
            AppenderUtils.validateMaxFileSize(0);
            fail("exception not thrown.");
        }
        catch (IllegalConfigurationException ice)
        {
            // pass
        }
    }

    @Test
    public void testUnwritableLogFileTarget() throws Exception
    {
        File unwriteableFile = File.createTempFile(getTestName(), null);

        try
        {
            assertTrue("could not set log target permissions for test", unwriteableFile.setWritable(false));
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
        Path unwriteableLogTargetPath = Files.createTempDirectory(getTestName());
        File unwriteableLogTarget = unwriteableLogTargetPath.toFile();

        try
        {
            if(unwriteableLogTarget.setWritable(false))
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
        Path unwriteableLogTargetPath = Files.createTempDirectory(getTestName());
        File unwriteableLogTarget = unwriteableLogTargetPath.toFile();

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
        try
        {
            AppenderUtils.validateLogFilePermissions(target);
            fail("test did not throw");
        }
        catch (IllegalConfigurationException ice)
        {
            // pass
        }
    }

}
