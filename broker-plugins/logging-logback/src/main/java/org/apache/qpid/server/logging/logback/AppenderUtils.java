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

import java.io.File;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RollingPolicyBase;
import ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.rolling.TriggeringPolicy;
import ch.qos.logback.core.util.FileSize;

import org.apache.qpid.server.configuration.IllegalConfigurationException;

public class AppenderUtils
{
    public static void configureRollingFileAppender(FileLoggerSettings fileLoggerSettings,
                                                    Context loggerContext,
                                                    RollingFileAppender<ILoggingEvent> appender)
    {
        String fileName = fileLoggerSettings.getFileName();
        File file = new File(fileName);
        if (file.getParentFile() != null)
        {
            file.getParentFile().mkdirs();
        }
        validateLogFilePermissions(file);
        validateMaxFileSize(fileLoggerSettings.getMaxFileSize());

        appender.setFile(fileName);
        appender.setAppend(true);
        appender.setContext(loggerContext);

        TriggeringPolicy triggeringPolicy;
        RollingPolicyBase rollingPolicy;
        final String maxFileSizeAsString = String.valueOf(fileLoggerSettings.getMaxFileSize()) + "MB";
        if(fileLoggerSettings.isRollDaily())
        {
            DailyTriggeringPolicy dailyTriggeringPolicy = new DailyTriggeringPolicy(fileLoggerSettings.isRollOnRestart(), maxFileSizeAsString);
            dailyTriggeringPolicy.setContext(loggerContext);
            TimeBasedRollingPolicy<ILoggingEvent> timeBasedRollingPolicy = new TimeBasedRollingPolicy<>();
            timeBasedRollingPolicy.setMaxHistory(fileLoggerSettings.getMaxHistory());
            timeBasedRollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(dailyTriggeringPolicy);
            timeBasedRollingPolicy.setFileNamePattern(fileName + ".%d{yyyy-MM-dd}.%i" + (fileLoggerSettings.isCompressOldFiles()
                    ? ".gz"
                    : ""));
            rollingPolicy = timeBasedRollingPolicy;
            triggeringPolicy = dailyTriggeringPolicy;
        }
        else
        {
            SizeTriggeringPolicy sizeTriggeringPolicy = new SizeTriggeringPolicy(fileLoggerSettings.isRollOnRestart(), maxFileSizeAsString);
            sizeTriggeringPolicy.setContext(loggerContext);
            SimpleRollingPolicy simpleRollingPolicy = new SimpleRollingPolicy(fileLoggerSettings.getMaxHistory());
            simpleRollingPolicy.setFileNamePattern(fileName + ".%i" + (fileLoggerSettings.isCompressOldFiles() ? ".gz" : ""));
            rollingPolicy = simpleRollingPolicy;
            triggeringPolicy = sizeTriggeringPolicy;
        }

        rollingPolicy.setContext(loggerContext);
        RollingPolicyDecorator decorator = new RollingPolicyDecorator(rollingPolicy, fileLoggerSettings.getRolloverListener(), fileLoggerSettings.getExecutorService());
        decorator.setParent(appender);
        appender.setRollingPolicy(decorator);
        appender.setTriggeringPolicy(triggeringPolicy);
        decorator.start();
        triggeringPolicy.start();

        final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern(fileLoggerSettings.getLayout());
        encoder.setContext(loggerContext);
        encoder.start();
        appender.setEncoder(encoder);
    }

    static void validateLogFilePermissions(final File file)
    {
        if ((file.exists() && (!file.isFile() || !file.canWrite())) || !file.getAbsoluteFile().getParentFile().canWrite())
        {
            throw new IllegalConfigurationException(String.format("Do not have the permissions to log to file '%s'.", file.getAbsolutePath()));
        }
    }

    static void validateMaxFileSize(final int maxFileSize)
    {
        if (maxFileSize < 1)
        {
            throw new IllegalConfigurationException(String.format("Maximum file size must be at least 1. Cannot set to %d.", maxFileSize));
        }
    }

    static class DailyTriggeringPolicy extends SizeAndTimeBasedFNATP<ILoggingEvent>
    {
        private final boolean _rollOnRestart;
        private final FileSize _maxFileSize;
        private volatile boolean _isFirst = true;

        public DailyTriggeringPolicy(boolean isRollOnRestart, String maxFileSize)
        {
            _rollOnRestart = isRollOnRestart;
            _maxFileSize = FileSize.valueOf(maxFileSize);
            setMaxFileSize(_maxFileSize);
        }

        @Override
        public void start()
        {
            super.start();
            if (_rollOnRestart)
            {
                nextCheck = 0L;
            }
        }

        @Override
        public boolean isTriggeringEvent(final File activeFile, final ILoggingEvent event)
        {
            if (_rollOnRestart && _isFirst)
            {
                _isFirst = false;
                if (activeFile != null && activeFile.exists() && activeFile.length() == 0)
                {
                    computeNextCheck();
                    return false;
                }
            }

            return super.isTriggeringEvent(activeFile, event);
        }

        public FileSize getMaxFileSize()
        {
            return _maxFileSize;
        }
    }

    static class SizeTriggeringPolicy extends SizeBasedTriggeringPolicy<ILoggingEvent>
    {
        private final boolean _rollOnRestart;
        private final FileSize _maxFileSize;
        private volatile boolean _isFirst = true;

        public SizeTriggeringPolicy(boolean isRollOnRestart, String maxFileSize)
        {
            _rollOnRestart = isRollOnRestart;
            _maxFileSize = FileSize.valueOf(maxFileSize);
            setMaxFileSize(_maxFileSize);

        }

        @Override
        public boolean isTriggeringEvent(final File activeFile, final ILoggingEvent event)
        {
            if (_rollOnRestart && _isFirst)
            {
                _isFirst = false;
                return activeFile != null && activeFile.exists() && activeFile.length() != 0;
            }
            else
            {
                return super.isTriggeringEvent(activeFile, event);
            }
        }

        public FileSize getMaxFileSize()
        {
            return _maxFileSize;
        }
    }

    static class SimpleRollingPolicy extends FixedWindowRollingPolicy
    {
        private final int _maxFiles;

        public SimpleRollingPolicy(int maxHistory)
        {
            _maxFiles = maxHistory;
            setMaxIndex(maxHistory);
            setMinIndex(1);
        }

        @Override
        protected int getMaxWindowSize()
        {
            return _maxFiles;
        }
    }

}
