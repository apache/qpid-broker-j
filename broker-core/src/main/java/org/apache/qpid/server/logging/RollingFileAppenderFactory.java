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
import org.apache.qpid.server.logging.logback.RollingPolicyDecorator;

public class RollingFileAppenderFactory
{
    public RollingFileAppender<ILoggingEvent> createRollingFileAppender(FileLoggerSettings fileLoggerSettings, Context loggerContext)
    {
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setFile(fileLoggerSettings.getFileName());
        appender.setAppend(true);
        appender.setContext(loggerContext);

        RollingPolicyBase policy;
        if(fileLoggerSettings.isRollDaily())
        {
            DailyTriggeringPolicy triggeringPolicy = new DailyTriggeringPolicy(fileLoggerSettings.isRollOnRestart(), fileLoggerSettings.getMaxFileSize());
            triggeringPolicy.setContext(loggerContext);
            TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
            rollingPolicy.setMaxHistory(fileLoggerSettings.getMaxHistory());
            rollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(triggeringPolicy);
            rollingPolicy.setFileNamePattern(fileLoggerSettings.getFileName() + ".%d{yyyy-MM-dd}.%i" + (fileLoggerSettings.isCompressOldFiles()
                    ? ".gz"
                    : ""));
            policy = rollingPolicy;
        }
        else
        {
            SizeTriggeringPolicy sizeTriggeringPolicy = new SizeTriggeringPolicy(fileLoggerSettings.isRollOnRestart(), fileLoggerSettings.getMaxFileSize());
            sizeTriggeringPolicy.setContext(loggerContext);
            SimpleRollingPolicy rollingPolicy = new SimpleRollingPolicy(fileLoggerSettings.getMaxHistory());
            rollingPolicy.setFileNamePattern(fileLoggerSettings.getFileName() + ".%i" + (fileLoggerSettings.isCompressOldFiles() ? ".gz" : ""));
            appender.setTriggeringPolicy(sizeTriggeringPolicy);
            sizeTriggeringPolicy.start();
            policy = rollingPolicy;
        }
        policy.setContext(loggerContext);
        RollingPolicyDecorator decorator = new RollingPolicyDecorator(policy, fileLoggerSettings.getRolloverListener(), fileLoggerSettings.getExecutorService());
        decorator.setParent(appender);
        appender.setRollingPolicy(decorator);
        decorator.start();

        final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern(fileLoggerSettings.getLayout());
        encoder.setContext(loggerContext);
        encoder.start();
        appender.setEncoder(encoder);
        return appender;
    }


    class DailyTriggeringPolicy extends SizeAndTimeBasedFNATP<ILoggingEvent>
    {
        private final boolean _rollOnRestart;
        private boolean _isFirst = true;

        public DailyTriggeringPolicy(boolean isRollOnRestart, String maxFileSize)
        {
            _rollOnRestart = isRollOnRestart;
            setMaxFileSize(maxFileSize);
        }

        @Override
        protected void computeNextCheck()
        {
            super.computeNextCheck();
            if (_rollOnRestart && _isFirst)
            {
                _isFirst = false;
                nextCheck = 0l;
            }
        }

        @Override
        public boolean isTriggeringEvent(final File activeFile, final ILoggingEvent event)
        {
            if (_rollOnRestart && _isFirst)
            {
                _isFirst = false;
                return activeFile.exists() && activeFile.length() != 0l;
            }
            else
            {
                return super.isTriggeringEvent(activeFile, event);
            }
        }

    }

    class SizeTriggeringPolicy extends SizeBasedTriggeringPolicy<ILoggingEvent>
    {
        private final boolean _rollOnRestart;
        private boolean _isFirst = true;

        public SizeTriggeringPolicy(boolean isRollOnRestart, String maxFileSize)
        {
            _rollOnRestart = isRollOnRestart;
            setMaxFileSize(maxFileSize);

        }

        @Override
        public boolean isTriggeringEvent(final File activeFile, final ILoggingEvent event)
        {
            if (_rollOnRestart && _isFirst)
            {
                _isFirst = false;
                return activeFile.exists() && activeFile.length() != 0l;
            }
            else
            {
                return super.isTriggeringEvent(activeFile, event);
            }
        }

    }

    class SimpleRollingPolicy extends FixedWindowRollingPolicy
    {
        private int _maxFiles;

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
