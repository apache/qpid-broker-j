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
import java.util.Map;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLoggerFilter;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

public class BrokerFileLoggerImpl extends AbstractConfiguredObject<BrokerFileLoggerImpl> implements BrokerFileLogger<BrokerFileLoggerImpl>
{
    @ManagedAttributeField
    private String _layout;
    @ManagedAttributeField
    private String _fileName;
    @ManagedAttributeField
    private boolean _rollDaily;
    @ManagedAttributeField
    private boolean _rollOnRestart;
    @ManagedAttributeField
    private boolean _compressOldFiles;
    @ManagedAttributeField
    private int _maxHistory;
    @ManagedAttributeField
    private String _maxFileSize;
    @ManagedAttributeField
    private boolean _safeMode;

    @ManagedObjectFactoryConstructor
    protected BrokerFileLoggerImpl(final Map<String, Object> attributes, Broker<?> broker)
    {
        super(parentsMap(broker),attributes);
    }

    @Override
    public String getFileName()
    {
        return _fileName;
    }

    @Override
    public boolean isRollDaily()
    {
        return _rollDaily;
    }

    @Override
    public boolean isRollOnRestart()
    {
        return _rollOnRestart;
    }

    @Override
    public boolean isCompressOldFiles()
    {
        return _compressOldFiles;
    }

    @Override
    public int getMaxHistory()
    {
        return _maxHistory;
    }

    @Override
    public String getMaxFileSize()
    {
        return _maxFileSize;
    }

    @Override
    public boolean isSafeMode()
    {
        return _safeMode;
    }

    @Override
    public String getLayout()
    {
        return _layout;
    }

    @Override
    public Appender<ILoggingEvent> asAppender()
    {
        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        LoggerContext loggerContext = rootLogger.getLoggerContext();
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setFile(getFileName());
        appender.setAppend(true);
        appender.setContext(loggerContext);

        for(BrokerLoggerFilter<?> filter : getChildren(BrokerLoggerFilter.class))
        {
            appender.addFilter(filter.asFilter());
        }
        appender.addFilter(DenyAllFilter.getInstance());
        appender.setName(getName());

        if(isRollDaily())
        {
            DailyTriggeringPolicy triggeringPolicy = new DailyTriggeringPolicy();
            triggeringPolicy.setContext(loggerContext);
            TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
            rollingPolicy.setContext(loggerContext);
            rollingPolicy.setMaxHistory(getMaxHistory());
            rollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(triggeringPolicy);
            rollingPolicy.setFileNamePattern(getFileName() + ".%d{yyyy-MM-dd}.%i" + (isCompressOldFiles()
                    ? ".gz"
                    : ""));
            appender.setRollingPolicy(rollingPolicy);
            rollingPolicy.setParent(appender);
            rollingPolicy.start();
        }
        else
        {
            SizeTriggeringPolicy sizeTriggeringPolicy = new SizeTriggeringPolicy();
            sizeTriggeringPolicy.setContext(loggerContext);
            SimpleRollingPolicy rollingPolicy = new SimpleRollingPolicy();
            rollingPolicy.setContext(loggerContext);
            rollingPolicy.setFileNamePattern(getFileName() + ".%i" + (isCompressOldFiles() ? ".gz" : ""));
            appender.setRollingPolicy(rollingPolicy);
            appender.setTriggeringPolicy(sizeTriggeringPolicy);
            rollingPolicy.setParent(appender);
            rollingPolicy.start();
            sizeTriggeringPolicy.start();
        }
        final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern("%d %-5p [%t] \\(%c{2}\\) - %m%n");
        encoder.setContext(loggerContext);
        encoder.start();
        appender.setEncoder(encoder);
        appender.start();
        return appender;
    }

    private class DailyTriggeringPolicy extends SizeAndTimeBasedFNATP<ILoggingEvent>
    {
        private final boolean _rollOnRestart;
        private boolean _isFirst = true;

        public DailyTriggeringPolicy()
        {
            _rollOnRestart = isRollOnRestart();
            setMaxFileSize(getMaxFileSize());

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


    private class SizeTriggeringPolicy extends SizeBasedTriggeringPolicy<ILoggingEvent>
    {
        private final boolean _rollOnRestart;
        private boolean _isFirst = true;

        public SizeTriggeringPolicy()
        {
            _rollOnRestart = isRollOnRestart();
            setMaxFileSize(getMaxFileSize());

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

    private class SimpleRollingPolicy extends FixedWindowRollingPolicy
    {
        private int _maxFiles;

        public SimpleRollingPolicy()
        {
            _maxFiles = getMaxHistory();
            setMaxIndex(getMaxHistory());
            setMinIndex(1);
        }

        @Override
        protected int getMaxWindowSize()
        {
            return _maxFiles;
        }
    }

}
