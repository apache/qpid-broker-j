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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.SystemConfig;

public class LogbackLoggingSystemLauncherListener implements SystemLauncherListener
{
    private StartupAppender _startupAppender;
    private ch.qos.logback.classic.Logger _logger;
    private Logback1027WorkaroundTurboFilter _logback1027WorkaroundTurboFilter;

    @Override
    public void beforeStartup()
    {
        _logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (!_logger.iteratorForAppenders().hasNext())
        {
            _logger.setLevel(Level.ALL);
            _logger.setAdditive(true);
        }

        final LoggerContext loggerContext = _logger.getLoggerContext();
        _logback1027WorkaroundTurboFilter = new Logback1027WorkaroundTurboFilter();
        loggerContext.addTurboFilter(_logback1027WorkaroundTurboFilter);

        _startupAppender = new StartupAppender();
        _startupAppender.setContext(loggerContext);
        _startupAppender.start();
        _logger.addAppender(_startupAppender);
    }

    @Override
    public void errorOnStartup(final RuntimeException e)
    {
        _startupAppender.logToConsole();
    }

    @Override
    public void afterStartup()
    {
        _logger.detachAppender(_startupAppender);
        _startupAppender.stop();
    }

    @Override
    public void onContainerResolve(SystemConfig<?> systemConfig)
    {
        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

        StartupAppender startupAppender = (StartupAppender) rootLogger.getAppender(StartupAppender.class.getName());
        if (startupAppender != null)
        {
            rootLogger.detachAppender(startupAppender);
            startupAppender.stop();
        }
    }

    @Override
    public void onContainerClose(final SystemConfig<?> systemConfig)
    {
        QpidLoggerTurboFilter.uninstallFromRootContext();
        _logger.getLoggerContext().getTurboFilterList().remove(_logback1027WorkaroundTurboFilter);
    }


    @Override
    public void onShutdown(final int exitCode)
    {
    }

    @Override
    public void exceptionOnShutdown(final Exception e)
    {
    }
}
