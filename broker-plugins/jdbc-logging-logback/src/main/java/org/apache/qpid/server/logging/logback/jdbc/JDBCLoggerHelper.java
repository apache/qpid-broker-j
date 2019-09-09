/*
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
 */

package org.apache.qpid.server.logging.logback.jdbc;

import ch.qos.logback.classic.db.DBAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.db.ConnectionSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.jdbc.JDBCSettings;

class JDBCLoggerHelper
{
    final static ch.qos.logback.classic.Logger ROOT_LOGGER =
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCLoggerHelper.class);

    Appender<ILoggingEvent> createAppenderInstance(final Context context,
                                                   final ConfiguredObject<?> logger,
                                                   final JDBCSettings settings)
    {
        try
        {
            final JDBCSettingsDBNameResolver dbNameResolver = new JDBCSettingsDBNameResolver(settings);
            final ConnectionSource connectionSource = createConnectionSource(context, logger, settings);
            final DBAppender appender = new DBAppender();
            appender.setDbNameResolver(dbNameResolver);
            appender.setConnectionSource(connectionSource);
            appender.setContext(context);
            appender.start();
            return appender;
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to create appender", e);
            throw new IllegalConfigurationException("Cannot create appender");
        }
    }

    void restartAppenderIfExists(final Appender appender)
    {
        if (appender != null)
        {
            appender.stop();
            appender.start();
        }
    }

    void restartConnectionSourceIfExists(final Appender appender)
    {
        if (appender instanceof DBAppender)
        {
            final ConnectionSource connectionSource = ((DBAppender) appender).getConnectionSource();
            if (connectionSource != null)
            {
                connectionSource.stop();
                connectionSource.start();
            }
        }
    }

    void validateConnectionSourceSettings(final ConfiguredObject<?> logger, final JDBCSettings settings)
    {
        try
        {
            final ConnectionSource connectionSource = createConnectionSource(logger, settings);
            connectionSource.getConnection().close();
        }
        catch (Exception e)
        {
            throw new IllegalConfigurationException(
                    "Cannot create connection source from given URL, credentials and connection pool type");
        }
    }

    private ConnectionSource createConnectionSource(final ConfiguredObject<?> logger,
                                                    final JDBCSettings settings)
    {
        return createConnectionSource(ROOT_LOGGER.getLoggerContext(), logger, settings);
    }

    private ConnectionSource createConnectionSource(final Context context,
                                                    final ConfiguredObject<?> logger,
                                                    final JDBCSettings settings)
    {
        final JDBCSettingsDrivenConnectionSource connectionSource =
                new JDBCSettingsDrivenConnectionSource(logger, settings);
        connectionSource.setContext(context);
        connectionSource.start();
        return connectionSource;
    }
}
