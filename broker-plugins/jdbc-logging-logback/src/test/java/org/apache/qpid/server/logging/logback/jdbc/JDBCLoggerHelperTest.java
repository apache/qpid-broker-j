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

import static org.apache.qpid.server.logging.logback.jdbc.JDBCLoggerHelper.ROOT_LOGGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.db.DBAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.db.ConnectionSource;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.store.jdbc.JDBCSettings;

public class JDBCLoggerHelperTest extends InMemoryDatabaseTestBase
{
    static final String INVALID_JDBC_URL = "jdbc:invalid";
    private JDBCSettings _jdbcSettings;
    private JDBCLoggerHelper _jdbcLoggerHelper;
    private Broker<?> _broker;

    @Before
    public void setUp()
    {
        _jdbcSettings = mock(JDBCSettings.class);
        when(_jdbcSettings.getConnectionUrl()).thenReturn(getTestDatabaseUrl());
        _jdbcLoggerHelper = new JDBCLoggerHelper();
        _broker = BrokerTestHelper.createBrokerMock();
    }

    @Test
    public void createAppenderInstance()
    {
        final LoggerContext context = ROOT_LOGGER.getLoggerContext();
        final Appender<ILoggingEvent> appender =
                _jdbcLoggerHelper.createAppenderInstance(context, _broker, _jdbcSettings);
        assertTrue(appender instanceof DBAppender);
        assertTrue(appender.isStarted());
        assertEquals(context, appender.getContext());
        assertTrue(((DBAppender) appender).getConnectionSource() instanceof JDBCSettingsDrivenConnectionSource);
    }

    @Test
    public void restartAppenderIfExists()
    {
        final Appender appender = mock(Appender.class);
        _jdbcLoggerHelper.restartAppenderIfExists(appender);
        verify(appender).stop();
        verify(appender).start();
        verifyNoMoreInteractions(appender);
    }

    @Test
    public void restartConnectionSourceIfExists()
    {
        final ConnectionSource connectionSource = mock(ConnectionSource.class);
        final DBAppender appender = mock(DBAppender.class);
        when(appender.getConnectionSource()).thenReturn(connectionSource);
        _jdbcLoggerHelper.restartConnectionSourceIfExists(appender);
        verify(connectionSource).stop();
        verify(connectionSource).start();
        verifyNoMoreInteractions(connectionSource);
    }

    @Test
    public void validateConnectionSourceSettings()
    {
        _jdbcLoggerHelper.validateConnectionSourceSettings(_broker, _jdbcSettings);
    }

    @Test
    public void validateConnectionSourceSettingsForInvalidURL()
    {
        when(_jdbcSettings.getConnectionUrl()).thenReturn(INVALID_JDBC_URL);
        try
        {
            _jdbcLoggerHelper.validateConnectionSourceSettings(_broker, _jdbcSettings);
            fail("Exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }
}
