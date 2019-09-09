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
import static org.apache.qpid.server.logging.logback.jdbc.JDBCLoggerHelperTest.INVALID_JDBC_URL;
import static org.apache.qpid.server.model.BrokerTestHelper.createBrokerMock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ch.qos.logback.classic.db.DBAppender;
import ch.qos.logback.core.Appender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.store.jdbc.JDBCSettings;

public class JDBCBrokerLoggerImplTest extends InMemoryDatabaseTestBase
{
    private static final String TABLE_PREFIX = "broker_";

    private JDBCBrokerLoggerImpl _logger;

    @Before
    public void setUp()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(JDBCBrokerLoggerImpl.NAME, getTestName());
        attributes.put(JDBCSettings.CONNECTION_URL, getTestDatabaseUrl());
        _logger = new JDBCBrokerLoggerImpl(attributes, createBrokerMock());
    }

    @After
    public void tearDown()
    {
        if (_logger != null)
        {
            _logger.close();
        }
    }

    @Test
    public void createAppenderOnCreate()
    {
        _logger.create();
        final Appender appender = ROOT_LOGGER.getAppender(getTestName());
        assertTrue(appender instanceof DBAppender);
    }

    @Test
    public void createAppenderOnOpen()
    {
        _logger.open();
        final Appender appender = ROOT_LOGGER.getAppender(getTestName());
        assertTrue(appender instanceof DBAppender);
    }

    @Test
    public void closeAndReopenLogger()
    {
        _logger.create();
        _logger.close();
        final Appender appender = ROOT_LOGGER.getAppender(getTestName());
        assertNotNull(appender);
        _logger.open();
        assertSame(appender, ROOT_LOGGER.getAppender(getTestName()));
    }

    @Test
    public void detachAppenderInstanceOnDelete()
    {
        _logger.create();
        _logger.delete();
        final Appender appender = ROOT_LOGGER.getAppender(getTestName());
        assertNull(appender);
    }

    @Test
    public void createLoggerWithInvalidURL()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(JDBCBrokerLoggerImpl.NAME, getTestName());
        attributes.put(JDBCSettings.CONNECTION_URL, INVALID_JDBC_URL);
        final JDBCBrokerLoggerImpl logger = new JDBCBrokerLoggerImpl(attributes, createBrokerMock());
        try
        {
            logger.create();
            fail("Exception should be thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void changeLoggerURLtoInvalid()
    {
        _logger.create();
        final Map<String, Object> attributes = Collections.singletonMap(JDBCSettings.CONNECTION_URL, INVALID_JDBC_URL);
        try
        {
            _logger.setAttributes(attributes);
            fail("Exception should be thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void changeTablePrefix()
    {
        _logger.create();

        final Map<String, Object> attributes = Collections.singletonMap(JDBCSettings.TABLE_NAME_PREFIX, TABLE_PREFIX);
        _logger.setAttributes(attributes);

        assertEquals(TABLE_PREFIX, _logger.getTableNamePrefix());
    }
}
