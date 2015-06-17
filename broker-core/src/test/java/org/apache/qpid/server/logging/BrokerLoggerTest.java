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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.read.ListAppender;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLoggerFilter;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.test.utils.QpidTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerLoggerTest extends QpidTestCase
{
    public static final String APPENDER_NAME = "test";

    private AbstractBrokerLogger<?> _brokerLogger;
    private ListAppender _loggerAppender;
    private TaskExecutor _taskExecutor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _loggerAppender = new ListAppender();
        _loggerAppender.setName(APPENDER_NAME);

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        Model model = BrokerModel.getInstance();

        org.apache.qpid.server.security.SecurityManager securityManager = mock(SecurityManager.class);
        Broker<?> broker = mock(Broker.class);
        when(broker.getSecurityManager()).thenReturn(securityManager);
        when(broker.getModel()).thenReturn(model);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);
        doReturn(Broker.class).when(broker).getCategoryClass();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", APPENDER_NAME);
        _brokerLogger = new AbstractBrokerLogger(attributes, broker)
        {
            @Override
            public Appender<ILoggingEvent> createAppenderInstance(Context context)
            {
                return _loggerAppender;
            }
        };
        _brokerLogger.open();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _brokerLogger.delete();
            _taskExecutor.stopImmediately();
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testAddNewFilter()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("loggerName", "org.apache.qpid");
        attributes.put("level", LogLevel.INFO);
        attributes.put("name", "test");
        attributes.put("type", BrokerNameAndLevelFilter.TYPE);

        Collection<BrokerLoggerFilter> filtersBefore = _brokerLogger.getChildren(BrokerLoggerFilter.class);
        assertEquals("Unexpected number of filters before creation", 0, filtersBefore.size());

        BrokerLoggerFilter<?> createdFilter = _brokerLogger.createChild(BrokerLoggerFilter.class, attributes);
        assertEquals("Unexpected filter name", "test", createdFilter.getName());

        Collection<BrokerLoggerFilter> filtersAfter = _brokerLogger.getChildren(BrokerLoggerFilter.class);
        assertEquals("Unexpected number of filters after creation", 1, filtersAfter.size());

        BrokerLoggerFilter filter = filtersAfter.iterator().next();
        assertEquals("Unexpected filter", createdFilter, filter);

        Logger logger = LoggerFactory.getLogger("org.apache.qpid");

        logger.debug("Test2");
        logger.info("Test3");
        assertLoggedEvent(false, "Test2", logger.getName(), Level.DEBUG);
        assertLoggedEvent(true, "Test3", logger.getName(), Level.INFO);
    }

    public void testRemoveExistingFilter()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("loggerName", "org.apache.qpid");
        attributes.put("level", LogLevel.INFO);
        attributes.put("name", "test");
        attributes.put("type", BrokerNameAndLevelFilter.TYPE);
        BrokerLoggerFilter<?> createdFilter = _brokerLogger.createChild(BrokerLoggerFilter.class, attributes);
        Logger logger = LoggerFactory.getLogger("org.apache.qpid");

        logger.info("Test1");
        assertLoggedEvent(true, "Test1", logger.getName(), Level.INFO);

        createdFilter.delete();

        logger.info("Test2");
        assertLoggedEvent(false, "Test2", logger.getName(), Level.INFO);
    }

    public void testDeleteLogger()
    {
        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        assertNotNull("Appender not found when it should have been created", rootLogger.getAppender(_brokerLogger.getName()));
        _brokerLogger.delete();
        assertEquals("Unexpected state after deletion", State.DELETED, _brokerLogger.getState());
        assertNull("Appender found when it should have been deleted", rootLogger.getAppender(_brokerLogger.getName()));
    }

    private void assertLoggedEvent(boolean exists, String message, String loggerName, Level level)
    {
        List<ILoggingEvent> events;
        synchronized(_loggerAppender)
        {
            events = new ArrayList<>(_loggerAppender.list);
        }

        boolean logged = false;
        for (ILoggingEvent event: events)
        {
            if (event.getFormattedMessage().equals(message) && event.getLoggerName().equals(loggerName) && event.getLevel() == level)
            {
                logged = true;
                break;
            }
        }
        assertEquals("Event " + message + " from logger " + loggerName + " of log level " + level
                + " is " + (exists ? "not" : "") + " found", exists, logged);
    }
}
