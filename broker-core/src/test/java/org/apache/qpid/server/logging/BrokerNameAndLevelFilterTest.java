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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.test.utils.QpidTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerNameAndLevelFilterTest extends QpidTestCase
{
    private Logger _logger = null;
    private Logger _nonQpidLogger = null;

    private BrokerLogger<?> _brokerLogger;
    private BrokerNameAndLevelFilter<?> _brokerNameAndLevelFilter;
    private ListAppender _loggerAppender;
    private TaskExecutor _taskExecutor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _loggerAppender = new ListAppender();

        _taskExecutor =  new TaskExecutorImpl();
        _taskExecutor.start();

        Model model = BrokerModel.getInstance();

        SecurityManager securityManager = mock(SecurityManager.class);
        Broker<?> broker = mock(Broker.class);
        when(broker.getSecurityManager()).thenReturn(securityManager);
        when(broker.getModel()).thenReturn(model);
        doReturn(Broker.class).when(broker).getCategoryClass();

        _brokerLogger = mock(BrokerLogger.class);
        when(_brokerLogger.getModel()).thenReturn(model);
        when(_brokerLogger.getChildExecutor()).thenReturn(_taskExecutor);
        when(_brokerLogger.getParent(Broker.class)).thenReturn(broker);
        doReturn(BrokerLogger.class).when(_brokerLogger).getCategoryClass();


        _logger = LoggerFactory.getLogger("org.apache.qpid.server.test");
        _nonQpidLogger = LoggerFactory.getLogger("org.apache.qpid.test");
    }

    private void setUpFilterAndAppender(String loggerName, LogLevel logLevel)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("loggerName", loggerName);
        attributes.put("level", logLevel);
        attributes.put("name", "test");

        _brokerNameAndLevelFilter = new BrokerNameAndLevelFilterImplFactory().createInstance(attributes, _brokerLogger);
        _brokerNameAndLevelFilter.open();
        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(_loggerAppender);
        _loggerAppender.setContext(rootLogger.getLoggerContext());
        _loggerAppender.addFilter(_brokerNameAndLevelFilter.asFilter());
        _loggerAppender.addFilter(DenyAllFilter.getInstance());
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _brokerNameAndLevelFilter.close();
            _taskExecutor.stopImmediately();

            ch.qos.logback.classic.Logger rootLogger =
                    (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

            _loggerAppender.clearAllFilters();

            _loggerAppender.stop();
            rootLogger.detachAppender(_loggerAppender);
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testSetNameOnWildcardFilter() throws Exception
    {
        setUpFilterAndAppender("org.apache.qpid.server.*", LogLevel.DEBUG);
        _loggerAppender.start();
        _logger.debug("Test1");
        _nonQpidLogger.debug("Test2");
        _loggerAppender.stop();

        assertLoggedEvent(true, "Test1", _logger.getName(), Level.DEBUG);
        assertLoggedEvent(false, "Test2", _nonQpidLogger.getName(), Level.DEBUG);

        try
        {
            _brokerNameAndLevelFilter.setAttributes(Collections.<String, Object>singletonMap("loggerName", "org.apache.qpid.*"));
            fail("Changing of logger name is unsupported");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testSetLevelOnWildcardFilter() throws Exception
    {
        setUpFilterAndAppender("org.apache.qpid.server.*", LogLevel.DEBUG);
        doChangeLevelTest();
    }

    public void testSetLevelOnEmptyLogNameFilter() throws Exception
    {
        setUpFilterAndAppender("", LogLevel.DEBUG);
        doChangeLevelTest();
    }

    public void testSetLevelOnNonEmptyAndNonWildCardLogNameFilter() throws Exception
    {
        setUpFilterAndAppender(_logger.getName(), LogLevel.DEBUG);
        doChangeLevelTest();
    }

    private void doChangeLevelTest()
    {
        _loggerAppender.start();
        _logger.debug("Test1");
        _loggerAppender.stop();

        assertLoggedEvent(true, "Test1", _logger.getName(), Level.DEBUG);

        _brokerNameAndLevelFilter.setAttributes(Collections.<String, Object>singletonMap("level", LogLevel.INFO));
        assertEquals("Unexpected log level attribute", LogLevel.INFO, _brokerNameAndLevelFilter.getLevel());

        _loggerAppender.start();
        _logger.debug("Test2");
        _logger.info("Test3");
        _loggerAppender.stop();

        assertLoggedEvent(false, "Test2", _logger.getName(), Level.DEBUG);
        assertLoggedEvent(true, "Test3", _logger.getName(), Level.INFO);
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
