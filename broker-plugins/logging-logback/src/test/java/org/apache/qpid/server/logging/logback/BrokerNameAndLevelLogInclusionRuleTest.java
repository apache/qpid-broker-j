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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("unchecked")
public class BrokerNameAndLevelLogInclusionRuleTest extends UnitTestBase
{
    private BrokerLogger<?> _brokerLogger;
    private TaskExecutor _taskExecutor;
    private final Broker<?> _broker = mock(Broker.class);

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor =  new TaskExecutorImpl();
        _taskExecutor.start();

        final Model model = BrokerModel.getInstance();

        when(_broker.getModel()).thenReturn(model);
        doReturn(Broker.class).when(_broker).getCategoryClass();

        _brokerLogger = mock(BrokerLogger.class);
        when(_brokerLogger.getModel()).thenReturn(model);
        when(_brokerLogger.getChildExecutor()).thenReturn(_taskExecutor);
        when(_brokerLogger.getParent()).thenReturn((Broker)_broker);
        doReturn(BrokerLogger.class).when(_brokerLogger).getCategoryClass();
   }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
    }

    @Test
    public void testAsFilter()
    {
        final BrokerNameAndLevelLogInclusionRule<?> rule = createRule("org.apache.qpid", LogLevel.INFO);
        final Filter<ILoggingEvent> filter = rule.asFilter();

        final boolean condition = filter instanceof LoggerNameAndLevelFilter;
        assertTrue(condition, "Unexpected filter instance");

        final LoggerNameAndLevelFilter f = (LoggerNameAndLevelFilter)filter;
        assertEquals(Level.INFO, f.getLevel(), "Unexpected log level");
        assertEquals("org.apache.qpid", f.getLoggerName(), "Unexpected logger name");
    }

    @Test
    public void testLevelChangeAffectsFilter()
    {
        final BrokerNameAndLevelLogInclusionRule<?> rule = createRule("org.apache.qpid", LogLevel.INFO);
        final LoggerNameAndLevelFilter filter = (LoggerNameAndLevelFilter)rule.asFilter();

        assertEquals(Level.INFO, filter.getLevel(), "Unexpected log level");

        rule.setAttributes(Collections.singletonMap("level", LogLevel.DEBUG));
        assertEquals(Level.DEBUG, filter.getLevel(), "Unexpected log level attribute");
    }

    @Test
    public void testLoggerNameChangeNotAllowed()
    {
        final BrokerNameAndLevelLogInclusionRule<?> rule = createRule("org.apache.qpid", LogLevel.INFO);
        final LoggerNameAndLevelFilter filter = (LoggerNameAndLevelFilter)rule.asFilter();

        assertEquals("org.apache.qpid", filter.getLoggerName(), "Unexpected logger name");

        assertThrows(IllegalConfigurationException.class,
                () -> rule.setAttributes(Map.of(BrokerNameAndLevelLogInclusionRule.LOGGER_NAME, "org.apache.qpid.foo")),
                "IllegalConfigurationException is expected to throw on attempt to change logger name");

        assertEquals("org.apache.qpid", filter.getLoggerName(), "Unexpected logger name");
    }


    private BrokerNameAndLevelLogInclusionRule<?> createRule(String loggerName, LogLevel logLevel)
    {
        final Map<String, Object> attributes = Map.of("loggerName", loggerName,
                "level", logLevel,
                "name", "test");
        final BrokerNameAndLevelLogInclusionRule<?> brokerNameAndLevelLogInclusionRule =
                new BrokerNameAndLevelLogInclusionRuleImpl(attributes, _brokerLogger);
        brokerNameAndLevelLogInclusionRule.open();
        return brokerNameAndLevelLogInclusionRule;
    }
}
