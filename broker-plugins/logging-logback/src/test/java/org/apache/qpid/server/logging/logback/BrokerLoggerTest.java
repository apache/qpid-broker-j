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

import static org.apache.qpid.server.util.LoggerTestHelper.assertLoggedEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.read.ListAppender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogInclusionRule;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BrokerLoggerTest extends UnitTestBase
{
    public static final String APPENDER_NAME = "test";

    private AbstractBrokerLogger<?> _brokerLogger;
    private ListAppender _loggerAppender;
    private TaskExecutor _taskExecutor;
    private Broker _broker;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        _loggerAppender = new ListAppender<>();
        _loggerAppender.setName(APPENDER_NAME);

        final Model model = BrokerModel.getInstance();

        _broker = mock(Broker.class);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getTypeClass()).thenReturn(Broker.class);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        doReturn(Broker.class).when(_broker).getCategoryClass();

        final Map<String, Object> attributes = Map.of("name", APPENDER_NAME);
        _brokerLogger = new AbstractBrokerLogger(attributes, _broker)
        {
            @Override
            public Appender<ILoggingEvent> createAppenderInstance(final Context context)
            {
                return _loggerAppender;
            }
        };
        _brokerLogger.open();
    }

    @AfterEach
    public void tearDown()
    {
        _brokerLogger.delete();
        _taskExecutor.stopImmediately();
    }

    @Test
    public void testAddNewLogInclusionRule()
    {
        final Map<String, Object> attributes = createBrokerNameAndLevelLogInclusionRuleAttributes("org.apache.qpid", LogLevel.INFO);

        final Collection<BrokerLogInclusionRule> rulesBefore = _brokerLogger.getChildren(BrokerLogInclusionRule.class);
        assertEquals(0, (long) rulesBefore.size(), "Unexpected number of rules before creation");

        final BrokerLogInclusionRule<?> createdRule = _brokerLogger.createChild(BrokerLogInclusionRule.class, attributes);
        assertEquals("test", createdRule.getName(), "Unexpected rule name");

        final Collection<BrokerLogInclusionRule> rulesAfter = _brokerLogger.getChildren(BrokerLogInclusionRule.class);
        assertEquals(1, (long) rulesAfter.size(), "Unexpected number of rules after creation");

        final BrokerLogInclusionRule filter = rulesAfter.iterator().next();
        assertEquals(createdRule, filter, "Unexpected rule");

        final Logger logger = LoggerFactory.getLogger("org.apache.qpid");

        logger.debug("Test2");
        logger.info("Test3");
        assertLoggedEvent(_loggerAppender, false, "Test2", logger.getName(), Level.DEBUG);
        assertLoggedEvent(_loggerAppender, true, "Test3", logger.getName(), Level.INFO);
    }

    @Test
    public void testRemoveExistingRule()
    {
        final Map<String, Object> attributes = createBrokerNameAndLevelLogInclusionRuleAttributes("org.apache.qpid", LogLevel.INFO);
        final BrokerLogInclusionRule<?> createdRule = _brokerLogger.createChild(BrokerLogInclusionRule.class, attributes);
        final Logger logger = LoggerFactory.getLogger("org.apache.qpid");

        logger.info("Test1");
        assertLoggedEvent(_loggerAppender, true, "Test1", logger.getName(), Level.INFO);

        createdRule.delete();

        logger.info("Test2");
        assertLoggedEvent(_loggerAppender, false, "Test2", logger.getName(), Level.INFO);
    }

    @Test
    public void testDeleteLogger()
    {
        final ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        assertNotNull(rootLogger.getAppender(_brokerLogger.getName()),
                "Appender not found when it should have been created");

        _brokerLogger.delete();
        assertEquals(State.DELETED, _brokerLogger.getState(), "Unexpected state after deletion");
        assertNull(rootLogger.getAppender(_brokerLogger.getName()),
                "Appender found when it should have been deleted");
    }

    @Test
    public void testBrokerMemoryLoggerGetLogEntries()
    {
        final Map<String, Object> attributes = Map.of(BrokerMemoryLogger.NAME, getTestName(),
                ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        final BrokerMemoryLogger logger = new BrokerMemoryLoggerImplFactory().createInstance(attributes, _broker);
        try
        {
            logger.open();

            final Map<String, Object> filterAttributes = createBrokerNameAndLevelLogInclusionRuleAttributes("", LogLevel.ALL);
            logger.createChild(BrokerLogInclusionRule.class, filterAttributes);

            final Logger messageLogger = LoggerFactory.getLogger("org.apache.qpid.test");
            messageLogger.debug("test message 1");
            final Collection<LogRecord> logRecords = logger.getLogEntries(0);

            final LogRecord foundRecord = findLogRecord("test message 1", logRecords);

            assertNotNull(foundRecord, "Record is not found");

            messageLogger.debug("test message 2");

            final Collection<LogRecord> logRecords2 = logger.getLogEntries(foundRecord.getId());
            for (final LogRecord record: logRecords2)
            {
                assertTrue(record.getId() > foundRecord.getId(),
                        "Record id " + record.getId() + " is below " + foundRecord.getId());

            }

            final LogRecord foundRecord2 = findLogRecord("test message 2", logRecords2);

            assertNotNull(foundRecord2, "Record2 is not found");
        }
        finally
        {
            logger.delete();
        }
    }

    @Test
    public void testStatistics()
    {
        Map<String, Object> attributes;
        final String loggerName = getTestName();
        final Logger messageLogger = LoggerFactory.getLogger(loggerName);

        assertEquals(0L, _brokerLogger.getWarnCount());
        assertEquals(0L, _brokerLogger.getErrorCount());

        attributes = createBrokerNameAndLevelLogInclusionRuleAttributes(loggerName, LogLevel.WARN);
        final BrokerLogInclusionRule warnFilter = _brokerLogger.createChild(BrokerLogInclusionRule.class, attributes);

        messageLogger.warn("warn");
        assertEquals(1L, _brokerLogger.getWarnCount());
        assertEquals(0L, _brokerLogger.getErrorCount());

        messageLogger.error("error");
        assertEquals(1L, _brokerLogger.getWarnCount());
        assertEquals(1L, _brokerLogger.getErrorCount());

        warnFilter.delete();

        attributes = createBrokerNameAndLevelLogInclusionRuleAttributes(loggerName, LogLevel.ERROR);
        final BrokerLogInclusionRule errorFilter = _brokerLogger.createChild(BrokerLogInclusionRule.class, attributes);

        messageLogger.warn("warn");
        assertEquals(1L, _brokerLogger.getWarnCount());
        assertEquals(1L, _brokerLogger.getErrorCount());
        messageLogger.error("error");
        assertEquals(1L, _brokerLogger.getWarnCount());
        assertEquals(2L, _brokerLogger.getErrorCount());

        _brokerLogger.resetStatistics();

        assertEquals(0L, _brokerLogger.getWarnCount());
        assertEquals(0L, _brokerLogger.getErrorCount());

        errorFilter.delete();
    }

    @Test
    public void testTurningLoggingOff()
    {
        final Map<String, Object> fooRuleAttributes =
                createBrokerNameAndLevelLogInclusionRuleAttributes("fooRule",
                                                                   "org.apache.qpid.foo.*",
                                                                   LogLevel.INFO);
        final Map<String, Object> barRuleAttributes =
                createBrokerNameAndLevelLogInclusionRuleAttributes("barRule",
                                                                   "org.apache.qpid.foo.bar",
                                                                   LogLevel.OFF);

        _brokerLogger.createChild(BrokerLogInclusionRule.class, fooRuleAttributes);
        _brokerLogger.createChild(BrokerLogInclusionRule.class, barRuleAttributes);

        final Logger barLogger = LoggerFactory.getLogger("org.apache.qpid.foo.bar");
        barLogger.warn("bar message");

        final Logger fooLogger = LoggerFactory.getLogger("org.apache.qpid.foo.foo");
        fooLogger.warn("foo message");

        assertLoggedEvent(_loggerAppender, false, "bar message", barLogger.getName(), Level.WARN);
        assertLoggedEvent(_loggerAppender, false, "bar message", fooLogger.getName(), Level.WARN);
        assertLoggedEvent(_loggerAppender, true, "foo message", fooLogger.getName(), Level.WARN);
    }

    @Test
    public void testExactLoggerRuleSupersedeWildCardLoggerRule()
    {
        final Map<String, Object> fooRuleAttributes =
                createBrokerNameAndLevelLogInclusionRuleAttributes("fooRule",
                                                                   "org.apache.qpid.foo.*",
                                                                   LogLevel.INFO);
        final Map<String, Object> barRuleAttributes =
                createBrokerNameAndLevelLogInclusionRuleAttributes("barRule",
                                                                   "org.apache.qpid.foo.bar",
                                                                   LogLevel.WARN);

        _brokerLogger.createChild(BrokerLogInclusionRule.class, fooRuleAttributes);
        _brokerLogger.createChild(BrokerLogInclusionRule.class, barRuleAttributes);

        final Logger barLogger = LoggerFactory.getLogger("org.apache.qpid.foo.bar");
        barLogger.info("info bar message");
        barLogger.error("error bar message");

        final Logger fooLogger = LoggerFactory.getLogger("org.apache.qpid.foo.foo");
        fooLogger.info("info foo message");

        assertLoggedEvent(_loggerAppender, false, "info bar message", barLogger.getName(), Level.INFO);
        assertLoggedEvent(_loggerAppender, true, "error bar message", barLogger.getName(), Level.ERROR);
        assertLoggedEvent(_loggerAppender, true, "info foo message", fooLogger.getName(), Level.INFO);
    }

    private Map<String, Object> createBrokerNameAndLevelLogInclusionRuleAttributes(final String loggerName,
                                                                                   final LogLevel logLevel)
    {
        return createBrokerNameAndLevelLogInclusionRuleAttributes("test", loggerName, logLevel);
    }

    private Map<String, Object> createBrokerNameAndLevelLogInclusionRuleAttributes(final String ruleName,
                                                                                   final String loggerName,
                                                                                   final LogLevel logLevel)
    {
        return Map.of(BrokerNameAndLevelLogInclusionRule.LOGGER_NAME, loggerName,
                BrokerNameAndLevelLogInclusionRule.LEVEL, logLevel,
                BrokerNameAndLevelLogInclusionRule.NAME, ruleName,
                ConfiguredObject.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
    }

    private LogRecord findLogRecord(final String message, final Collection<LogRecord> logRecords)
    {
        for (final LogRecord record: logRecords)
        {
            if (message.equals(record.getMessage()))
            {
                return record;
            }
        }
        return null;
    }
}
