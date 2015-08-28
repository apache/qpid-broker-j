/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.jmx.mbeans;

import static ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.qpid.management.common.mbeans.LoggingManagement;
import org.apache.qpid.server.jmx.ManagedObjectRegistry;
import org.apache.qpid.server.logging.BrokerFileLogger;
import org.apache.qpid.server.logging.BrokerNameAndLevelLogInclusionRule;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.BrokerLogInclusionRule;
import org.apache.qpid.test.utils.QpidTestCase;

public class LoggingManagementMBeanTest extends QpidTestCase
{
    private static final String INHERITED_PSUEDO_LOG_LEVEL = "INHERITED";
    private static final String TEST_LOGGER_NAME = "org.apache.qpid.test";
    private static final String UNSUPPORTED_LOG_LEVEL = "unsupported";

    private LoggingManagementMBean _loggingMBean;
    private ManagedObjectRegistry _mockManagedObjectRegistry;
    private BrokerFileLogger _brokerFileLogger;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _mockManagedObjectRegistry = mock(ManagedObjectRegistry.class);

        _brokerFileLogger = mock(BrokerFileLogger.class);
        _loggingMBean = new LoggingManagementMBean(_brokerFileLogger, _mockManagedObjectRegistry);
    }

    public void testMBeanRegistersItself() throws Exception
    {
        LoggingManagementMBean connectionMBean = new LoggingManagementMBean(_brokerFileLogger, _mockManagedObjectRegistry);
        verify(_mockManagedObjectRegistry).registerObject(connectionMBean);
    }

    public void testLog4jLogWatchInterval() throws Exception
    {
        assertEquals("Unexpected watch interval", new Integer(-1), _loggingMBean.getLog4jLogWatchInterval());
    }

    public void testGetAvailableLoggerLevels()  throws Exception
    {
        Set<String> expectedLogLevels = new HashSet<>(LogLevel.validValues());
        Set<String> actualLevels = new HashSet<>(Arrays.asList(_loggingMBean.getAvailableLoggerLevels()));
        assertEquals(expectedLogLevels, actualLevels);
    }

    public void testViewEffectiveRuntimeLoggerLevels()  throws Exception
    {
        Collection<BrokerLogInclusionRule> rules = new ArrayList<>();
        rules.add(createMockRule("a.b.D", LogLevel.DEBUG));
        rules.add(createMockRule("a.b.C", LogLevel.INFO));
        rules.add(createMockRule("", LogLevel.WARN));

        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(rules);

        TabularData table = _loggingMBean.viewEffectiveRuntimeLoggerLevels();
        assertEquals(3, table.size());

        final CompositeData row1 = table.get(new String[] {"a.b.C"} );
        final CompositeData row2 = table.get(new String[]{"a.b.D"});
        final CompositeData row3 = table.get(new String[]{""});
        assertChannelRow(row1, "a.b.C", LogLevel.INFO.name());
        assertChannelRow(row2, "a.b.D", LogLevel.DEBUG.name());
        assertChannelRow(row3, "", LogLevel.WARN.name());
    }

    private BrokerNameAndLevelLogInclusionRule createMockRule(String loggerName, LogLevel logLevel, boolean durability)
    {
        BrokerNameAndLevelLogInclusionRule rule = mock(BrokerNameAndLevelLogInclusionRule.class);
        when(rule.getLevel()).thenReturn(logLevel);
        when(rule.getLoggerName()).thenReturn(loggerName);
        when(rule.isDurable()).thenReturn(durability);
        return rule;
    }

    private BrokerNameAndLevelLogInclusionRule createMockRule(String loggerName, LogLevel logLevel)
    {
        return createMockRule(loggerName, logLevel, false);
    }

    public void testGetRuntimeRootLoggerLevel()  throws Exception
    {
        assertEquals(LogLevel.OFF.toString(), _loggingMBean.getRuntimeRootLoggerLevel());

        BrokerLogInclusionRule rootRuleWithNull = createMockRule(null, LogLevel.WARN);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(rootRuleWithNull));
        assertEquals(LogLevel.WARN.name(), _loggingMBean.getRuntimeRootLoggerLevel());

        BrokerLogInclusionRule rootRuleWithEmptyName = createMockRule("", LogLevel.ERROR);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(rootRuleWithEmptyName));
        assertEquals(LogLevel.ERROR.name(), _loggingMBean.getRuntimeRootLoggerLevel());

        BrokerLogInclusionRule rootRuleWithRootLoggerName = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(rootRuleWithRootLoggerName));
        assertEquals(LogLevel.DEBUG.name(), _loggingMBean.getRuntimeRootLoggerLevel());
    }

    public void testSetRuntimeRootLoggerLevel()  throws Exception
    {
        assertFalse("Should not be able to set runtime root log level without existing rule",
                _loggingMBean.setRuntimeRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLogInclusionRule durableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(durableRootRule));
        assertFalse("Should not be able to set runtime root log level on durable root rule",
                _loggingMBean.setRuntimeRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLogInclusionRule nonDurableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(nonDurableRootRule));
        assertTrue("Should be able to set runtime root log level on non-durable root rule",
                _loggingMBean.setRuntimeRootLoggerLevel(LogLevel.WARN.name()));
    }

    public void testSetRuntimeRootLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        BrokerLogInclusionRule nonDurableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(nonDurableRootRule));

        String currentRootLogLevel = _loggingMBean.getRuntimeRootLoggerLevel();

        boolean result = _loggingMBean.setRuntimeRootLoggerLevel(UNSUPPORTED_LOG_LEVEL);
        assertFalse("Should not be able to set runtime root logger level to unsupported value", result);

        assertEquals("Runtime root log level was changed unexpectedly on setting level to unsupported value",
                currentRootLogLevel, _loggingMBean.getRuntimeRootLoggerLevel());

        result = _loggingMBean.setRuntimeRootLoggerLevel(INHERITED_PSUEDO_LOG_LEVEL);
        assertFalse("Should not be able to set runtime root logger level to INHERITED value", result);

        assertEquals("Runtime root log level was changed unexpectedly on setting level to INHERITED value",
                currentRootLogLevel, _loggingMBean.getRuntimeRootLoggerLevel());
    }

    public void testSetRuntimeLoggerLevel()  throws Exception
    {
        assertFalse("Should not be able to set runtime log level without existing rule '" + TEST_LOGGER_NAME + "'",
                _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLogInclusionRule durableRule = createMockRule(TEST_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(durableRule));
        assertFalse("Should not be able to set runtime log level on durable rule '" + TEST_LOGGER_NAME + "'",
                _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLogInclusionRule nonDurableRule = createMockRule(TEST_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(nonDurableRule));
        assertTrue("Should be able to set runtime log level on non-durable rule '" + TEST_LOGGER_NAME + "'",
                _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));
    }

    public void testSetRuntimeLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        boolean result = _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, UNSUPPORTED_LOG_LEVEL);
        assertFalse("Should not be able to set runtime logger level to unsupported value", result);

        result = _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, INHERITED_PSUEDO_LOG_LEVEL);
        assertFalse("Should not be able to set runtime logger level to INHERITED value", result);
    }

    public void testViewEffectiveConfigFileLoggerLevels()  throws Exception
    {
        Collection<BrokerLogInclusionRule> rules = new ArrayList<>();
        rules.add(createMockRule("a.b.D", LogLevel.DEBUG, true));
        rules.add(createMockRule("a.b.C", LogLevel.INFO, true));
        rules.add(createMockRule("", LogLevel.WARN, true));
        rules.add(createMockRule("a.b.E", LogLevel.WARN, false));

        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(rules);

        TabularData table = _loggingMBean.viewConfigFileLoggerLevels();
        assertEquals(3, table.size());

        final CompositeData row1 = table.get(new String[]{"a.b.C"});
        final CompositeData row2 = table.get(new String[]{"a.b.D"});
        final CompositeData row3 = table.get(new String[]{""});
        assertChannelRow(row1, "a.b.C", LogLevel.INFO.name());
        assertChannelRow(row2, "a.b.D", LogLevel.DEBUG.name());
        assertChannelRow(row3, "", LogLevel.WARN.name());
    }

    public void testGetConfigFileRootLoggerLevel() throws Exception
    {
        assertEquals("Unexpected config root logger level when no rule is set", LogLevel.OFF.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLogInclusionRule rootRuleWithNullName = createMockRule(null, LogLevel.WARN, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(rootRuleWithNullName));
        assertEquals("Unexpected config root logger level", LogLevel.WARN.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLogInclusionRule rootRuleWithEmptyName = createMockRule("", LogLevel.ERROR, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(rootRuleWithEmptyName));
        assertEquals("Unexpected config root logger level", LogLevel.ERROR.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLogInclusionRule rootRuleWithRootLoggerName = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(rootRuleWithRootLoggerName));
        assertEquals("Unexpected config root logger level", LogLevel.DEBUG.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLogInclusionRule nonDurableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(nonDurableRootRule));
        assertEquals("Unexpected config root logger level when root rule is non-durable", LogLevel.OFF.name(), _loggingMBean.getConfigFileRootLoggerLevel());
    }

    public void testSetConfigFileRootLoggerLevel()  throws Exception
    {
        assertFalse("Should not be able to set config root log level without existing rule",
                _loggingMBean.setConfigFileRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLogInclusionRule durableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(durableRootRule));
        assertTrue("Should be able to set config root log level on durable root rule",
                _loggingMBean.setConfigFileRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLogInclusionRule nonDurableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(nonDurableRootRule));
        assertFalse("Should not be able to set config root log level on non-durable root rule",
                _loggingMBean.setConfigFileRootLoggerLevel(LogLevel.WARN.name()));
    }


    public void testSetConfigFileRootLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        BrokerLogInclusionRule durableRootRule = createMockRule(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(durableRootRule));

        String currentRootLogLevel = _loggingMBean.getConfigFileRootLoggerLevel();

        boolean result = _loggingMBean.setConfigFileRootLoggerLevel(UNSUPPORTED_LOG_LEVEL);
        assertFalse("Should not be able to set config root logger level to unsupported value", result);

        assertEquals("Runtime root log level was changed unexpectedly on setting level to unsupported value",
                currentRootLogLevel, _loggingMBean.getConfigFileRootLoggerLevel());

        result = _loggingMBean.setConfigFileRootLoggerLevel(INHERITED_PSUEDO_LOG_LEVEL);
        assertFalse("Should not be able to set config root logger level to INHERITED value", result);

        assertEquals("Config root log level was changed unexpectedly on setting level to INHERITED value",
                currentRootLogLevel, _loggingMBean.getConfigFileRootLoggerLevel());
    }

    public void testSetConfigFileLoggerLevel() throws Exception
    {
        assertFalse("Should not be able to set config log level without existing rule",
                _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLogInclusionRule durableRule = createMockRule(TEST_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(durableRule));
        assertTrue("Should be able to set config log level on durable rule",
                _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLogInclusionRule nonDurableRule = createMockRule(TEST_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(nonDurableRule));
        assertFalse("Should not be able to set config log level on non-durable rule",
                _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));
    }

    public void testSetConfigFileLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        BrokerLogInclusionRule durableRule = createMockRule(TEST_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLogInclusionRule.class)).thenReturn(Collections.singleton(durableRule));

        boolean result = _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, UNSUPPORTED_LOG_LEVEL);
        assertFalse("Should not be able to set config root logger level to unsupported value", result);

        result = _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, INHERITED_PSUEDO_LOG_LEVEL);
        assertFalse("Should not be able to set config root logger level to INHERITED value", result);
    }

    public void testReloadConfigFile() throws Exception
    {
        try
        {
            _loggingMBean.reloadConfigFile();
            fail("Reloading is unsupported");
        }
        catch(UnsupportedOperationException e)
        {
            // pass
        }
    }

    private void assertChannelRow(final CompositeData row, String logger, String level)
    {
        assertNotNull("No row for  " + logger, row);
        assertEquals("Unexpected logger name", logger, row.get(LoggingManagement.LOGGER_NAME));
        assertEquals("Unexpected level", level, row.get(LoggingManagement.LOGGER_LEVEL));
    }

}
