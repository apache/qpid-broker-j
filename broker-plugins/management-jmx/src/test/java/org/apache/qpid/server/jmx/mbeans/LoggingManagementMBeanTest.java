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
import org.apache.qpid.server.logging.BrokerNameAndLevelFilter;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.BrokerLoggerFilter;
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
        Collection<BrokerLoggerFilter> filters = new ArrayList<>();
        filters.add(createMockFilter("a.b.D", LogLevel.DEBUG));
        filters.add(createMockFilter("a.b.C", LogLevel.INFO));
        filters.add(createMockFilter("", LogLevel.WARN));

        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(filters);

        TabularData table = _loggingMBean.viewEffectiveRuntimeLoggerLevels();
        assertEquals(3, table.size());

        final CompositeData row1 = table.get(new String[] {"a.b.C"} );
        final CompositeData row2 = table.get(new String[]{"a.b.D"});
        final CompositeData row3 = table.get(new String[]{""});
        assertChannelRow(row1, "a.b.C", LogLevel.INFO.name());
        assertChannelRow(row2, "a.b.D", LogLevel.DEBUG.name());
        assertChannelRow(row3, "", LogLevel.WARN.name());
    }

    private BrokerNameAndLevelFilter createMockFilter(String loggerName, LogLevel logLevel, boolean durability)
    {
        BrokerNameAndLevelFilter filter = mock(BrokerNameAndLevelFilter.class);
        when(filter.getLevel()).thenReturn(logLevel);
        when(filter.getLoggerName()).thenReturn(loggerName);
        when(filter.isDurable()).thenReturn(durability);
        return filter;
    }

    private BrokerNameAndLevelFilter createMockFilter(String loggerName, LogLevel logLevel)
    {
        return createMockFilter(loggerName, logLevel, false);
    }

    public void testGetRuntimeRootLoggerLevel()  throws Exception
    {
        assertEquals(LogLevel.OFF.toString(), _loggingMBean.getRuntimeRootLoggerLevel());

        BrokerLoggerFilter rootFilterWithNull = createMockFilter(null, LogLevel.WARN);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(rootFilterWithNull));
        assertEquals(LogLevel.WARN.name(), _loggingMBean.getRuntimeRootLoggerLevel());

        BrokerLoggerFilter rootFilterWithEmptyName = createMockFilter("", LogLevel.ERROR);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(rootFilterWithEmptyName));
        assertEquals(LogLevel.ERROR.name(), _loggingMBean.getRuntimeRootLoggerLevel());

        BrokerLoggerFilter rootFilterWithRootLoggerName = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(rootFilterWithRootLoggerName));
        assertEquals(LogLevel.DEBUG.name(), _loggingMBean.getRuntimeRootLoggerLevel());
    }

    public void testSetRuntimeRootLoggerLevel()  throws Exception
    {
        assertFalse("Should not be able to set runtime root log level without existing filter",
                _loggingMBean.setRuntimeRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLoggerFilter durableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(durableRootFilter));
        assertFalse("Should not be able to set runtime root log level on durable root filter",
                _loggingMBean.setRuntimeRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLoggerFilter nonDurableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(nonDurableRootFilter));
        assertTrue("Should be able to set runtime root log level on non-durable root filter",
                _loggingMBean.setRuntimeRootLoggerLevel(LogLevel.WARN.name()));
    }

    public void testSetRuntimeRootLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        BrokerLoggerFilter nonDurableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(nonDurableRootFilter));

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
        assertFalse("Should not be able to set runtime log level without existing filter '" + TEST_LOGGER_NAME + "'",
                _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLoggerFilter durableFilter = createMockFilter(TEST_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(durableFilter));
        assertFalse("Should not be able to set runtime log level on durable filter '" + TEST_LOGGER_NAME + "'",
                _loggingMBean.setRuntimeLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLoggerFilter nonDurableFilter = createMockFilter(TEST_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(nonDurableFilter));
        assertTrue("Should be able to set runtime log level on non-durable filter '" + TEST_LOGGER_NAME + "'",
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
        Collection<BrokerLoggerFilter> filters = new ArrayList<>();
        filters.add(createMockFilter("a.b.D", LogLevel.DEBUG, true));
        filters.add(createMockFilter("a.b.C", LogLevel.INFO, true));
        filters.add(createMockFilter("", LogLevel.WARN, true));
        filters.add(createMockFilter("a.b.E", LogLevel.WARN, false));

        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(filters);

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
        assertEquals("Unexpected config root logger level when no filter is set", LogLevel.OFF.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLoggerFilter rootFilterWithNullName = createMockFilter(null, LogLevel.WARN, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(rootFilterWithNullName));
        assertEquals("Unexpected config root logger level", LogLevel.WARN.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLoggerFilter rootFilterWithEmptyName = createMockFilter("", LogLevel.ERROR, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(rootFilterWithEmptyName));
        assertEquals("Unexpected config root logger level", LogLevel.ERROR.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLoggerFilter rootFilterWithRootLoggerName = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(rootFilterWithRootLoggerName));
        assertEquals("Unexpected config root logger level", LogLevel.DEBUG.name(), _loggingMBean.getConfigFileRootLoggerLevel());

        BrokerLoggerFilter nonDurableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(nonDurableRootFilter));
        assertEquals("Unexpected config root logger level when root filter is non-durable", LogLevel.OFF.name(), _loggingMBean.getConfigFileRootLoggerLevel());
    }

    public void testSetConfigFileRootLoggerLevel()  throws Exception
    {
        assertFalse("Should not be able to set config root log level without existing filter",
                _loggingMBean.setConfigFileRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLoggerFilter durableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(durableRootFilter));
        assertTrue("Should be able to set config root log level on durable root filter",
                _loggingMBean.setConfigFileRootLoggerLevel(LogLevel.WARN.name()));

        BrokerLoggerFilter nonDurableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(nonDurableRootFilter));
        assertFalse("Should not be able to set config root log level on non-durable root filter",
                _loggingMBean.setConfigFileRootLoggerLevel(LogLevel.WARN.name()));
    }


    public void testSetConfigFileRootLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        BrokerLoggerFilter durableRootFilter = createMockFilter(ROOT_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(durableRootFilter));

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
        assertFalse("Should not be able to set config log level without existing filter",
                _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLoggerFilter durableFilter = createMockFilter(TEST_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(durableFilter));
        assertTrue("Should be able to set config log level on durable filter",
                _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));

        BrokerLoggerFilter nonDurableFilter = createMockFilter(TEST_LOGGER_NAME, LogLevel.DEBUG, false);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(nonDurableFilter));
        assertFalse("Should not be able to set config log level on non-durable filter",
                _loggingMBean.setConfigFileLoggerLevel(TEST_LOGGER_NAME, LogLevel.WARN.name()));
    }

    public void testSetConfigFileLoggerLevelWhenLoggingLevelUnsupported()  throws Exception
    {
        BrokerLoggerFilter durableFilter = createMockFilter(TEST_LOGGER_NAME, LogLevel.DEBUG, true);
        when(_brokerFileLogger.getChildren(BrokerLoggerFilter.class)).thenReturn(Collections.singleton(durableFilter));

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
