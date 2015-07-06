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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.management.JMException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.qpid.server.logging.BrokerFileLogger;
import org.apache.qpid.server.logging.BrokerNameAndLevelFilter;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.BrokerLoggerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.management.common.mbeans.LoggingManagement;
import org.apache.qpid.management.common.mbeans.annotations.MBeanDescription;
import org.apache.qpid.server.jmx.AMQManagedObject;
import org.apache.qpid.server.jmx.ManagedObject;
import org.apache.qpid.server.jmx.ManagedObjectRegistry;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;


/** MBean class for LoggingManagement. It implements all the management features exposed for managing logging. */
@MBeanDescription("Logging Management Interface")
public class LoggingManagementMBean extends AMQManagedObject implements LoggingManagement
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingManagementMBean.class);
    private static final TabularType LOGGER_LEVEL_TABULAR_TYPE;
    private static final CompositeType LOGGER_LEVEL_COMPOSITE_TYPE;
    private static final String UNSUPPORTED_LOGGER_FILTER_TYPE = "<UNSUPPORTED>";

    static
    {
        try
        {
            OpenType[] loggerLevelItemTypes = new OpenType[]{SimpleType.STRING, SimpleType.STRING};

            LOGGER_LEVEL_COMPOSITE_TYPE = new CompositeType("LoggerLevelList", "Logger Level Data",
                                                         COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]),
                                                         COMPOSITE_ITEM_DESCRIPTIONS.toArray(new String[COMPOSITE_ITEM_DESCRIPTIONS.size()]),
                                                         loggerLevelItemTypes);

            LOGGER_LEVEL_TABULAR_TYPE = new TabularType("LoggerLevel", "List of loggers with levels",
                                                       LOGGER_LEVEL_COMPOSITE_TYPE,
                                                       TABULAR_UNIQUE_INDEX.toArray(new String[TABULAR_UNIQUE_INDEX.size()]));
        }
        catch (OpenDataException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String[] _allAvailableLogLevels;
    private final BrokerFileLogger _brokerFileLogger;

    public LoggingManagementMBean(BrokerFileLogger brokerFileLogger, ManagedObjectRegistry registry) throws JMException
    {
        super(LoggingManagement.class, LoggingManagement.TYPE, registry);
        register();
        _brokerFileLogger = brokerFileLogger;

        Collection<String> validLogLevels = LogLevel.validValues();
        _allAvailableLogLevels = validLogLevels.toArray(new String[validLogLevels.size()]);
    }

    @Override
    public String getObjectInstanceName()
    {
        return LoggingManagement.TYPE;
    }

    @Override
    public ManagedObject getParentObject()
    {
        return null;
    }

    @Override
    public Integer getLog4jLogWatchInterval()
    {
        return -1;
    }
    
    @Override
    public String[] getAvailableLoggerLevels()
    {
        return _allAvailableLogLevels;
    }

    @Override
    public TabularData viewEffectiveRuntimeLoggerLevels()
    {
        return getTabularData(findFiltersByDurability(FilterDurability.EITHER));
    }

    @Override
    public String getRuntimeRootLoggerLevel()
    {
        return getLogLevel(ROOT_LOGGER_NAME, FilterDurability.NONDURABLE);
    }

    @Override
    public boolean setRuntimeRootLoggerLevel(String level)
    {
        return setRuntimeLoggerLevel(ROOT_LOGGER_NAME, level);
    }

    @Override
    public boolean setRuntimeLoggerLevel(String logger, String level)
    {
        return setLogLevel(logger, level, FilterDurability.NONDURABLE);
    }

    @Override
    public TabularData viewConfigFileLoggerLevels()
    {
        return getTabularData(findFiltersByDurability(FilterDurability.DURABLE));
    }

    @Override
    public String getConfigFileRootLoggerLevel() throws IOException
    {
        return getLogLevel(ROOT_LOGGER_NAME, FilterDurability.DURABLE);
    }

    @Override
    public boolean setConfigFileLoggerLevel(String logger, String level)
    {
        return setLogLevel(logger, level, FilterDurability.DURABLE);
    }

    @Override
    public boolean setConfigFileRootLoggerLevel(String level)
    {
        return setConfigFileLoggerLevel(ROOT_LOGGER_NAME, level);
    }

    @Override
    public void reloadConfigFile() throws IOException
    {
        throw new UnsupportedOperationException("Reloading of configuration file is not supported.");
    }

    private TabularData createTabularDataFromLevelsMap(Map<String, String> levels)
    {
        TabularData loggerLevelList = new TabularDataSupport(LOGGER_LEVEL_TABULAR_TYPE);
        for (Map.Entry<String,String> entry : levels.entrySet())
        {
            String loggerName = entry.getKey();
            String level = entry.getValue();

            CompositeData loggerData = createRow(loggerName, level);
            loggerLevelList.put(loggerData);
        }
        return loggerLevelList;
    }

    private CompositeData createRow(String loggerName, String level)
    {
        Object[] itemData = {loggerName, level.toUpperCase()};
        try
        {
            CompositeData loggerData = new CompositeDataSupport(LOGGER_LEVEL_COMPOSITE_TYPE,
                    COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]), itemData);
            return loggerData;
        }
        catch (OpenDataException ode)
        {
            // Should not happen
            throw new ConnectionScopedRuntimeException(ode);
        }
    }

    private boolean isValidLogLevel(String logLevel)
    {
        try
        {
            LogLevel.valueOf(logLevel);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    private TabularData getTabularData(Collection<BrokerLoggerFilter<?>> filters)
    {
        Map<String,String> logToLevelMap = new TreeMap<>();
        for (BrokerLoggerFilter<?> filter: filters)
        {
            if (filter instanceof BrokerNameAndLevelFilter)
            {
                BrokerNameAndLevelFilter<?> nameAndLevelFilter = (BrokerNameAndLevelFilter)filter;
                logToLevelMap.put(nameAndLevelFilter.getLoggerName(), nameAndLevelFilter.getLevel().name());
            }
            else
            {
                logToLevelMap.put(UNSUPPORTED_LOGGER_FILTER_TYPE, "");
            }
        }
        return createTabularDataFromLevelsMap(logToLevelMap);
    }

    private String getLogLevel(String loggerName, FilterDurability filterDurability)
    {
        LogLevel level = LogLevel.OFF;
        List<BrokerNameAndLevelFilter<?>> filters = findFiltersByLoggerNameAndDurability(loggerName, filterDurability);

        for(BrokerNameAndLevelFilter<?> filter: filters)
        {
            final LogLevel filterLevel = filter.getLevel();
            if (level.compareTo(filterLevel) > 0)
            {
                level = filterLevel;
            }
        }
        return level.name();
    }

    private boolean setLogLevel(String logger, String level, FilterDurability durability)
    {
        if (!isValidLogLevel(level))
        {
            LOGGER.warn("{} is not a known level", level);
            return false;
        }

        List<BrokerNameAndLevelFilter<?>> filters = findFiltersByLoggerNameAndDurability(logger, durability);
        if (filters.isEmpty())
        {
            LOGGER.warn("There is no logger with name '{}' and durability '{}'", logger, durability.name().toLowerCase());
            return false;
        }

        LogLevel targetLevel = LogLevel.valueOf(level);
        for (BrokerNameAndLevelFilter<?> filter: filters)
        {
            try
            {
                filter.setAttributes(Collections.<String, Object>singletonMap(BrokerNameAndLevelFilter.LEVEL, targetLevel));
            }
            catch(RuntimeException e)
            {
                LOGGER.error("Aborting setting runtime logging level due to failure", e);
                return false;
            }
        }

        return true;
    }

    private List<BrokerLoggerFilter<?>> findFiltersByDurability(FilterDurability durability)
    {
        Collection<BrokerLoggerFilter<?>> filters = _brokerFileLogger.getChildren(BrokerLoggerFilter.class);
        List<BrokerLoggerFilter<?>> results = new ArrayList<>();
        if (durability == durability.EITHER)
        {
            results.addAll(filters);
        }
        else
        {
            for (BrokerLoggerFilter<?> filter: filters)
            {
                if (durability == FilterDurability.valueOf(filter.isDurable()))
                {
                    results.add(filter);
                }
            }
        }
        return results;
    }

    private List<BrokerNameAndLevelFilter<?>> findFiltersByLoggerNameAndDurability(String loggerName, FilterDurability filterDurability)
    {
        List<BrokerNameAndLevelFilter<?>> results = new ArrayList<>();
        Collection<BrokerLoggerFilter<?>> filters = findFiltersByDurability(filterDurability);
        String sanitizedLoggerName = sanitizeLoggerName(loggerName);
        for (BrokerLoggerFilter<?> filter: filters)
        {
            if (filter instanceof BrokerNameAndLevelFilter)
            {
                BrokerNameAndLevelFilter<?> brokerNameAndLevelFilter = (BrokerNameAndLevelFilter<?>)filter;
                String filterLoggerName = sanitizeLoggerName(brokerNameAndLevelFilter.getLoggerName());
                if (sanitizedLoggerName.equals(filterLoggerName))
                {
                    results.add(brokerNameAndLevelFilter);
                }
            }
        }
        return results;
    }

    private String sanitizeLoggerName(String loggerName)
    {
        if (loggerName == null || "".equals(loggerName))
        {
            return ROOT_LOGGER_NAME;
        }
        return loggerName;
    }

    private enum FilterDurability
    {
        DURABLE,
        NONDURABLE,
        EITHER;

        public static FilterDurability valueOf(boolean durable)
        {
            return durable ? DURABLE : NONDURABLE;
        }
    }
}
