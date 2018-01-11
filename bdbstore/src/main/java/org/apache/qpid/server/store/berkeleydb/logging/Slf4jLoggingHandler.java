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
package org.apache.qpid.server.store.berkeleydb.logging;

import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.DEFAULT_LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT;
import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.JUL_LOGGER_LEVEL_OVERRIDE;
import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME;
import static org.apache.qpid.server.util.ParameterizedTypes.MAP_OF_STRING_STRING;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sleepycat.je.cleaner.Cleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.store.berkeleydb.StandardEnvironmentConfiguration;


public class Slf4jLoggingHandler extends Handler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jLoggingHandler.class);

    private static Pattern NOT_DELETED_DUE_TO_PROTECTION = Pattern.compile("Cleaner has ([0-9]+) files not deleted because they are protected.*");

    private final ConcurrentMap<String,Logger> _loggers = new ConcurrentHashMap<>();
    private final int _logHandlerCleanerProtectedFilesLimit;
    private final String _prefix;
    private final Set<java.util.logging.Logger> _overridedenLoggers = new HashSet<>();

    public Slf4jLoggingHandler(final StandardEnvironmentConfiguration configuration)
    {
        _prefix = configuration.getName();
        setFormatter(new Formatter()
        {
            @Override
            public String format(final LogRecord record)
            {
                return _prefix + " " + formatMessage(record);
            }
        });

        _logHandlerCleanerProtectedFilesLimit = configuration.getFacadeParameter(Integer.class,
                                                                                 LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME,
                                                                                 DEFAULT_LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT);

        final Map<String, String> levelOverrides = configuration.getFacadeParameter(Map.class,
                                                                                    MAP_OF_STRING_STRING,
                                                                                    JUL_LOGGER_LEVEL_OVERRIDE,
                                                                                    Collections.emptyMap());
        applyJulLoggerLevelOverrides(levelOverrides);
    }

    private void applyJulLoggerLevelOverrides(final Map<String, String> julLoggerLevelOverrides)
    {
        julLoggerLevelOverrides.forEach((julLoggerName, julDesiredLevelString) -> {
            Level julDesiredLevel;
            try
            {
                julDesiredLevel = Level.parse(julDesiredLevelString);
            }
            catch (IllegalArgumentException e)
            {
                julDesiredLevel = null;
                LOGGER.warn("Unrecognised JUL level name '{}' in JUL override for logger name '{}'",
                            julDesiredLevelString, julLoggerName);
            }

            if (julDesiredLevel != null)
            {
                java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(julLoggerName);
                if (julLogger.getLevel() == null || !julLogger.isLoggable(julDesiredLevel))
                {
                    _overridedenLoggers.add(julLogger);  // Retain reference in case logger is not yet held by the class
                    julLogger.setLevel(julDesiredLevel);

                    LOGGER.warn("JUL logger {} overridden to level {}",
                                julLogger.getName(),
                                julLogger.getLevel());
                }
            }
        });
    }

    private interface MappedLevel
    {
        boolean isEnabled(final Logger logger);

        void log(Logger logger, String message);
    }

    private static final MappedLevel ERROR = new MappedLevel()
    {
        @Override
        public boolean isEnabled(final Logger logger)
        {
            return logger.isErrorEnabled();
        }

        @Override
        public void log(final Logger logger, final String message)
        {
            logger.error(message);
        }
    };

    private static final MappedLevel WARN = new MappedLevel()
    {
        @Override
        public boolean isEnabled(final Logger logger)
        {
            return logger.isWarnEnabled();
        }

        @Override
        public void log(final Logger logger, final String message)
        {
            logger.warn(message);
        }
    };

    private static final MappedLevel INFO = new MappedLevel()
    {
        @Override
        public boolean isEnabled(final Logger logger)
        {
            return logger.isInfoEnabled();
        }

        @Override
        public void log(final Logger logger, final String message)
        {
            logger.info(message);
        }
    };

    private static final MappedLevel DEBUG = new MappedLevel()
    {
        @Override
        public boolean isEnabled(final Logger logger)
        {
            return logger.isDebugEnabled();
        }

        @Override
        public void log(final Logger logger, final String message)
        {
            logger.debug(message);
        }
    };

    private static final MappedLevel TRACE = new MappedLevel()
    {
        @Override
        public boolean isEnabled(final Logger logger)
        {
            return logger.isTraceEnabled();
        }

        @Override
        public void log(final Logger logger, final String message)
        {
            logger.trace(message);
        }
    };

    private static final Map<Level, MappedLevel> LEVEL_MAP;
    static
    {
        Map<Level, MappedLevel> map = new HashMap<>();
        map.put(Level.SEVERE, ERROR);
        map.put(Level.WARNING, WARN);
        //Note that INFO comes out at DEBUG level as the BDB logging at INFO seems to be more of a DEBUG nature
        map.put(Level.INFO, DEBUG);
        map.put(Level.CONFIG, DEBUG);
        map.put(Level.FINE, TRACE);
        map.put(Level.FINER, TRACE);
        map.put(Level.FINEST, TRACE);
        map.put(Level.ALL, TRACE);

        LEVEL_MAP = Collections.unmodifiableMap(map);
    }

    @Override
    public void publish(final LogRecord record)
    {
        MappedLevel level = convertLevel(record);
        final Logger logger = getLogger(record.getLoggerName());
        if (level.isEnabled(logger))
        {

            Formatter formatter = getFormatter();

            try
            {
                String message = formatter.format(record);
                try
                {
                    level.log(logger, message);
                }
                catch (RuntimeException e)
                {
                    reportError(null, e, ErrorManager.WRITE_FAILURE);
                }
            }
            catch (RuntimeException e)
            {
                reportError(null, e, ErrorManager.FORMAT_FAILURE);
            }
        }
    }

    private Logger getLogger(String loggerName)
    {
        Logger logger = _loggers.get(loggerName);
        if(logger == null)
        {
            logger = LoggerFactory.getLogger(loggerName);
            _loggers.putIfAbsent(loggerName, logger);
        }
        return logger;
    }

    private MappedLevel convertLevel(LogRecord record)
    {
        if (record.getLevel() == Level.SEVERE && isJECleaner(record)
            && record.getMessage().startsWith("Average cleaner backlog has grown from"))
        {
            // this is not a real ERROR condition; reducing level to INFO
            return INFO;
        }

        if (record.getLevel() == Level.WARNING && isJECleaner(record))
        {
            Matcher matcher = NOT_DELETED_DUE_TO_PROTECTION.matcher(record.getMessage());
            if (matcher.matches() && matcher.groupCount() > 0 && Integer.parseInt(matcher.group(1)) < _logHandlerCleanerProtectedFilesLimit)
            {
                return DEBUG;
            }
        }

        return convertLevel(record.getLevel());
    }

    private boolean isJECleaner(final LogRecord record)
    {
        return Cleaner.class.getName().equals(record.getLoggerName());
    }

    @Override
    public boolean isLoggable(final LogRecord record)
    {
        MappedLevel mappedLevel = convertLevel(record.getLevel());

        return mappedLevel.isEnabled(getLogger(record.getLoggerName()));
    }

    private MappedLevel convertLevel(final Level level)
    {
        //Note that INFO comes out at DEBUG level as the BDB logging at INFO seems to be more of a DEBUG nature
        MappedLevel result = LEVEL_MAP.get(level);
        if(result == null)
        {
            if (level.intValue() >= Level.SEVERE.intValue())
            {
                result = ERROR;
            }
            else if (level.intValue() >= Level.WARNING.intValue())
            {
                result = WARN;
            }
            else if (level.intValue() >= Level.CONFIG.intValue())
            {
                result = DEBUG;
            }
            else
            {
                result = TRACE;
            }
        }

        return result;
    }

    @Override
    public void flush()
    {

    }

    @Override
    public void close() throws SecurityException
    {
        _overridedenLoggers.clear();

    }
}
