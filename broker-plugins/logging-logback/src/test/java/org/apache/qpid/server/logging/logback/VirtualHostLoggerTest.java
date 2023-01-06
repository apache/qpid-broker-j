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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostLogInclusionRule;
import org.apache.qpid.server.model.VirtualHostLogger;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class VirtualHostLoggerTest extends UnitTestBase
{
    private VirtualHost<?> _virtualHost;
    private TaskExecutor _taskExecutor;
    private File _baseFolder;
    private File _logFile;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        final Model model = BrokerModel.getInstance();
        final EventLogger eventLogger = mock(EventLogger.class);

        final SystemConfig<?> systemConfig = mock(SystemConfig.class);
        when(systemConfig.getModel()).thenReturn(model);
        when(systemConfig.getChildExecutor()).thenReturn(_taskExecutor);
        when(systemConfig.getEventLogger()).thenReturn(eventLogger);
        when(systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        doReturn(SystemConfig.class).when(systemConfig).getCategoryClass();

        final Principal systemPrincipal = mock(Principal.class);

        final AccessControl<?> accessControlMock = BrokerTestHelper.createAccessControlMock();
        final Broker broker = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(
                Broker.class, systemPrincipal, accessControlMock);
        when(broker.getModel()).thenReturn(model);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(broker.getParent()).thenReturn(systemConfig);
        doReturn(Broker.class).when(broker).getCategoryClass();

        final VirtualHostNode<?> node =  BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, systemPrincipal, accessControlMock);
        when(node.getModel()).thenReturn(model);
        when(node.getChildExecutor()).thenReturn(_taskExecutor);
        when(node.getParent()).thenReturn(broker);
        when(node.getConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        doReturn(VirtualHostNode.class).when(node).getCategoryClass();
        when(node.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));


        // use real VH object rather then mock in order to test create/start/stop functionality
        final Map<String, Object> attributes = Map.of(VirtualHost.NAME, getTestName(),
                VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        _virtualHost = new TestMemoryVirtualHost(attributes, node);
        _virtualHost.open();

        _baseFolder = new File(TMP_FOLDER, "test-sub-folder");
        _logFile = new File(_baseFolder, "tmp-virtual-host.log." + System.currentTimeMillis());
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    @AfterEach
    public void tearDown()
    {
        try
        {
            _virtualHost.close();
            _taskExecutor.stopImmediately();
        }
        finally
        {
            if (_baseFolder != null && _baseFolder.exists())
            {
                FileUtils.delete(_baseFolder, true);
            }
        }
    }

    @Test
    public void testAddLoggerWithDefaultSettings()
    {
        final VirtualHostLogger<?> logger = createVirtualHostLogger();

        assertTrue(logger instanceof VirtualHostFileLogger, "Unexpected logger created " + logger);
        assertEquals(_logFile.getPath(), ((VirtualHostFileLogger<?>) logger).getFileName(), "Unexpected log file");
        assertEquals(State.ACTIVE, logger.getState(), "Unexpected state on creation");
        assertTrue(_logFile.exists(), "Log file does not exists");

        final Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertTrue(appender.isStarted(), "Appender was not started");
    }

    @Test
    public void testAddLoggerWithRollDailyOn()
    {
        final VirtualHostLogger<?> logger = createVirtualHostLogger(Map.of("rollDaily", true));

        assertTrue(logger instanceof VirtualHostFileLogger, "Unexpected logger created " + logger);
        assertEquals(_logFile.getPath(), ((VirtualHostFileLogger<?>) logger).getFileName(), "Unexpected log file");
        assertEquals(State.ACTIVE, logger.getState(), "Unexpected state on creation");
        assertTrue(_logFile.exists(), "Log file does not exists");

        final Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertTrue(appender.isStarted(), "Appender was not started");
    }

    @Test
    public void testDeleteLogger()
    {
        final VirtualHostLogger<?> logger = createVirtualHostLogger();
        assertEquals(State.ACTIVE, logger.getState(), "Unexpected state on creation");

        Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertTrue(appender.isStarted(), "Appender is not started");

        logger.delete();

        appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).getAppender(logger.getName());
        assertNull(appender, "Appender should be detached on logger deletion");
    }

    @Test
    public void testLoggersRemovedOnVirtualHostStop()
    {
        final VirtualHostLogger<?> logger = createVirtualHostLogger();
        ((AbstractConfiguredObject<?>)_virtualHost).stop();

        final Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertNull(appender, "Appender was not deleted");
    }

    @Test
    public void testLoggersRemovedOnVirtualHostClose()
    {
        final VirtualHostLogger<?> logger = createVirtualHostLogger();
        _virtualHost.close();

        final Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertNull(appender, "Appender was not deleted");
    }

    @Test
    public void testGetLogFiles()
    {
        final VirtualHostFileLogger<?> logger = (VirtualHostFileLogger<?>)createVirtualHostLogger();

        final Collection<LogFileDetails> logFileDetails = logger.getLogFiles();
        assertEquals(1, logFileDetails.size(), "File details should not be empty");

        for (final LogFileDetails logFile : logFileDetails)
        {
            assertEquals(_logFile.getName(), logFile.getName(), "Unexpected log name");
            assertEquals(0, logFile.getSize(), "Unexpected log name");
            assertEquals(_logFile.lastModified(), logFile.getLastModified(), "Unexpected log name");
        }
    }

    @Test
    public void testGetLogFilesOnResolutionErrors()
    {
        final VirtualHostFileLogger<?> logger = createErrorredLogger();

        final Collection<LogFileDetails> logFileDetails = logger.getLogFiles();
        assertTrue(logFileDetails.isEmpty(), "File details should be empty");
    }

    @Test
    public void testStopLoggingLoggerInErroredState()
    {
        final VirtualHostFileLogger<?> logger = createErrorredLogger();
        logger.stopLogging();
    }

    @Test
    public void testStatistics()
    {
        final String loggerName = getTestName();
        final VirtualHostFileLogger<?> logger = (VirtualHostFileLogger<?>) createVirtualHostLogger();
        logger.open();

        final Logger messageLogger = LoggerFactory.getLogger(loggerName);

        assertEquals(0L, logger.getWarnCount());
        assertEquals(0L, logger.getErrorCount());

        final VirtualHostLogInclusionRule<?> warnFilter = logger.createChild(VirtualHostLogInclusionRule.class,
                createInclusionRuleAttributes(loggerName, LogLevel.WARN));

        final Subject subject = new Subject(false, Set.of(_virtualHost.getPrincipal()), Set.of(), Set.of());
        Subject.doAs(subject, (PrivilegedAction<Void>) () ->
        {
            messageLogger.warn("warn");
            return null;
        });

        assertEquals(1L, logger.getWarnCount());
        assertEquals(0L, logger.getErrorCount());

        Subject.doAs(subject, (PrivilegedAction<Void>) () ->
        {
            messageLogger.error("error");
            return null;
        });
        assertEquals(1L, logger.getWarnCount());
        assertEquals(1L, logger.getErrorCount());

        warnFilter.delete();

        final VirtualHostLogInclusionRule<?> errorFilter = logger.createChild(VirtualHostLogInclusionRule.class,
                createInclusionRuleAttributes(loggerName, LogLevel.ERROR));

        Subject.doAs(subject, (PrivilegedAction<Void>) () ->
        {
            messageLogger.warn("warn");
            return null;
        });
        assertEquals(1L, logger.getWarnCount());
        assertEquals(1L, logger.getErrorCount());

        Subject.doAs(subject, (PrivilegedAction<Void>) () ->
        {
            messageLogger.error("error");
            return null;
        });
        assertEquals(1L, logger.getWarnCount());
        assertEquals(2L, logger.getErrorCount());

        logger.resetStatistics();

        assertEquals(0L, logger.getWarnCount());
        assertEquals(0L, logger.getErrorCount());

        errorFilter.delete();
    }

    private VirtualHostFileLogger createErrorredLogger()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHostLogger.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);
        attributes.put(VirtualHostFileLogger.FILE_NAME, _logFile.getPath());
        attributes.put(VirtualHostFileLogger.MAX_FILE_SIZE, "invalid");

        final VirtualHostFileLogger<?> logger = new VirtualHostFileLoggerImpl(attributes, _virtualHost);
        logger.open();

        assertEquals(State.ERRORED, logger.getState(), "Unexpected state");
        return logger;
    }

    private VirtualHostLogger<?> createVirtualHostLogger()
    {
        return createVirtualHostLogger(Map.of());
    }

    private VirtualHostLogger<?> createVirtualHostLogger(final Map<String, Object> additionalAttributes)
    {
        final Map<String, Object> attributes = Map.of(VirtualHostLogger.NAME, getTestName(),
                ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE,
                VirtualHostFileLogger.FILE_NAME, _logFile.getPath());
        return _virtualHost.createChild(VirtualHostLogger.class, attributes);
    }

    private Map<String, Object> createInclusionRuleAttributes(final String loggerName,
                                                              final LogLevel logLevel)
    {
        return Map.of(VirtualHostNameAndLevelLogInclusionRule.LOGGER_NAME, loggerName,
                VirtualHostNameAndLevelLogInclusionRule.LEVEL, logLevel,
                VirtualHostNameAndLevelLogInclusionRule.NAME, "test",
                ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);
    }
}
