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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostLogger;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostLoggerTest extends UnitTestBase
{
    private VirtualHost<?> _virtualHost;
    private TaskExecutor _taskExecutor;
    private File _baseFolder;
    private File _logFile;


    @Before
    public void setUp() throws Exception
    {
        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        Model model = BrokerModel.getInstance();

        EventLogger eventLogger = mock(EventLogger.class);

        SystemConfig<?> systemConfig = mock(SystemConfig.class);
        when(systemConfig.getModel()).thenReturn(model);
        when(systemConfig.getChildExecutor()).thenReturn(_taskExecutor);
        when(systemConfig.getEventLogger()).thenReturn(eventLogger);
        when(systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        doReturn(SystemConfig.class).when(systemConfig).getCategoryClass();

        Principal systemPrincipal = mock(Principal.class);

        AccessControl accessControlMock = BrokerTestHelper.createAccessControlMock();
        Broker broker = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(Broker.class, systemPrincipal,
                                                                                 accessControlMock);
        when(broker.getModel()).thenReturn(model);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(broker.getParent()).thenReturn(systemConfig);
        doReturn(Broker.class).when(broker).getCategoryClass();

        VirtualHostNode node =  BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, systemPrincipal, accessControlMock);
        when(node.getModel()).thenReturn(model);
        when(node.getChildExecutor()).thenReturn(_taskExecutor);
        when(node.getParent()).thenReturn(broker);
        when(node.getConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        doReturn(VirtualHostNode.class).when(node).getCategoryClass();
        when(node.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));


        // use real VH object rather then mock in order to test create/start/stop functionality
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHost.NAME, getTestName());
        attributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        _virtualHost = new TestMemoryVirtualHost(attributes, node);
        _virtualHost.open();

        _baseFolder = new File(TMP_FOLDER, "test-sub-folder");
        _logFile = new File(_baseFolder, "tmp-virtual-host.log." + System.currentTimeMillis());
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    @After
    public void tearDown() throws Exception
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
        VirtualHostLogger logger = createVirtualHostLogger();

        assertTrue("Unexpected logger created " + logger, logger instanceof VirtualHostFileLogger);
        assertEquals("Unexpected log file", _logFile.getPath(), ((VirtualHostFileLogger<?>) logger).getFileName());
        assertEquals("Unexpected state on creation", State.ACTIVE, logger.getState());
        assertTrue("Log file does not exists", _logFile.exists());

        Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertTrue("Appender was not started", appender.isStarted());
    }

    @Test
    public void testAddLoggerWithRollDailyOn()
    {
        VirtualHostLogger logger = createVirtualHostLogger(Collections.<String, Object>singletonMap("rollDaily", true));

        assertTrue("Unexpected logger created " + logger, logger instanceof VirtualHostFileLogger);
        assertEquals("Unexpected log file", _logFile.getPath(), ((VirtualHostFileLogger<?>) logger).getFileName());
        assertEquals("Unexpected state on creation", State.ACTIVE, logger.getState());
        assertTrue("Log file does not exists", _logFile.exists());

        Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertTrue("Appender was not started", appender.isStarted());
    }

    @Test
    public void testDeleteLogger()
    {
        VirtualHostLogger logger = createVirtualHostLogger();
        assertEquals("Unexpected state on creation", State.ACTIVE, logger.getState());

        Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertTrue("Appender is not started", appender.isStarted());

        logger.delete();

        appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).getAppender(logger.getName());
        assertNull("Appender should be detached on logger deletion", appender);
    }


    @Test
    public void testLoggersRemovedOnVirtualHostStop()
    {
        VirtualHostLogger logger = createVirtualHostLogger();
        ((AbstractConfiguredObject<?>)_virtualHost).stop();

        Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertNull("Appender was not deleted", appender);
    }

    @Test
    public void testLoggersRemovedOnVirtualHostClose()
    {
        VirtualHostLogger logger = createVirtualHostLogger();
        _virtualHost.close();

        Appender<ILoggingEvent> appender = ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .getAppender(logger.getName());
        assertNull("Appender was not deleted", appender);
    }

    @Test
    public void testGetLogFiles()
    {
        VirtualHostFileLogger logger = (VirtualHostFileLogger)createVirtualHostLogger();

        Collection<LogFileDetails> logFileDetails = logger.getLogFiles();
        assertEquals("File details should not be empty", 1, logFileDetails.size());

        for(LogFileDetails logFile : logFileDetails)
        {
            assertEquals("Unexpected log name", _logFile.getName(), logFile.getName());
            assertEquals("Unexpected log name", 0, logFile.getSize());
            assertEquals("Unexpected log name", _logFile.lastModified(), logFile.getLastModified());
        }
    }

    @Test
    public void testGetLogFilesOnResolutionErrors()
    {
        VirtualHostFileLogger logger = createErrorredLogger();

        Collection<LogFileDetails> logFileDetails = logger.getLogFiles();
        assertTrue("File details should be empty", logFileDetails.isEmpty());
    }

    @Test
    public void testStopLoggingLoggerInErroredState()
    {
        VirtualHostFileLogger logger = createErrorredLogger();
        logger.stopLogging();
    }

    private VirtualHostFileLogger createErrorredLogger()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHostLogger.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);
        attributes.put(VirtualHostFileLogger.FILE_NAME, _logFile.getPath());
        attributes.put(VirtualHostFileLogger.MAX_FILE_SIZE, "invalid");

        VirtualHostFileLogger logger = new VirtualHostFileLoggerImpl(attributes, _virtualHost);
        logger.open();

        assertEquals("Unexpected state", State.ERRORED, logger.getState());
        return logger;
    }

    private VirtualHostLogger createVirtualHostLogger()
    {
        return createVirtualHostLogger(Collections.<String,Object>emptyMap());
    }

    private VirtualHostLogger createVirtualHostLogger(Map<String, Object> additionalAttributes)
    {
        Map<String, Object> attributes = new HashMap<>(additionalAttributes);
        attributes.put(VirtualHostLogger.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);
        attributes.put(VirtualHostFileLogger.FILE_NAME, _logFile.getPath());
        return _virtualHost.createChild(VirtualHostLogger.class, attributes);
    }

}
