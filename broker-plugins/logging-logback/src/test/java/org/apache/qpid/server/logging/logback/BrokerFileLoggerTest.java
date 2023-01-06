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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes"})
public class BrokerFileLoggerTest extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private File _baseFolder;
    private File _logFile;
    private Broker _broker;
    private BrokerFileLogger<?> _logger;

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
        doReturn(SystemConfig.class).when(systemConfig).getCategoryClass();

        _broker = mock(Broker.class);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getParent()).thenReturn(systemConfig);
        doReturn(Broker.class).when(_broker).getCategoryClass();

        _baseFolder = new File(TMP_FOLDER, "test-sub-folder");
        _logFile = new File(_baseFolder, "tmp-broker-host.log." + System.currentTimeMillis());
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_logger != null)
        {
            _logger.close();
            _logger.stopLogging();
        }
        _taskExecutor.stopImmediately();
        if (_baseFolder != null && _baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    @Test
    public void testGetLogFilesOnResolutionErrors()
    {
        _logger = createLoggerInErroredState();

        final List<LogFileDetails> logFileDetails = _logger.getLogFiles();
        assertTrue(logFileDetails.isEmpty(), "File details should be empty");
    }

    private BrokerFileLogger createLoggerInErroredState()
    {
        final Map<String, Object> attributes = Map.of(BrokerLogger.NAME, getTestName(),
                ConfiguredObject.TYPE, BrokerFileLogger.TYPE,
                BrokerFileLogger.FILE_NAME, _logFile.getPath(),
                BrokerFileLogger.MAX_FILE_SIZE, "invalid");
        final BrokerFileLogger logger = new BrokerFileLoggerImpl(attributes, _broker);
        logger.open();

        assertEquals(State.ERRORED, logger.getState(), "Unexpected state");
        return logger;
    }
}
