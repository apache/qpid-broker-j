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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

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

    @Test
    public void testInitialArchiveScanFailurePreventsActivation() throws Exception
    {
        final Path directory = Files.createDirectories(_baseFolder.toPath()).toAbsolutePath();
        Files.writeString(_logFile.toPath(), "existing log");
        final Map<String, Object> attributes = Map.of(BrokerLogger.NAME, getTestName(),
                ConfiguredObject.TYPE, BrokerFileLogger.TYPE, BrokerFileLogger.FILE_NAME, _logFile.getPath(),
                "rollDaily", true);
        final BrokerFileLoggerImpl logger = new BrokerFileLoggerImpl(attributes, _broker);
        _logger = logger;
        final AtomicBoolean failNextListing = new AtomicBoolean(true);
        CompletableFuture.runAsync(() ->
        {
            try (final MockedStatic<Files> mockedFiles = mockStatic(Files.class, invocation ->
            {
                if ("newDirectoryStream".equals(invocation.getMethod().getName()) &&
                        invocation.getMethod().getParameterCount() == 1 &&
                        directory.equals(invocation.getArgument(0)) && failNextListing.compareAndSet(true, false))
                {
                    throw new IOException("Temporary archive listing failure");
                }
                return invocation.callRealMethod();
            }))
            {
                logger.open();
            }
        }, _taskExecutor).get(10, TimeUnit.SECONDS);

        assertFalse(failNextListing.get(), "The initial archive listing must fail");
        assertFalse(logger.getContextValue(Boolean.class, BrokerFileLogger.BROKER_FAIL_ON_LOGGER_IO_ERROR));
        assertEquals(State.ERRORED, logger.getState(), "A failed logging policy must prevent activation");
        assertNull(logger.getAppender(), "A failed appender must not be attached");
        verify(logger.getAncestor(SystemConfig.class), never()).closeAsync();

        try (final DirectoryStream<Path> entries = Files.newDirectoryStream(directory))
        {
            assertTrue(entries.iterator().hasNext(), "Directory access has recovered");
        }
        assertEquals(State.ERRORED, logger.getState(), "Restoring directory access must not hide the startup failure");
        assertEquals("existing log", Files.readString(_logFile.toPath()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testStartupDoesNotWaitForUnrelatedSubdirectory(final boolean rollDaily) throws Exception
    {
        final Path unrelated = Files.createDirectories(_baseFolder.toPath().resolve("unrelated")).toAbsolutePath();
        Files.writeString(unrelated.resolve("unrelated.txt"), "not a log archive");
        final Map<String, Object> attributes = Map.of(BrokerLogger.NAME, getTestName(),
                ConfiguredObject.TYPE, BrokerFileLogger.TYPE, BrokerFileLogger.FILE_NAME, _logFile.getPath(),
                "rollDaily", rollDaily);
        final BrokerFileLoggerImpl logger = new BrokerFileLoggerImpl(attributes, _broker);
        _logger = logger;
        final AtomicBoolean unrelatedDirectoryOpened = new AtomicBoolean();
        final CountDownLatch release = new CountDownLatch(1);
        final CompletableFuture<Void> opening = CompletableFuture.runAsync(() ->
        {
            try (final MockedStatic<Files> mockedFiles = mockStatic(Files.class, invocation ->
            {
                if ("newDirectoryStream".equals(invocation.getMethod().getName()) &&
                        invocation.getMethod().getParameterCount() == 1 &&
                        unrelated.equals(invocation.getArgument(0)))
                {
                    unrelatedDirectoryOpened.set(true);
                    assertTrue(release.await(30, TimeUnit.SECONDS), "The test must release the blocked listing");
                }
                return invocation.callRealMethod();
            }))
            {
                logger.open();
            }
        }, _taskExecutor);

        try
        {
            opening.get(10, TimeUnit.SECONDS);
            assertFalse(unrelatedDirectoryOpened.get(), "Startup must not open unrelated subdirectories");
            assertEquals(State.ACTIVE, logger.getState());
            assertTrue(logger.getAppender().isStarted());
        }
        finally
        {
            release.countDown();
            opening.get(10, TimeUnit.SECONDS);
        }
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
