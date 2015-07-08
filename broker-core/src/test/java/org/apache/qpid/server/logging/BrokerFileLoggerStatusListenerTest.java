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

package org.apache.qpid.server.logging;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOError;
import java.io.IOException;

import ch.qos.logback.core.status.Status;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.test.utils.QpidTestCase;

public class BrokerFileLoggerStatusListenerTest extends QpidTestCase
{
    private BrokerFileLoggerImpl.BrokerFileLoggerStatusListener _statusListener;
    private BrokerFileLogger<?> _fileLogger;
    private SystemConfig<?> _systemConfig;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _fileLogger = mock(BrokerFileLogger.class);
        _systemConfig = mock(SystemConfig.class);
        EventLogger eventLogger = mock(EventLogger.class);
        when(_systemConfig.getEventLogger()).thenReturn(eventLogger);
        _statusListener = new BrokerFileLoggerImpl.BrokerFileLoggerStatusListener(_fileLogger, _systemConfig);
        when(_fileLogger.getContextValue(Boolean.class, BrokerFileLogger.BROKER_FAIL_ON_LOGGER_IO_ERROR)).thenReturn(true);
    }

    public void testAddStatusEventForIOError() throws Exception
    {
        Status event = createEvent(new IOError(new IOException("Mocked: No disk space left")), Status.ERROR);
        _statusListener.addStatusEvent(event);

        verify(_systemConfig).closeAsync();
    }

    public void testAddStatusEventForIOErrorWithFailOnLoggerIOErrorDisabled() throws Exception
    {
        Status event = createEvent(new IOError(new IOException("Mocked: No disk space left")), Status.ERROR);
        when(_fileLogger.getContextValue(Boolean.class, BrokerFileLogger.BROKER_FAIL_ON_LOGGER_IO_ERROR)).thenReturn(false);
        _statusListener.addStatusEvent(event);

        verify(_systemConfig, never()).closeAsync();
    }

    public void testAddStatusEventForIOException() throws Exception
    {
        Status event = createEvent(new IOException("Mocked: No disk space left"), Status.ERROR);
        _statusListener.addStatusEvent(event);

        verify(_systemConfig).closeAsync();
    }

    public void testAddStatusEventForIOExceptionReportedAsWarning() throws Exception
    {
        Status event = createEvent(new IOException("Mocked: No disk space left"), Status.WARN);
        _statusListener.addStatusEvent(event);

        verify(_systemConfig, never()).closeAsync();
    }

    public void testAddStatusEventForNonIOException() throws Exception
    {
        Status event = createEvent(new RuntimeException("Mocked: No disk space left"), Status.ERROR);
        _statusListener.addStatusEvent(event);

        verify(_systemConfig, never()).closeAsync();
    }

    private Status createEvent(Throwable throwable, int status)
    {
        Status event = mock(Status.class);
        when(event.getThrowable()).thenReturn(throwable);
        when(event.getEffectiveLevel()).thenReturn(status);
        return event;
    }
}
