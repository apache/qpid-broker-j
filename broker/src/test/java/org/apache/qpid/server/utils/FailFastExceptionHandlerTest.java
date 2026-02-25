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

package org.apache.qpid.server.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FailFastExceptionHandlerTest
{
    private static final String EXITING = "# Exiting";
    private static final String FORCED_TO_CONTINUE = "# Forced to continue by JVM setting 'qpid.broker.exceptionHandler.continue'";

    private final FailFastExceptionHandler handler = new FailFastExceptionHandler();
    private final PrintStream standardErr = System.err;
    private final ByteArrayOutputStream errorStreamCaptor = new ByteArrayOutputStream();

    @BeforeEach
    public void setUp()
    {
        System.setErr(new PrintStream(errorStreamCaptor));
    }

    /** Clear the system property between the tests */
    @AfterEach
    void tearDown()
    {
        System.clearProperty(FailFastExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME);
        System.setErr(standardErr);
    }

    /** ExceptionHandler should call ExitHandler.halt(1) when continueOnError is false */
    @Test
    void exceptionHandlerCallsExitHandlerExitOnFlagFalse()
    {
        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            System.setProperty(FailFastExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME, "false");
            final Thread testThread = mock(Thread.class);
            final Throwable testThrowable = new RuntimeException("Test Exception");

            handler.uncaughtException(testThread, testThrowable);

            // verify that ExitHandler.exit(1) was called
            mockedExitHandler.verify(() -> ExitHandler.halt(1));
            mockedExitHandler.verify(() -> ExitHandler.exit(anyInt()), never());

            assertTrue(errorStreamCaptor.toString().contains(EXITING));
            assertFalse(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }

    /** ExceptionHandler should call ExitHandler.halt(1) when continueOnError property is not set */
    @Test
    void exceptionHandlerCallsExitHandlerExitOnFlagNotSet()
    {
        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            final Thread testThread = mock(Thread.class);
            final Throwable testThrowable = new RuntimeException("Test Exception");

            handler.uncaughtException(testThread, testThrowable);

            // verify that ExitHandler.exit(1) was called
            mockedExitHandler.verify(() -> ExitHandler.halt(1));
            mockedExitHandler.verify(() -> ExitHandler.exit(anyInt()), never());

            assertTrue(errorStreamCaptor.toString().contains(EXITING));
            assertFalse(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }

    /** ExceptionHandler shouldn't call ExitHandler.halt() when continueOnError is true */
    @Test
    void exceptionHandlerCallsExitHandlerExitOnFlagTrue()
    {
        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            System.setProperty(FailFastExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME, "true");
            final Thread testThread = mock(Thread.class);
            final Throwable testThrowable = new RuntimeException("Test Exception");

            handler.uncaughtException(testThread, testThrowable);

            // verify that ExitHandler.exit() or ExitHandler.halt() was never called
            mockedExitHandler.verify(() -> ExitHandler.halt(anyInt()), never());
            mockedExitHandler.verify(() -> ExitHandler.exit(anyInt()), never());

            assertFalse(errorStreamCaptor.toString().contains(EXITING));
            assertTrue(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }

    /** ExceptionHandler should call diagnostic and logging methods */
    @Test
    void exceptionHandlerCallsDiagnosticAndLoggingMethods()
    {
        final FailFastExceptionHandler spyHandler = spy(new FailFastExceptionHandler());
        final Thread testThread = mock(Thread.class);
        final Throwable testThrowable = new RuntimeException("Test Exception");
        final boolean continueOnError = Boolean.getBoolean(FailFastExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME);

        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            spyHandler.uncaughtException(testThread, testThrowable);

            // verify that printDiagnostic and logError were called with the correct parameters.
            verify(spyHandler, times(1)).printDiagnostic(testThread, testThrowable, continueOnError);
            verify(spyHandler, times(1)).logError(testThrowable, continueOnError);

            // verify that ExitHandler.exit(1) was called
            mockedExitHandler.verify(() -> ExitHandler.halt(1));
            mockedExitHandler.verify(() -> ExitHandler.exit(anyInt()), never());

            assertTrue(errorStreamCaptor.toString().contains(EXITING));
            assertFalse(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }
}
