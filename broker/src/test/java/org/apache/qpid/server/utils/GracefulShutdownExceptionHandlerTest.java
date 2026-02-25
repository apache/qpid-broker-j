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

public class GracefulShutdownExceptionHandlerTest
{
    private static final String EXITING = "# Exiting";
    private static final String FORCED_TO_CONTINUE = "# Forced to continue by JVM setting 'qpid.broker.exceptionHandler.continue'";

    private final GracefulShutdownExceptionHandler handler = new GracefulShutdownExceptionHandler();
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
        System.clearProperty(GracefulShutdownExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME);
        System.setErr(standardErr);
    }

    /** ExceptionHandler should call ExitHandler.exit(1) when continueOnError is false */
    @Test
    void exceptionHandlerCallsExitHandlerExitOnFlagFalse()
    {
        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            System.setProperty(GracefulShutdownExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME, "false");
            final Thread testThread = mock(Thread.class);
            final Throwable testThrowable = new RuntimeException("Test Exception");

            handler.uncaughtException(testThread, testThrowable);

            // verify that ExitHandler.exit(1) was called
            mockedExitHandler.verify(() -> ExitHandler.exit(1));
            mockedExitHandler.verify(() -> ExitHandler.halt(anyInt()), never());

            assertTrue(errorStreamCaptor.toString().contains(EXITING));
            assertFalse(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }

    /** ExceptionHandler should call ExitHandler.exit(1) when continueOnError property is not set */
    @Test
    void exceptionHandlerCallsExitHandlerExitOnFlagNotSet()
    {
        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            final Thread testThread = mock(Thread.class);
            final Throwable testThrowable = new RuntimeException("Test Exception");

            handler.uncaughtException(testThread, testThrowable);

            // verify that ExitHandler.exit(1) was called
            mockedExitHandler.verify(() -> ExitHandler.exit(1));
            mockedExitHandler.verify(() -> ExitHandler.halt(anyInt()), never());

            assertTrue(errorStreamCaptor.toString().contains(EXITING));
            assertFalse(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }

    /** ExceptionHandler shouldn't call ExitHandler.exit() when continueOnError is true */
    @Test
    void exceptionHandlerCallsExitHandlerExitOnFlagTrue()
    {
        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            System.setProperty(GracefulShutdownExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME, "true");
            final Thread testThread = mock(Thread.class);
            final Throwable testThrowable = new RuntimeException("Test Exception");

            handler.uncaughtException(testThread, testThrowable);

            // verify that ExitHandler.exit() was never called
            mockedExitHandler.verify(() -> ExitHandler.exit(anyInt()), never());
            mockedExitHandler.verify(() -> ExitHandler.halt(anyInt()), never());

            assertFalse(errorStreamCaptor.toString().contains(EXITING));
            assertTrue(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }

    /** ExceptionHandler should call diagnostic and logging methods */
    @Test
    void exceptionHandlerCallsDiagnosticAndLoggingMethods()
    {
        final GracefulShutdownExceptionHandler spyHandler1 = spy(new GracefulShutdownExceptionHandler());
        final Thread testThread = mock(Thread.class);
        final Throwable testThrowable = new RuntimeException("Test Exception");
        boolean continueOnError = Boolean.getBoolean(GracefulShutdownExceptionHandler.CONTINUE_ON_ERROR_PROPERTY_NAME);

        try (final MockedStatic<ExitHandler> mockedExitHandler = mockStatic(ExitHandler.class))
        {
            spyHandler1.uncaughtException(testThread, testThrowable);

            // verify that printDiagnostic and logError were called with the correct parameters
            verify(spyHandler1, times(1)).printDiagnostic(testThread, testThrowable, continueOnError);
            verify(spyHandler1, times(1)).logError(testThrowable, continueOnError);

            // verify that the exit path was taken, as continueOnError is false by default
            mockedExitHandler.verify(() -> ExitHandler.exit(1));
            mockedExitHandler.verify(() -> ExitHandler.halt(anyInt()), never());

            assertTrue(errorStreamCaptor.toString().contains(EXITING));
            assertFalse(errorStreamCaptor.toString().contains(FORCED_TO_CONTINUE));
        }
    }
}
