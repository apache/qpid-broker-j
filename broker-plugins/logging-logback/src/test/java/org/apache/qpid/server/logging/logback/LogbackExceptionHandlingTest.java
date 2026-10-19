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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Arrays;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import org.apache.qpid.test.utils.UnitTestBase;

public class LogbackExceptionHandlingTest extends UnitTestBase
{
    private static final String TEST_LOG_MESSAGE = "hello";
    private static final Marker TEST_MARKER = MarkerFactory.getMarker("exception-handling-test");

    private LoggerContext _loggerContext;
    private Logger _logger;
    private ListAppender<ILoggingEvent> _appender;
    private PatternLayout _layout;

    @BeforeEach
    public void setUp()
    {
        _loggerContext = new LoggerContext();
        _loggerContext.setMDCAdapter(new LogbackMDCAdapter());
        _loggerContext.start();
        _logger = _loggerContext.getLogger(getTestClassName());
        _logger.setLevel(Level.INFO);
        _logger.setAdditive(false);

        _appender = new ListAppender<>();
        _appender.setContext(_loggerContext);
        _appender.start();
        _logger.addAppender(_appender);

        _layout = new PatternLayout();
        _layout.setContext(_loggerContext);
        _layout.setPattern("%msg%n%ex");
        _layout.start();
    }

    @AfterEach
    public void tearDown()
    {
        _layout.stop();
        _loggerContext.stop();
    }

    @Test
    public void testSingleException()
    {
        final Exception exception = new IllegalStateException("outer");
        final ILoggingEvent event = logException(exception, false);

        assertNull(event.getThrowableProxy().getCause());
        assertEquals(0, event.getThrowableProxy().getSuppressed().length);
        assertFalse(_layout.doLayout(event).contains("CIRCULAR REFERENCE"));
    }

    @Test
    public void testCauseAndSuppressedException()
    {
        final Exception cause = new IllegalArgumentException("cause");
        final Exception suppressed = new UnsupportedOperationException("suppressed");
        final Exception exception = new IllegalStateException("outer", cause);
        exception.addSuppressed(suppressed);

        final ILoggingEvent event = logException(exception, false);
        assertThrowable(cause, event.getThrowableProxy().getCause());
        assertEquals(1, event.getThrowableProxy().getSuppressed().length);
        assertThrowable(suppressed, event.getThrowableProxy().getSuppressed()[0]);
        assertRendered(event, "Caused by: " + cause);
        assertRendered(event, "Suppressed: " + suppressed);
        assertFalse(_layout.doLayout(event).contains("CIRCULAR REFERENCE"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCircularCauses(final boolean parameterized)
    {
        final Exception exception = new IllegalStateException("outer");
        final Exception cause = new IllegalArgumentException("cause", exception);
        exception.initCause(cause);

        final ILoggingEvent event = logException(exception, parameterized);
        final IThrowableProxy causeProxy = event.getThrowableProxy().getCause();
        assertThrowable(cause, causeProxy);
        assertCyclicReference(exception, causeProxy.getCause());
        assertRendered(event, "Caused by: " + cause);
        assertRendered(event, "CIRCULAR REFERENCE");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCircularSuppressedExceptions(final boolean parameterized)
    {
        final Exception exception = new IllegalStateException("outer");
        final Exception suppressed = new IllegalArgumentException("suppressed");
        exception.addSuppressed(suppressed);
        suppressed.addSuppressed(exception);

        final ILoggingEvent event = logException(exception, parameterized);
        assertEquals(1, event.getThrowableProxy().getSuppressed().length);
        final IThrowableProxy suppressedProxy = event.getThrowableProxy().getSuppressed()[0];
        assertThrowable(suppressed, suppressedProxy);
        assertEquals(1, suppressedProxy.getSuppressed().length);
        assertCyclicReference(exception, suppressedProxy.getSuppressed()[0]);
        assertRendered(event, "Suppressed: " + suppressed);
        assertRendered(event, "CIRCULAR REFERENCE");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMixedCauseAndSuppressedCycle(final boolean parameterized)
    {
        final Exception cause = new IllegalArgumentException("cause");
        final Exception exception = new IllegalStateException("outer", cause);
        cause.addSuppressed(exception);

        final ILoggingEvent event = logException(exception, parameterized);
        final IThrowableProxy causeProxy = event.getThrowableProxy().getCause();
        assertThrowable(cause, causeProxy);
        assertEquals(1, causeProxy.getSuppressed().length);
        assertCyclicReference(exception, causeProxy.getSuppressed()[0]);
        assertRendered(event, "Caused by: " + cause);
        assertRendered(event, "CIRCULAR REFERENCE");
    }

    @Test
    public void testCauseSharedWithNestedSuppressedException()
    {
        final Exception shared = new IllegalArgumentException("shared");
        final Exception suppressed = new UnsupportedOperationException("suppressed");
        final Exception exception = new IllegalStateException("outer", shared);
        exception.addSuppressed(suppressed);
        suppressed.addSuppressed(shared);

        final ILoggingEvent event = logException(exception, false);
        assertThrowable(shared, event.getThrowableProxy().getCause());
        assertEquals(1, event.getThrowableProxy().getSuppressed().length);
        final IThrowableProxy suppressedProxy = event.getThrowableProxy().getSuppressed()[0];
        assertThrowable(suppressed, suppressedProxy);
        assertEquals(1, suppressedProxy.getSuppressed().length);
        // Logback also uses a cyclic reference for a shared throwable in an otherwise acyclic graph.
        assertCyclicReference(shared, suppressedProxy.getSuppressed()[0]);
        assertRendered(event, "CIRCULAR REFERENCE");
    }

    @Test
    public void testSuppressedExceptionSharedWithNestedCause()
    {
        final Exception shared = new UnsupportedOperationException("shared");
        final Exception cause = new IllegalArgumentException("cause", shared);
        final Exception exception = new IllegalStateException("outer", cause);
        exception.addSuppressed(shared);

        final ILoggingEvent event = logException(exception, false);
        assertThrowable(cause, event.getThrowableProxy().getCause());
        assertThrowable(shared, event.getThrowableProxy().getCause().getCause());
        assertEquals(1, event.getThrowableProxy().getSuppressed().length);
        assertCyclicReference(shared, event.getThrowableProxy().getSuppressed()[0]);
        assertRendered(event, "CIRCULAR REFERENCE");
    }

    @Test
    public void testCircularExceptionAfterBrokerStartup()
    {
        final Logger rootLogger = _loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        _logger.setAdditive(true);
        final LogbackLoggingSystemLauncherListener listener = beforeStartup();
        try
        {
            final Exception exception = new IllegalStateException("outer");
            exception.initCause(new IllegalArgumentException("cause", exception));

            final ILoggingEvent event = logException(exception, false);
            assertCyclicReference(exception, event.getThrowableProxy().getCause().getCause());
            assertRendered(event, "CIRCULAR REFERENCE");

            final StartupAppender startupAppender =
                    assertInstanceOf(StartupAppender.class, rootLogger.getAppender(StartupAppender.class.getName()));
            startupAppender.replayAccumulatedEvents(_appender);
            assertEquals(2, _appender.list.size());
            assertSame(event, _appender.list.get(1));
        }
        finally
        {
            listener.afterStartup();
        }
    }

    @Test
    public void testDisabledLevelDoesNotInspectExceptionAfterBrokerStartup()
    {
        final LogbackLoggingSystemLauncherListener listener = beforeStartup();
        try
        {
            _logger.setLevel(Level.WARN);
            final Throwable exception = mock(Throwable.class);

            _logger.info(TEST_LOG_MESSAGE, exception);

            assertTrue(_appender.list.isEmpty());
            verifyNoInteractions(exception);
        }
        finally
        {
            listener.afterStartup();
        }
    }

    private LogbackLoggingSystemLauncherListener beforeStartup()
    {
        final Logger rootLogger = _loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        final LogbackLoggingSystemLauncherListener listener = new LogbackLoggingSystemLauncherListener();
        try (final MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class))
        {
            loggerFactory.when(() -> LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).thenReturn(rootLogger);
            listener.beforeStartup();
        }
        return listener;
    }

    private ILoggingEvent logException(final Throwable exception, final boolean parameterized)
    {
        if (parameterized)
        {
            _logger.info(TEST_MARKER, TEST_LOG_MESSAGE + " {}", "world", exception);
        }
        else
        {
            _logger.info(TEST_MARKER, TEST_LOG_MESSAGE, exception);
        }

        assertEquals(1, _appender.list.size(), "Exactly one logging event should be emitted");
        final ILoggingEvent event = _appender.list.get(0);
        assertEquals(Level.INFO, event.getLevel());
        assertEquals(_logger.getName(), event.getLoggerName());
        assertEquals(List.of(TEST_MARKER), event.getMarkerList());
        assertEquals(parameterized ? TEST_LOG_MESSAGE + " {}" : TEST_LOG_MESSAGE, event.getMessage());
        assertEquals(parameterized ? TEST_LOG_MESSAGE + " world" : TEST_LOG_MESSAGE, event.getFormattedMessage());
        if (parameterized)
        {
            assertArrayEquals(new Object[] {"world"}, event.getArgumentArray());
        }
        else
        {
            assertNull(event.getArgumentArray());
        }
        assertThrowable(exception, event.getThrowableProxy());
        assertRendered(event, exception.toString());
        return event;
    }

    private void assertThrowable(final Throwable expected, final IThrowableProxy actual)
    {
        final ThrowableProxy proxy = assertInstanceOf(ThrowableProxy.class, actual);
        assertSame(expected, proxy.getThrowable());
        assertEquals(expected.getClass().getName(), actual.getClassName());
        assertEquals(expected.getMessage(), actual.getMessage());
        assertFalse(actual.isCyclic());
        assertArrayEquals(expected.getStackTrace(), Arrays.stream(actual.getStackTraceElementProxyArray())
                .map(StackTraceElementProxy::getStackTraceElement).toArray(StackTraceElement[]::new));
    }

    private void assertCyclicReference(final Throwable expected, final IThrowableProxy actual)
    {
        final ThrowableProxy proxy = assertInstanceOf(ThrowableProxy.class, actual);
        assertSame(expected, proxy.getThrowable());
        assertEquals(expected.getClass().getName(), actual.getClassName());
        assertEquals(expected.getMessage(), actual.getMessage());
        assertTrue(actual.isCyclic());
        assertNull(actual.getCause());
        assertEquals(0, actual.getSuppressed().length);
        assertEquals(0, actual.getStackTraceElementProxyArray().length);
    }

    private void assertRendered(final ILoggingEvent event, final String expected)
    {
        final String rendered = _layout.doLayout(event);
        assertTrue(rendered.contains(expected), rendered);
    }
}
