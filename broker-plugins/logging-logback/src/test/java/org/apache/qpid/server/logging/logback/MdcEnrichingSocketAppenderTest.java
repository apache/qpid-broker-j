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
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.server.HardenedLoggingEventInputStream;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.LoggingEventVO;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.spi.PreSerializationTransformer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MarkerFactory;
import org.slf4j.event.KeyValuePair;

import org.apache.qpid.test.utils.UnitTestBase;

class MdcEnrichingSocketAppenderTest extends UnitTestBase
{
    private static final int TIMEOUT_SECONDS = 5;
    private static final Map<String, String> PRODUCER_MDC = Map.of("trace", "request", "common", "producer");

    private final AtomicReference<Map<String, String>> _additions = new AtomicReference<>();
    private final List<MdcEnrichingSocketAppender> _appenders = new ArrayList<>();

    private LoggerContext _loggerContext;
    private Logger _logger;
    private CountDownLatch _serializationAllowed;

    @BeforeEach
    void setUp()
    {
        _additions.set(Map.of());
        _serializationAllowed = new CountDownLatch(1);
        _loggerContext = new LoggerContext();
        _loggerContext.setMDCAdapter(new LogbackMDCAdapter());
        _loggerContext.start();
        _logger = _loggerContext.getLogger(getTestClassName());
        _logger.setLevel(Level.INFO);
        _logger.setAdditive(false);
    }

    @AfterEach
    void tearDown()
    {
        _serializationAllowed.countDown();
        for (final MdcEnrichingSocketAppender appender : _appenders)
        {
            appender.stop();
        }
        _appenders.clear();
        _loggerContext.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void enrichmentPreservesOriginalEventAndProducerMdc(final boolean prepareEarlier) throws Exception
    {
        final ListAppender<ILoggingEvent> earlier = addRecordingAppender(prepareEarlier);
        _additions.set(Map.of("common", "socket", "broker", "node"));
        try (final SocketReceiver receiver = new SocketReceiver())
        {
            addSocketAppender(receiver, _additions::get, false);
            final ListAppender<ILoggingEvent> later = addRecordingAppender(false);
            logOnProducer(() -> _logger.info("event"));

            _loggerContext.getMDCAdapter().put("trace", "reader");
            _serializationAllowed.countDown();
            final LoggingEventVO received = receiver.readEvent();

            assertEquals(Map.of("trace", "request", "common", "socket", "broker", "node"),
                    received.getMDCPropertyMap());
            assertEquals("producer-thread", received.getThreadName());
            assertEquals(1, earlier.list.size());
            assertEquals(1, later.list.size());
            assertSame(earlier.list.get(0), later.list.get(0));
            assertEquals(PRODUCER_MDC, later.list.get(0).getMDCPropertyMap());
        }
    }

    @Test
    void eachSocketHasIndependentMdc() throws Exception
    {
        try (final SocketReceiver first = new SocketReceiver();
             final SocketReceiver second = new SocketReceiver())
        {
            addSocketAppender(first, () -> Map.of("common", "first", "first-only", "value"), false);
            addSocketAppender(second, () -> Map.of("common", "second"), false);
            logOnProducer(() -> _logger.info("event"));
            _serializationAllowed.countDown();

            assertEquals(Map.of("trace", "request", "common", "first", "first-only", "value"),
                    first.readEvent().getMDCPropertyMap());
            assertEquals(Map.of("trace", "request", "common", "second"), second.readEvent().getMDCPropertyMap());
        }
    }

    @Test
    void queuedEventsRetainTheirMdcAfterConfigurationChanges() throws Exception
    {
        final Map<String, String> additions = new HashMap<>(Map.of("common", "first"));
        _additions.set(additions);
        try (final SocketReceiver receiver = new SocketReceiver())
        {
            addSocketAppender(receiver, _additions::get, false);
            logOnProducer(() -> _logger.info("first"));
            additions.put("common", "mutated");
            _additions.set(Map.of("common", "second"));
            logOnProducer(() -> _logger.info("second"));
            _additions.set(Map.of());
            logOnProducer(() -> _logger.info("third"));
            _serializationAllowed.countDown();

            final LoggingEventVO first = receiver.readEvent();
            final LoggingEventVO second = receiver.readEvent();
            final LoggingEventVO third = receiver.readEvent();
            assertEquals("first", first.getMessage());
            assertEquals("first", first.getMDCPropertyMap().get("common"));
            assertEquals("second", second.getMessage());
            assertEquals("second", second.getMDCPropertyMap().get("common"));
            assertEquals("third", third.getMessage());
            assertEquals(PRODUCER_MDC, third.getMDCPropertyMap());
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void callerCaptureOnProducer(final boolean includeCallerData, final boolean enrich) throws Exception
    {
        _additions.set(enrich ? Map.of("broker", "node") : Map.of());
        final ListAppender<ILoggingEvent> recording = addRecordingAppender(false);
        try (final SocketReceiver receiver = new SocketReceiver())
        {
            addSocketAppender(receiver, _additions::get, includeCallerData);
            logOnProducer(this::logWithCaller);
            assertEquals(includeCallerData, recording.list.get(0).hasCallerData());
            _serializationAllowed.countDown();
            final LoggingEventVO received = receiver.readEvent();

            assertEquals(includeCallerData, received.hasCallerData());
            if (includeCallerData)
            {
                assertTrue(received.getCallerData().length > 0);
                assertEquals(getClass().getName(), received.getCallerData()[0].getClassName());
                assertEquals("logWithCaller", received.getCallerData()[0].getMethodName());
            }
            else
            {
                assertNull(received.getCallerData());
            }
        }
    }

    @Test
    void nativeSerializationPreservesEventData() throws Exception
    {
        final Map<String, String> additions = new HashMap<>();
        additions.put("broker", "node");
        additions.put("nullable", null);
        _additions.set(additions);
        _loggerContext.putProperty("context-property", "context-value");
        final IllegalStateException exception = new IllegalStateException("outer");
        final IllegalArgumentException cause = new IllegalArgumentException("cause", exception);
        exception.initCause(cause);
        exception.addSuppressed(new UnsupportedOperationException("suppressed"));
        final LoggingEvent original = new LoggingEvent(Logger.class.getName(), _logger, Level.WARN,
                "value={}", exception, new Object[]{"argument"});
        original.setInstant(Instant.parse("2026-09-11T12:34:56.123456789Z"));
        original.setSequenceNumber(42);
        original.addMarker(MarkerFactory.getMarker("first"));
        original.addMarker(MarkerFactory.getMarker("second"));

        try (final SocketReceiver receiver = new SocketReceiver())
        {
            final MdcEnrichingSocketAppender appender = addSocketAppender(receiver, _additions::get, false);
            logOnProducer(() -> appender.doAppend(original));
            _serializationAllowed.countDown();
            final LoggingEventVO received = receiver.readEvent();

            assertEquals("producer-thread", received.getThreadName());
            assertEquals(original.getLoggerName(), received.getLoggerName());
            assertEquals(original.getLevel(), received.getLevel());
            assertEquals(original.getMessage(), received.getMessage());
            assertEquals(original.getFormattedMessage(), received.getFormattedMessage());
            assertArrayEquals(original.getArgumentArray(), received.getArgumentArray());
            assertEquals(original.getTimeStamp(), received.getTimeStamp());
            assertEquals(original.getNanoseconds(), received.getNanoseconds());
            assertEquals(original.getSequenceNumber(), received.getSequenceNumber());
            assertEquals(original.getMarkerList(), received.getMarkerList());
            assertEquals("context-value", received.getLoggerContextVO().getPropertyMap().get("context-property"));
            assertEquals("node", received.getMDCPropertyMap().get("broker"));
            assertTrue(received.getMDCPropertyMap().containsKey("nullable"));
            assertNull(received.getMDCPropertyMap().get("nullable"));
            assertEquals(IllegalStateException.class.getName(), received.getThrowableProxy().getClassName());
            assertEquals("outer", received.getThrowableProxy().getMessage());
            assertEquals("cause", received.getThrowableProxy().getCause().getMessage());
            assertTrue(received.getThrowableProxy().getCause().getCause().isCyclic());
            assertEquals("suppressed", received.getThrowableProxy().getSuppressed()[0].getMessage());
        }
    }

    @Test
    void transformerPreservesStructuredArgumentsAndNativeValueObjects()
    {
        final LoggingEvent original = new LoggingEvent(Logger.class.getName(), _logger, Level.INFO,
                "value={}", null, new Object[]{"argument"});
        original.setKeyValuePairs(List.of(new KeyValuePair("key", "value")));
        original.prepareForDeferredProcessing();
        final MdcEnrichingSocketAppender appender = new MdcEnrichingSocketAppender(_additions::get);
        final PreSerializationTransformer<ILoggingEvent> transformer = appender.getPST();
        final MdcLoggingEvent enriched = new MdcLoggingEvent(original, Map.of("broker", "node"));
        final LoggingEventVO transformed = assertInstanceOf(LoggingEventVO.class, transformer.transform(enriched));

        assertEquals(original.getKeyValuePairs(), transformed.getKeyValuePairs());
        assertEquals(original.getInstant(), enriched.getInstant());
        assertEquals("node", transformed.getMDCPropertyMap().get("broker"));
        final LoggingEventVO enrichedValueObject = assertInstanceOf(LoggingEventVO.class,
                transformer.transform(new MdcLoggingEvent(transformed, Map.of("broker", "another-node"))));
        assertEquals("another-node", enrichedValueObject.getMDCPropertyMap().get("broker"));
        assertEquals("node", transformed.getMDCPropertyMap().get("broker"));
        assertEquals(original.getKeyValuePairs(), enrichedValueObject.getKeyValuePairs());
        assertSame(transformed, transformer.transform(transformed));
        assertNull(transformer.transform(null));
    }

    @Test
    void rejectedEventsAreNotEnriched() throws Exception
    {
        final Supplier<Map<String, String>> supplier = mock(Supplier.class);
        final ILoggingEvent event = mock(ILoggingEvent.class);
        try (final SocketReceiver receiver = new SocketReceiver())
        {
            final MdcEnrichingSocketAppender appender = addSocketAppender(receiver, supplier, true);
            appender.addFilter(new Filter<>()
            {
                @Override
                public FilterReply decide(final ILoggingEvent loggingEvent)
                {
                    return FilterReply.DENY;
                }
            });
            appender.doAppend(event);
            appender.stop();
            appender.clearAllFilters();
            appender.doAppend(event);

            verifyNoInteractions(supplier, event);
        }
    }

    private void logWithCaller()
    {
        _logger.info("caller");
    }

    private void logOnProducer(final Runnable log) throws Exception
    {
        final FutureTask<Void> task = new FutureTask<>(() ->
        {
            _loggerContext.getMDCAdapter().setContextMap(PRODUCER_MDC);
            try
            {
                log.run();
                assertEquals(PRODUCER_MDC, _loggerContext.getMDCAdapter().getCopyOfContextMap());
            }
            finally
            {
                _loggerContext.getMDCAdapter().clear();
            }
        }, null);
        final Thread producer = new Thread(task, "producer-thread");
        producer.start();
        try
        {
            task.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        finally
        {
            producer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        }
        assertFalse(producer.isAlive());
    }

    private ListAppender<ILoggingEvent> addRecordingAppender(final boolean prepare)
    {
        final ListAppender<ILoggingEvent> appender = new ListAppender<>()
        {
            @Override
            protected void append(final ILoggingEvent event)
            {
                if (prepare)
                {
                    event.prepareForDeferredProcessing();
                }
                super.append(event);
            }
        };
        appender.setContext(_loggerContext);
        appender.start();
        _logger.addAppender(appender);
        return appender;
    }

    private MdcEnrichingSocketAppender addSocketAppender(final SocketReceiver receiver,
                                                         final Supplier<Map<String, String>> supplier,
                                                         final boolean includeCallerData)
    {
        final MdcEnrichingSocketAppender appender = new MdcEnrichingSocketAppender(supplier)
        {
            @Override
            public PreSerializationTransformer<ILoggingEvent> getPST()
            {
                final PreSerializationTransformer<ILoggingEvent> transformer = super.getPST();
                return event ->
                {
                    try
                    {
                        if (!_serializationAllowed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        {
                            throw new IllegalStateException("Serialization was not released");
                        }
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Serialization interrupted", e);
                    }
                    return transformer.transform(event);
                };
            }
        };
        appender.setContext(_loggerContext);
        appender.setName("socket-" + _appenders.size());
        appender.setRemoteHost(InetAddress.getLoopbackAddress().getHostAddress());
        appender.setPort(receiver._serverSocket.getLocalPort());
        appender.setIncludeCallerData(includeCallerData);
        _appenders.add(appender);
        appender.start();
        assertTrue(appender.isStarted());
        _logger.addAppender(appender);
        return appender;
    }

    private class SocketReceiver implements AutoCloseable
    {
        private final ServerSocket _serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        private Socket _socket;
        private HardenedLoggingEventInputStream _input;

        private SocketReceiver() throws IOException
        {
            _serverSocket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        }

        private LoggingEventVO readEvent() throws IOException, ClassNotFoundException
        {
            if (_socket == null)
            {
                _socket = _serverSocket.accept();
                _socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
                _input = new HardenedLoggingEventInputStream(_loggerContext, _socket.getInputStream());
            }
            return assertInstanceOf(LoggingEventVO.class, _input.readObject());
        }

        @Override
        public void close() throws IOException
        {
            try
            {
                if (_socket != null)
                {
                    _socket.close();
                }
            }
            finally
            {
                _serverSocket.close();
            }
        }
    }
}
