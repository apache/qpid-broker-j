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
package org.apache.qpid.server.transport.websocket.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.WebSocketContainer;
import org.eclipse.jetty.websocket.api.exceptions.ProtocolException;
import org.eclipse.jetty.websocket.common.JettyWebSocketFrameHandler;
import org.eclipse.jetty.websocket.common.JettyWebSocketFrameHandlerFactory;
import org.eclipse.jetty.websocket.core.CloseStatus;
import org.eclipse.jetty.websocket.core.CoreSession;
import org.eclipse.jetty.websocket.core.WebSocketComponents;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.test.utils.UnitTestBase;

class AmqpWebSocketTest extends UnitTestBase
{
    private WebSocketConnectionScheduler _connectionScheduler;
    private AmqpWebSocket _endpoint;
    private WebSocketConnection _connection;
    private JettyWebSocketFrameHandler _frameHandler;
    private CoreSession _coreSession;
    private MultiVersionProtocolEngine _protocolEngine;
    private QpidByteBuffer _inputBuffer;
    private Logger _logger;
    private Level _originalLevel;
    private ListAppender<ILoggingEvent> _appender;

    @BeforeEach
    void beforeEach() throws Exception
    {
        _connectionScheduler = new WebSocketConnectionScheduler(getTestName(), () -> 0L, () -> 0L,
                TimeUnit.SECONDS.toNanos(1L));
        _protocolEngine = mock(MultiVersionProtocolEngine.class);
        final MultiVersionProtocolEngineFactory factory = mock(MultiVersionProtocolEngineFactory.class);
        when(factory.newProtocolEngine(any(SocketAddress.class))).thenReturn(_protocolEngine);
        final WebSocketSettings settings = new WebSocketSettings(1024);
        _endpoint = new AmqpWebSocket(factory, mock(ThreadPool.class), _connectionScheduler, settings, null);
        final WebSocketContainer container = mock(WebSocketContainer.class);
        final JettyWebSocketFrameHandlerFactory handlerFactory =
                new JettyWebSocketFrameHandlerFactory(container, mock(WebSocketComponents.class));
        _frameHandler = new JettyWebSocketFrameHandler(container, _endpoint,
                handlerFactory.getMetadata(_endpoint.getClass()));
        _coreSession = mock(CoreSession.class);
        when(_coreSession.isOutputOpen()).thenReturn(true);
        when(_coreSession.getLocalAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10000));
        when(_coreSession.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10001));
        final Callback openCallback = mock(Callback.class);
        _frameHandler.onOpen(_coreSession, openCallback);
        verify(openCallback).succeeded();
        final WebSocketReceiver receiver = (WebSocketReceiver) getField(_endpoint, "_receiver");
        receiver.dispose();
        _inputBuffer = mock(QpidByteBuffer.class);
        setField(receiver, "_netInputBuffer", _inputBuffer);
        _connection = (WebSocketConnection) getField(_endpoint, "_connection");

        _logger = (Logger) LoggerFactory.getLogger(AmqpWebSocket.class.getPackageName());
        _originalLevel = _logger.getLevel();
        _logger.setLevel(Level.WARN);
        _appender = new ListAppender<>();
        _appender.start();
        _logger.addAppender(_appender);
        clearInvocations(_coreSession, _protocolEngine, _inputBuffer);
    }

    @AfterEach
    void afterEach() throws Exception
    {
        try
        {
            if (_connection != null)
            {
                setField(_endpoint, "_connection", _connection);
                if (!_connection.isClosed())
                {
                    notifyClosed();
                }
            }
            if (_connectionScheduler != null)
            {
                _connectionScheduler.shutdown();
            }
        }
        finally
        {
            if (_logger != null)
            {
                _logger.detachAppender(_appender);
                _appender.stop();
                _logger.setLevel(_originalLevel);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"CLOSE_REQUESTED", "CLOSE_SENT", "FORCE_CLOSING", "CLOSED"})
    void expectedClosedChannelErrorDoesNotWarn(final String state)
    {
        advanceTo(state);

        reportError(new ClosedChannelException());

        assertTrue(_appender.list.isEmpty(), "Expected closure emitted a warning");
        verifyNoInteractions(_coreSession, _protocolEngine, _inputBuffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expectedClosedChannelErrorIsAvailableAtDebug(final boolean asynchronous)
    {
        _logger.setLevel(Level.DEBUG);
        advanceTo("CLOSE_REQUESTED");
        final ClosedChannelException failure = asynchronous ? new AsynchronousCloseException() : new ClosedChannelException();

        reportError(failure);

        assertLog(Level.DEBUG, failure);
        verifyNoInteractions(_coreSession, _protocolEngine, _inputBuffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closedChannelAfterProtocolCloseDoesNotWarn(final boolean debugEnabled)
    {
        when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        _logger.setLevel(debugEnabled ? Level.DEBUG : Level.WARN);
        final ClosedChannelException failure = new ClosedChannelException();

        reportError(failure);

        if (debugEnabled)
        {
            assertLog(Level.DEBUG, failure);
        }
        else
        {
            assertTrue(_appender.list.isEmpty());
        }
        assertTrue(_connection.isOpen(), "Protocol close does not require transport close to have started");
        verify(_protocolEngine, never()).closed();
        verifyNoInteractions(_coreSession, _inputBuffer);
    }

    @Test
    void closedChannelErrorOnOpenBrokerConnectionWarns()
    {
        when(_coreSession.isOutputOpen()).thenReturn(false);
        final ClosedChannelException failure = new ClosedChannelException();

        reportError(failure);

        assertLog(Level.WARN, failure);
        assertTrue(_connection.isOpen());
        verify(_protocolEngine, never()).closed();
        verify(_inputBuffer, never()).dispose();
    }

    @Test
    void closedChannelErrorBeforeConnectionInitializationWarns() throws Exception
    {
        setField(_endpoint, "_connection", null);
        final ClosedChannelException failure = new ClosedChannelException();

        reportError(failure);

        assertLog(Level.WARN, failure);
        verifyNoInteractions(_coreSession, _protocolEngine, _inputBuffer);
    }

    @ParameterizedTest
    @MethodSource("unexpectedErrors")
    void unexpectedErrorWarnsRegardlessOfCloseState(final Throwable failure, final String state)
    {
        if ("PROTOCOL_CLOSED".equals(state))
        {
            when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
            clearInvocations(_protocolEngine);
        }
        else if ("CLOSED".equals(state))
        {
            advanceTo("CLOSED");
        }

        reportError(failure);

        assertLog(Level.WARN, failure);
        verifyNoInteractions(_coreSession, _protocolEngine, _inputBuffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void errorNotificationDoesNotWaitForProtocolProcessing(final boolean protocolCloseComplete)
            throws Exception
    {
        if (protocolCloseComplete)
        {
            when(_protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        }
        else
        {
            advanceTo("CLOSE_REQUESTED");
        }
        _connection.lockProtocol();
        final FutureTask<Void> notification = new FutureTask<>(() ->
        {
            reportError(new ClosedChannelException());
            return null;
        });
        final Thread worker = new Thread(notification, getTestName());
        worker.setDaemon(true);
        try
        {
            worker.start();
            notification.get(5L, TimeUnit.SECONDS);
            verify(_protocolEngine, never()).closed();
            verifyNoInteractions(_inputBuffer);
        }
        finally
        {
            _connection.unlockProtocol();
            notification.get(5L, TimeUnit.SECONDS);
        }
        assertTrue(_appender.list.isEmpty());
    }

    @Test
    void closeAfterErrorStillCompletesDeferredCleanup()
    {
        advanceTo("CLOSE_REQUESTED");
        reportError(new ClosedChannelException());
        verifyNoInteractions(_protocolEngine, _inputBuffer);

        notifyClosed();
        verifyNoInteractions(_protocolEngine, _inputBuffer);
        _connectionScheduler.shutdown();

        verify(_protocolEngine).closed();
        verify(_inputBuffer).dispose();
        assertTrue(_connection.isClosed());
        assertTrue(_appender.list.isEmpty());
    }

    private static Stream<Arguments> unexpectedErrors()
    {
        return Stream.of("OPEN", "CLOSED", "PROTOCOL_CLOSED").flatMap(state -> Stream.of(
                new IOException("Unexpected I/O failure"), new IllegalStateException("Unexpected application failure"),
                new IllegalStateException("Wrapped failure", new ClosedChannelException()),
                new ProtocolException("Unexpected protocol failure")).map(failure -> Arguments.of(failure, state)));
    }

    private void advanceTo(final String state)
    {
        _connection.close();
        switch (state)
        {
            case "CLOSE_REQUESTED":
                break;
            case "CLOSE_SENT":
                assertTrue(_connection.commitWebSocketClose());
                break;
            case "FORCE_CLOSING":
                _connection.processClose(TimeUnit.SECONDS.toNanos(1L));
                assertTrue(_connection.isForceClosing());
                break;
            case "CLOSED":
                notifyClosed();
                assertTrue(_connection.isClosed());
                break;
            default:
                throw new IllegalArgumentException("Unexpected test state: " + state);
        }
        _appender.list.clear();
        clearInvocations(_coreSession, _protocolEngine, _inputBuffer);
    }

    private void reportError(final Throwable failure)
    {
        final Callback callback = mock(Callback.class);
        _frameHandler.onError(failure, callback);
        verify(callback).succeeded();
        verify(callback, never()).failed(any(Throwable.class));
    }

    private void notifyClosed()
    {
        final Callback callback = mock(Callback.class);
        _frameHandler.onClosed(new CloseStatus(CloseStatus.NORMAL), callback);
        verify(callback).succeeded();
    }

    private void assertLog(final Level level, final Throwable failure)
    {
        assertEquals(1, _appender.list.size());
        final ILoggingEvent event = _appender.list.get(0);
        assertEquals(level, event.getLevel());
        final ThrowableProxy proxy = assertInstanceOf(ThrowableProxy.class, event.getThrowableProxy());
        assertSame(failure, proxy.getThrowable());
    }

    private static Object getField(final Object target, final String name) throws ReflectiveOperationException
    {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(final Object target, final String name, final Object value)
            throws ReflectiveOperationException
    {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
