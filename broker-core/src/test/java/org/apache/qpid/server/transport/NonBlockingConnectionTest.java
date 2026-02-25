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

package org.apache.qpid.server.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;

class NonBlockingConnectionTest
{
    private static final TestAppender appender = new TestAppender(NonBlockingConnection.class);
    private static final ch.qos.logback.classic.Logger ROOT_LOGGER =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    private final SelectorThread.SelectionTask selectionTask = mock(SelectorThread.SelectionTask.class);

    private NonBlockingConnection _nonBlockingConnection;

    @BeforeAll
    static void beforeAll()
    {
        ROOT_LOGGER.addAppender(appender);
    }

    @AfterAll
    static void afterAll()
    {
        ROOT_LOGGER.detachAppender(appender.getName());
    }

    @BeforeEach
    void beforeEach()
    {
        final SocketAddress localAddress = mock(SocketAddress.class);
        when(localAddress.toString()).thenReturn("127.0.0.1:5672");
        final Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("localhost", 1000));
        when(socket.getLocalSocketAddress()).thenReturn(localAddress);
        final SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.socket()).thenReturn(socket);
        final ProtocolEngine protocolEngine = mock(ProtocolEngine.class);
        final NetworkConnectionScheduler scheduler = mock(NetworkConnectionScheduler.class);
        final AmqpPort<?> port = mock(AmqpPort.class);
        final EventLogger eventLogger = mock(EventLogger.class);
        final Broker broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(eventLogger);
        when(port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(100);
        when(port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(100L);
        when(port.getParent()).thenReturn(broker);
        final Set<TransportEncryption> encryptionSet = Set.of(TransportEncryption.NONE);

        _nonBlockingConnection =
                new NonBlockingConnection(socketChannel, protocolEngine, encryptionSet, () -> {}, scheduler, port);

        appender.clear();
    }

    /** Delegate always returns WriteResult containing complete = false, causing an infinite loop in
     * NonBlockingConnection#shutdownFinalWrite(), which should be handled by the timeout handling */
    @Test
    void shutdownFinalWriteLooping() throws Exception
    {
        // construct delegate mock returning WriteResult[complete=false]
        final NonBlockingConnectionPlainDelegate delegate = mock(NonBlockingConnectionPlainDelegate.class);
        when(delegate.doWrite(any())).thenReturn(new NonBlockingConnectionDelegate.WriteResult(false, 0L));
        injectDelegate(delegate);

        // close the connection
        _nonBlockingConnection.setSelectionTask(selectionTask);
        _nonBlockingConnection.close();
        _nonBlockingConnection.doWork();

        // there should be only 2 log messages
        assertEquals(2, appender.getEvents().size());

        // first log message states that connection will be closed
        final ILoggingEvent firstLogEntry = appender.getEvents().get(0);
        assertEquals(Level.DEBUG, firstLogEntry.getLevel());
        assertEquals("Closing localhost/127.0.0.1:1000", firstLogEntry.getMessage());

        // second log message informs about timeout which happened during shutdownFinalWrite()
        final ILoggingEvent secondLogEntry = appender.getEvents().get(1);
        assertEquals(Level.INFO, secondLogEntry.getLevel());
        assertEquals("Exception performing final write/close for '{}': {}", secondLogEntry.getMessage());
        assertEquals("localhost/127.0.0.1:1000", secondLogEntry.getArgumentArray()[0]);
        assertTrue(String.valueOf(secondLogEntry.getArgumentArray()[1]).startsWith("Failed to perform final write to connection after"));
    }

    /** Delegate immediately returns WriteResult containing complete = true, no timeout handling involved */
    @Test
    void shutdownFinalWriteWithoutLooping() throws Exception
    {
        // construct delegate mock returning WriteResult[complete=true]
        final NonBlockingConnectionPlainDelegate delegate = mock(NonBlockingConnectionPlainDelegate.class);
        when(delegate.doWrite(any())).thenReturn(new NonBlockingConnectionDelegate.WriteResult(true, 0L));
        injectDelegate(delegate);

        // close the connection
        _nonBlockingConnection.setSelectionTask(selectionTask);
        _nonBlockingConnection.close();
        _nonBlockingConnection.doWork();

        // there should be only 1 log message (no timeout)
        assertEquals(1, appender.getEvents().size());

        // first log message states that connection will be closed
        final ILoggingEvent firstLogEntry = appender.getEvents().get(0);
        assertEquals(Level.DEBUG, firstLogEntry.getLevel());
        assertEquals("Closing localhost/127.0.0.1:1000", firstLogEntry.getMessage());
    }

    /** Inject delegate using reflection */
    private void injectDelegate(final NonBlockingConnectionPlainDelegate delegate) throws Exception
    {
        final Field delegateField = NonBlockingConnection.class.getDeclaredField("_delegate");
        delegateField.setAccessible(true);
        delegateField.set(_nonBlockingConnection, delegate);
    }

    /** Logging Appender string list of logging events */
    static class TestAppender implements Appender<ILoggingEvent>
    {
        private final String className;
        private final List<ILoggingEvent> _events = new ArrayList<>();

        TestAppender(final Class<?> clazz)
        {
            className = clazz.getCanonicalName();
        }

        @Override
        public String getName()
        {
            return getClass().getSimpleName();
        }

        @Override
        public void doAppend(final ILoggingEvent iLoggingEvent) throws LogbackException
        {
            if (className.equals(iLoggingEvent.getLoggerName()))
            {
                _events.add(iLoggingEvent);
            }
        }

        @Override
        public void setName(final String s)
        {

        }

        @Override
        public void setContext(final Context context)
        {

        }

        @Override
        public Context getContext()
        {
            return null;
        }

        @Override
        public void addStatus(final Status status)
        {

        }

        @Override
        public void addInfo(final String s)
        {

        }

        @Override
        public void addInfo(final String s, final Throwable throwable)
        {

        }

        @Override
        public void addWarn(final String s)
        {

        }

        @Override
        public void addWarn(final String s, final Throwable throwable)
        {

        }

        @Override
        public void addError(final String s)
        {

        }

        @Override
        public void addError(final String s, final Throwable throwable)
        {

        }

        @Override
        public void addFilter(final Filter<ILoggingEvent> filter)
        {

        }

        @Override
        public void clearAllFilters()
        {

        }

        @Override
        public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList()
        {
            return List.of();
        }

        @Override
        public FilterReply getFilterChainDecision(final ILoggingEvent iLoggingEvent)
        {
            return null;
        }

        @Override
        public void start()
        {

        }

        @Override
        public void stop()
        {

        }

        @Override
        public boolean isStarted()
        {
            return true;
        }

        public List<ILoggingEvent> getEvents()
        {
            return _events;
        }

        public void clear()
        {
            _events.clear();
        }
    }
}
