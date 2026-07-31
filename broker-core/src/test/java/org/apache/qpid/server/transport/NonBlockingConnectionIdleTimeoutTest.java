/*
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
 */

package org.apache.qpid.server.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionIdleTimeoutTest extends UnitTestBase
{
    private ProtocolEngine _protocolEngine;
    private AggregateTicker _ticker;
    private NonBlockingConnection _connection;

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

        _ticker = mock(AggregateTicker.class);
        _protocolEngine = mock(ProtocolEngine.class);
        when(_protocolEngine.getAggregateTicker()).thenReturn(_ticker);
        when(_protocolEngine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());

        final NetworkConnectionScheduler scheduler = mock(NetworkConnectionScheduler.class);
        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getNetworkBufferSize()).thenReturn(1024);
        when(port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(100);
        when(port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(100L);
        when(port.getContextValue(Integer.class, AmqpPort.MAX_GATHERING_WRITE_BUFFERS)).thenReturn(1024);
        final EventLogger eventLogger = mock(EventLogger.class);
        final Broker<?> broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(eventLogger);
        doReturn(broker).when(port).getParent();

        _connection = new NonBlockingConnection(socketChannel, _protocolEngine, Set.of(TransportEncryption.NONE),
                () -> { }, scheduler, port);
        final SelectorThread.SelectionTask _selectionTask = mock(SelectorThread.SelectionTask.class);
        _connection.setSelectionTask(_selectionTask);
    }

    @AfterEach
    void afterEach()
    {
        _connection.close();
        _connection.doWork();
    }

    @Test
    void immediateRerunUsesCurrentTimeForTicker()
    {
        final List<Long> observedScheduledTimes = new ArrayList<>();
        when(_ticker.getTimeToNextTick(anyLong())).thenAnswer(invocation ->
        {
            final long scheduledTime = _connection.getScheduledTime();
            observedScheduledTimes.add(scheduledTime);
            return scheduledTime > 0 ? 1 : 0;
        });
        when(_ticker.tick(anyLong())).thenReturn(Integer.MAX_VALUE);

        assertTrue(_connection.setScheduled(), "Connection should be scheduled");
        final long initialScheduledTime = _connection.getScheduledTime();

        _connection.doWork();

        assertEquals(initialScheduledTime, observedScheduledTimes.get(0),
                "First ticker evaluation should see the original scheduled time");
        assertEquals(0, _connection.getScheduledTime(),
                "Scheduled time should be cleared after the first ticker evaluation");
        verify(_ticker, never()).tick(anyLong());

        _connection.doWork();

        assertEquals(0, observedScheduledTimes.get(1), "Immediate rerun should evaluate the ticker against current time");
        verify(_ticker, times(1)).tick(anyLong());
    }

    @Test
    void tickerCloseSkipsOrdinaryIoAndRunsShutdown() throws Exception
    {
        final NonBlockingConnectionDelegate delegate = mock(NonBlockingConnectionDelegate.class);
        when(delegate.doWrite(any())).thenReturn(new NonBlockingConnectionDelegate.WriteResult(true, 0));
        injectDelegate(delegate);
        when(_ticker.getTimeToNextTick(anyLong())).thenReturn(0);
        doAnswer(invocation ->
        {
            _connection.close();
            return Integer.MAX_VALUE;
        }).when(_ticker).tick(anyLong());
        assertTrue(_connection.setScheduled(), "Connection should be scheduled");

        _connection.doWork();

        verify(_protocolEngine, never()).processPendingIterator();
        verify(_protocolEngine, never()).setTransportBlockedForWriting(anyBoolean());
        verify(delegate, never()).readyForRead();
        verify(delegate, times(1)).doWrite(any());
        verify(_protocolEngine, times(1)).closed();
        verify(delegate, times(1)).shutdownInput();
        verify(delegate, times(1)).shutdownOutput();
    }

    @Test
    void blockedPendingOutputDoesNotStarveReadIdleTicker() throws Exception
    {
        final int readDelay = 1000;
        final long lastReadTime = System.currentTimeMillis() - 2 * readDelay;
        final long timeoutTime = lastReadTime + readDelay;
        final AtomicBoolean readerIdle = new AtomicBoolean();
        final AggregateTicker aggregateTicker = new AggregateTicker();
        when(_protocolEngine.getAggregateTicker()).thenReturn(aggregateTicker);
        when(_protocolEngine.getLastReadTime()).thenReturn(lastReadTime);
        doAnswer(invocation ->
        {
            readerIdle.set(true);
            _connection.close();
            return null;
        }).when(_protocolEngine).readerIdle();
        aggregateTicker.addTicker(new ServerIdleReadTimeoutTicker(_connection, _protocolEngine, readDelay));

        final Iterator<Runnable> pendingIterator = new Iterator<>()
        {
            private boolean _outputQueued;

            @Override
            public boolean hasNext()
            {
                return true;
            }

            @Override
            public Runnable next()
            {
                if (!_outputQueued)
                {
                    _outputQueued = true;
                    return () -> _connection.send(QpidByteBuffer.wrap(new byte[1024]));
                }
                return () -> { };
            }
        };
        when(_protocolEngine.processPendingIterator()).thenReturn(pendingIterator);

        final NonBlockingConnectionDelegate delegate = mock(NonBlockingConnectionDelegate.class);
        when(delegate.doWrite(any())).thenAnswer(invocation ->
        {
            if (!readerIdle.get())
            {
                return new NonBlockingConnectionDelegate.WriteResult(false, 0);
            }

            long bytesConsumed = 0;
            final Collection<QpidByteBuffer> buffers = invocation.getArgument(0);
            for (final QpidByteBuffer buffer : buffers)
            {
                bytesConsumed += buffer.remaining();
                buffer.position(buffer.limit());
            }
            return new NonBlockingConnectionDelegate.WriteResult(true, bytesConsumed);
        });
        injectDelegate(delegate);

        assertTrue(_connection.setScheduled(), "Connection should be scheduled");
        injectScheduledTime(timeoutTime - 1);

        _connection.doWork();

        assertEquals(0, _connection.getScheduledTime(), "Scheduled time should be cleared while pending output remains blocked");
        verify(_protocolEngine, never()).readerIdle();
        verify(delegate, times(2)).doWrite(any());

        _connection.doWork();

        verify(_protocolEngine, times(1)).readerIdle();
        verify(_protocolEngine, times(1)).processPendingIterator();
        verify(delegate, times(3)).doWrite(any());
        verify(_protocolEngine, times(1)).closed();
    }

    private void injectDelegate(final NonBlockingConnectionDelegate delegate) throws Exception
    {
        final Field delegateField = NonBlockingConnection.class.getDeclaredField("_delegate");
        delegateField.setAccessible(true);
        delegateField.set(_connection, delegate);
    }

    private void injectScheduledTime(final long scheduledTime) throws Exception
    {
        final Field scheduledTimeField = NonBlockingConnection.class.getDeclaredField("_scheduledTime");
        scheduledTimeField.setAccessible(true);
        scheduledTimeField.setLong(_connection, scheduledTime);
    }
}
