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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionWriteTest extends UnitTestBase
{
    private static final int NETWORK_BUFFER_SIZE = 1024;

    private final AtomicBoolean _hasWork = new AtomicBoolean();
    private final AtomicBoolean _blocked = new AtomicBoolean();
    private final AtomicInteger _writableBytes = new AtomicInteger();
    private final List<Boolean> _blockedTransitions = new ArrayList<>();
    private ProtocolEngine _engine;
    private SocketChannel _channel;
    private NonBlockingConnection _connection;

    @BeforeEach
    void setUp() throws Exception
    {
        _hasWork.set(false);
        _blocked.set(false);
        _writableBytes.set(0);
        _blockedTransitions.clear();
        final Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("localhost", 1000));
        when(socket.getLocalSocketAddress()).thenReturn(new InetSocketAddress("localhost", 5672));
        _channel = mock(SocketChannel.class);
        when(_channel.socket()).thenReturn(socket);
        when(_channel.write(any(ByteBuffer[].class))).thenAnswer(invocation ->
        {
            final ByteBuffer[] buffers = invocation.getArgument(0);
            final int offset = 0;
            final int length = buffers.length;
            long written = 0;
            for (int i = offset; i < offset + length; i++)
            {
                final int size = Math.min(buffers[i].remaining(), _writableBytes.get());
                buffers[i].position(buffers[i].position() + size);
                _writableBytes.addAndGet(-size);
                written += size;
            }
            return written;
        });

        _engine = mock(ProtocolEngine.class);
        final AggregateTicker ticker = mock(AggregateTicker.class);
        when(ticker.getTimeToNextTick(anyLong())).thenReturn(Integer.MAX_VALUE);
        when(_engine.getAggregateTicker()).thenReturn(ticker);
        when(_engine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());
        when(_engine.hasWork()).thenAnswer(invocation -> _hasWork.get());
        when(_engine.isTransportBlockedForWriting()).thenAnswer(invocation -> _blocked.get());
        doAnswer(invocation ->
        {
            _hasWork.set(false);
            return null;
        }).when(_engine).clearWork();
        doAnswer(invocation ->
        {
            _hasWork.set(true);
            return null;
        }).when(_engine).notifyWork();
        doAnswer(invocation ->
        {
            final boolean blocked = invocation.getArgument(0);
            if (_blocked.getAndSet(blocked) != blocked)
            {
                _blockedTransitions.add(blocked);
                if (!blocked)
                {
                    _engine.notifyWork();
                }
            }
            return null;
        }).when(_engine).setTransportBlockedForWriting(anyBoolean());

        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getNetworkBufferSize()).thenReturn(NETWORK_BUFFER_SIZE);
        when(port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(1);
        when(port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(0L);
        when(port.getContextValue(Integer.class, AmqpPort.MAX_GATHERING_WRITE_BUFFERS)).thenReturn(1024);
        final Broker<?> broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        doReturn(broker).when(port).getParent();
        _connection = new NonBlockingConnection(_channel, _engine, Set.of(TransportEncryption.NONE),
                () -> { }, mock(NetworkConnectionScheduler.class), port);
        _connection.setSelectionTask(mock(SelectorThread.SelectionTask.class));
    }

    @AfterEach
    void tearDown()
    {
        _writableBytes.set(Integer.MAX_VALUE);
        _connection.close();
        _connection.doWork();
    }

    @Test
    void blockedOutputWaitsForSocketReadiness()
    {
        send(128);

        assertFalse(_connection.doWork());

        assertTrue(_connection.wantsWrite());
        assertTrue(_connection.wantsRead(), "Completed protocol work must allow incoming traffic during a blocked write");
        assertFalse(_connection.isStateChanged(), "Pending output alone must not reschedule the connection");
        assertEquals(List.of(true), _blockedTransitions);

        _engine.notifyWork();
        _connection.doWork();
        assertEquals(List.of(true), _blockedTransitions, "A failed retry must not briefly resume consumers");
        assertFalse(_connection.isStateChanged());

        _writableBytes.set(32);
        _connection.doWork();
        assertTrue(_connection.wantsWrite(), "Partial progress must retain write interest");
        assertEquals(List.of(true), _blockedTransitions);
        assertFalse(_connection.isStateChanged());

        _writableBytes.set(96);
        _connection.doWork();
        assertEquals(List.of(true, false), _blockedTransitions);
        assertTrue(_connection.isStateChanged(), "Unblocking must resume pending protocol output");
        assertTrue(_connection.wantsRead());
        assertFalse(_connection.wantsWrite());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, NETWORK_BUFFER_SIZE / 4})
    void pendingIteratorIsRescheduledWhenSecondWriteDrainsOutput(final int firstWriteBytes) throws Exception
    {
        final AtomicInteger completed = new AtomicInteger();
        final List<Runnable> tasks = List.of(() -> send(2 * NETWORK_BUFFER_SIZE), completed::incrementAndGet);
        when(_engine.processPendingIterator()).thenReturn(tasks.iterator());
        final AtomicInteger writeCalls = new AtomicInteger();
        doAnswer(invocation ->
        {
            final ByteBuffer[] buffers = invocation.getArgument(0);
            int writableBytes = writeCalls.getAndIncrement() == 0 ? firstWriteBytes : Integer.MAX_VALUE;
            long written = 0;
            for (final ByteBuffer buffer : buffers)
            {
                final int size = Math.min(buffer.remaining(), writableBytes);
                buffer.position(buffer.position() + size);
                writableBytes -= size;
                written += size;
            }
            return written;
        }).when(_channel).write(any(ByteBuffer[].class));

        assertFalse(_connection.doWork());

        assertEquals(2, writeCalls.get(), "The second write must drain output after the iterator is suspended");
        assertEquals(0, completed.get(), "The iterator still has runnable work after the second write");
        assertTrue(_connection.wantsRead());
        assertFalse(_connection.wantsWrite());
        assertFalse(_connection.isPartialRead());
        assertFalse(_blocked.get());
        assertTrue(_blockedTransitions.isEmpty(), "There is no backpressure transition to supply a wake-up");
        assertTrue(_connection.isStateChanged(),
                "Pending protocol work must be rescheduled when output drains in the same pass");

        assertFalse(_connection.doWork());
        assertEquals(1, completed.get(), "The retained iterator must resume exactly once");
        assertFalse(_connection.isStateChanged(), "An exhausted iterator must not keep rescheduling");
        verify(_engine).processPendingIterator();
    }

    @Test
    void pendingIteratorResumesAfterOutputDrains()
    {
        final AtomicInteger completed = new AtomicInteger();
        final List<Runnable> tasks = List.of(() -> send(NETWORK_BUFFER_SIZE), completed::incrementAndGet);
        when(_engine.processPendingIterator()).thenReturn(tasks.iterator());

        _connection.doWork();

        assertEquals(0, completed.get());
        assertTrue(_connection.wantsWrite());
        assertFalse(_connection.wantsRead(), "Suspended protocol work must continue to apply read backpressure");
        assertFalse(_connection.isStateChanged(), "A suspended iterator must wait for its buffered output to drain");

        _writableBytes.set(NETWORK_BUFFER_SIZE);
        _connection.doWork();

        assertEquals(1, completed.get(), "Suspended work must resume exactly once");
        assertFalse(_connection.wantsWrite());
    }

    @ParameterizedTest
    @CsvSource({"false, 768", "true, 768", "false, 1023", "true, 1023"})
    void partialWriteBelowBufferLimitResumesWorkAndReads(final boolean pendingTask,
                                                        final int remainingOutput) throws Exception
    {
        final AtomicInteger completed = new AtomicInteger();
        final AtomicInteger received = new AtomicInteger();
        final List<Runnable> tasks = new ArrayList<>();
        tasks.add(() -> send(remainingOutput + NETWORK_BUFFER_SIZE / 4));
        if (pendingTask)
        {
            tasks.add(completed::incrementAndGet);
        }
        when(_engine.processPendingIterator()).thenReturn(tasks.iterator());
        _writableBytes.set(NETWORK_BUFFER_SIZE / 4);
        when(_channel.read(any(ByteBuffer.class))).thenAnswer(invocation ->
        {
            final ByteBuffer input = invocation.getArgument(0);
            input.put((byte) 42);
            return 1;
        }).thenReturn(0);
        doAnswer(invocation ->
        {
            final QpidByteBuffer input = invocation.getArgument(0);
            assertEquals(42, input.get());
            received.incrementAndGet();
            return null;
        }).when(_engine).received(any());

        _connection.doWork();

        assertEquals(0, completed.get());
        assertEquals(0, received.get());
        assertEquals(0, _writableBytes.get());
        assertFalse(_connection.isPartialRead());
        assertTrue(_connection.isStateChanged(), "Freed buffer space must schedule a continuation");
        assertTrue(_connection.wantsWrite(), "The partial write must leave output pending");
        assertEquals(List.of(true), _blockedTransitions);

        _connection.doWork();

        assertEquals(pendingTask ? 1 : 0, completed.get());
        assertEquals(1, received.get(), "Incoming data must be read without further write capacity");
        assertTrue(_connection.wantsRead(), "Completing the iterator must restore read interest");
        assertTrue(_connection.wantsWrite());
        assertTrue(_connection.isPartialRead());
        assertFalse(_connection.isStateChanged());
        assertEquals(List.of(true), _blockedTransitions, "Consumers must remain blocked until output drains");

        _connection.doWork();

        assertEquals(pendingTask ? 1 : 0, completed.get(), "Pending work must run exactly once");
        assertEquals(1, received.get());
        assertTrue(_connection.wantsRead());
        assertTrue(_connection.wantsWrite());
        assertFalse(_connection.isPartialRead());
        assertFalse(_connection.isStateChanged(), "The connection must return to selector waiting");
        assertEquals(List.of(true), _blockedTransitions);
    }

    @ParameterizedTest
    @CsvSource({"false, 1024", "true, 1024", "false, 1025", "true, 1025"})
    void partialWriteAtOrAboveBufferLimitWaitsForSocketReadiness(final boolean pendingTask,
                                                                 final int remainingOutput) throws Exception
    {
        final AtomicInteger completed = new AtomicInteger();
        final List<Runnable> tasks = new ArrayList<>();
        tasks.add(() -> send(remainingOutput + NETWORK_BUFFER_SIZE / 4));
        if (pendingTask)
        {
            tasks.add(completed::incrementAndGet);
        }
        when(_engine.processPendingIterator()).thenReturn(tasks.iterator());
        _writableBytes.set(NETWORK_BUFFER_SIZE / 4);

        _connection.doWork();

        assertEquals(0, _writableBytes.get());
        assertEquals(0, completed.get());
        assertFalse(_connection.isStateChanged(), "A full buffer must wait for socket write readiness");
        assertFalse(_connection.isPartialRead());
        assertFalse(_connection.wantsRead());
        assertTrue(_connection.wantsWrite());

        _engine.notifyWork();
        _connection.doWork();

        assertEquals(0, completed.get());
        assertFalse(_connection.isStateChanged(), "Retrying with no write progress must not cause a busy loop");
        assertFalse(_connection.isPartialRead());
        assertFalse(_connection.wantsRead());
        assertTrue(_connection.wantsWrite());
        assertEquals(List.of(true), _blockedTransitions);
        verify(_channel, never()).read(any(ByteBuffer.class));
    }

    @Test
    void resumedIteratorWaitsAgainWhenOutputReachesBufferLimit()
    {
        final AtomicInteger completed = new AtomicInteger();
        final List<Runnable> tasks = List.of(() -> send(NETWORK_BUFFER_SIZE),
                () -> send(NETWORK_BUFFER_SIZE / 4), completed::incrementAndGet);
        when(_engine.processPendingIterator()).thenReturn(tasks.iterator());
        _writableBytes.set(NETWORK_BUFFER_SIZE / 4);

        _connection.doWork();

        assertTrue(_connection.isStateChanged(), "The partial write must allow pending work to resume");

        _connection.doWork();

        assertEquals(0, completed.get(), "Refilling the buffer must suspend the remaining task");
        assertFalse(_connection.isStateChanged());
        assertFalse(_connection.isPartialRead());
        assertFalse(_connection.wantsRead());
        assertTrue(_connection.wantsWrite());
        assertEquals(List.of(true), _blockedTransitions);

        _writableBytes.set(NETWORK_BUFFER_SIZE);
        _connection.doWork();

        assertEquals(1, completed.get(), "Write readiness must eventually resume the remaining task");
        assertFalse(_connection.wantsWrite());
        assertEquals(List.of(true, false), _blockedTransitions);
    }

    @Test
    void blockedPendingOutputClearsPreviousReadIndication() throws Exception
    {
        when(_channel.read(any(ByteBuffer.class))).thenReturn(1);
        _connection.doWork();
        assertTrue(_connection.isPartialRead());
        final List<Runnable> tasks = List.of(() -> send(NETWORK_BUFFER_SIZE), () -> { });
        when(_engine.processPendingIterator()).thenReturn(tasks.iterator());

        _connection.doWork();

        assertTrue(_connection.wantsWrite());
        assertFalse(_connection.isPartialRead(), "Skipping reads must not reuse the preceding pass's read indication");
        assertFalse(_connection.isStateChanged());
    }

    @Test
    void explicitProtocolWorkRunsWhileOutputIsBlocked()
    {
        send(128);
        _connection.doWork();
        final AtomicInteger completed = new AtomicInteger();
        when(_engine.processPendingIterator()).thenReturn(List.<Runnable>of(completed::incrementAndGet).iterator());
        _engine.notifyWork();

        _connection.doWork();

        assertEquals(1, completed.get());
        assertEquals(List.of(true), _blockedTransitions);
        assertFalse(_connection.isStateChanged());
    }

    @Test
    void closeDeadlineStillRunsWithBlockedOutput() throws Exception
    {
        send(NETWORK_BUFFER_SIZE);
        _connection.doWork();
        final AggregateTicker ticker = _engine.getAggregateTicker();
        when(ticker.getTimeToNextTick(anyLong())).thenReturn(0);
        when(ticker.tick(anyLong())).thenAnswer(invocation ->
        {
            _connection.close();
            return Integer.MAX_VALUE;
        });

        assertTrue(_connection.doWork());

        verify(_engine).closed();
        verify(_channel).close();
    }

    @Test
    void pendingTlsWrapDoesNotSelectForReading() throws Exception
    {
        final NonBlockingConnectionDelegate delegate = mock(NonBlockingConnectionDelegate.class);
        when(delegate.readyForRead()).thenReturn(false);
        when(delegate.doWrite(any())).thenReturn(new NonBlockingConnectionDelegate.WriteResult(false, 0));
        final Field field = NonBlockingConnection.class.getDeclaredField("_delegate");
        field.setAccessible(true);
        ((NonBlockingConnectionDelegate) field.get(_connection)).shutdownInput();
        field.set(_connection, delegate);

        send(128);
        _connection.doWork();

        assertTrue(_connection.wantsWrite());
        assertFalse(_connection.wantsRead(), "TLS NEED_WRAP cannot make progress from read readiness");
        assertFalse(_connection.isStateChanged(), "Pending TLS output must not reschedule the connection");

        when(delegate.readyForRead()).thenReturn(true);
        assertTrue(_connection.wantsRead(), "Read interest must return when TLS can process incoming data");
    }

    @Test
    void pendingOutputCanWaitForTlsInput() throws Exception
    {
        final AtomicBoolean inputProcessed = new AtomicBoolean();
        final AtomicInteger completed = new AtomicInteger();
        final NonBlockingConnectionDelegate delegate = mock(NonBlockingConnectionDelegate.class);
        when(delegate.needsReadForWrite()).thenAnswer(invocation -> !inputProcessed.get());
        when(delegate.readyForRead()).thenReturn(true);
        when(delegate.processData()).thenAnswer(invocation ->
        {
            inputProcessed.set(true);
            return false;
        });
        when(delegate.doWrite(any())).thenAnswer(invocation ->
        {
            long consumed = 0;
            if (inputProcessed.get())
            {
                final Collection<QpidByteBuffer> buffers = invocation.getArgument(0);
                for (final QpidByteBuffer buffer : buffers)
                {
                    consumed += buffer.remaining();
                    buffer.position(buffer.limit());
                }
            }
            return new NonBlockingConnectionDelegate.WriteResult(inputProcessed.get(), consumed);
        });
        final Field field = NonBlockingConnection.class.getDeclaredField("_delegate");
        field.setAccessible(true);
        ((NonBlockingConnectionDelegate) field.get(_connection)).shutdownInput();
        field.set(_connection, delegate);
        try (QpidByteBuffer input = QpidByteBuffer.allocate(NETWORK_BUFFER_SIZE))
        {
            when(delegate.getNetInputBuffer()).thenReturn(input);
            final List<Runnable> tasks = List.of(() -> send(NETWORK_BUFFER_SIZE), completed::incrementAndGet);
            when(_engine.processPendingIterator()).thenReturn(tasks.iterator());

            _connection.doWork();

            assertTrue(_connection.wantsRead(), "TLS must read the peer's response with a suspended iterator");
            assertFalse(_connection.wantsWrite(), "Writable socket readiness cannot satisfy a TLS input wait");
            assertFalse(_connection.isStateChanged());
            assertEquals(0, completed.get());

            when(_channel.read(any(ByteBuffer.class))).thenReturn(1, 0);
            _connection.doWork();
            assertTrue(inputProcessed.get());
            _connection.doWork();

            assertEquals(1, completed.get(), "The TLS input wait must not strand pending protocol work");
            assertFalse(_connection.wantsWrite());
            assertEquals(List.of(true, false), _blockedTransitions);
        }
    }

    private void send(final int size)
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[size]))
        {
            _connection.send(buffer);
        }
    }
}
