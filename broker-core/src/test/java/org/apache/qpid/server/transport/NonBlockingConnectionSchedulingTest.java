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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionSchedulingTest extends UnitTestBase
{
    private static final int TIMEOUT_MILLIS = 10000;

    private final List<Thread> _threads = new CopyOnWriteArrayList<>();
    private final List<TestConnection> _connections = new ArrayList<>();
    private NetworkConnectionScheduler _scheduler;

    @ParameterizedTest
    @ValueSource(ints = {2, 4})
    void blockedSocketReturnsWorkersAndRecoversPendingOutput(final int workers) throws Exception
    {
        startScheduler(workers);
        final TestConnection blocked = createConnection(1024);
        final TestConnection independent = createConnection(1024);
        final byte[] payload = new byte[2 * 1024 * 1024];
        Arrays.fill(payload, (byte) 42);
        final byte[] marker = new byte[]{1, 2, 3};
        for (int i = 0; i < 3; i++)
        {
            blocked.submit(List.of(() -> blocked.send(payload), () -> blocked.send(marker)));
            blocked.awaitWriteInterest();

            independent.submit(List.of(() -> independent.send(marker)));
            assertArrayEquals(marker, independent._client.getInputStream().readNBytes(marker.length),
                    "An independent connection must progress while the first socket remains blocked");

            assertArrayEquals(payload, blocked._client.getInputStream().readNBytes(payload.length));
            assertArrayEquals(marker, blocked._client.getInputStream().readNBytes(marker.length),
                    "The suspended protocol iterator must resume after socket capacity becomes available");
        }
    }

    @Test
    void incomingHeartbeatsPreventReadIdleTimeoutWhileOutputIsBlocked() throws Exception
    {
        startScheduler(2);
        final TestConnection blocked = createConnection(4 * 1024 * 1024);
        blocked.submit(List.of(() -> blocked.send(new byte[2 * 1024 * 1024])));
        blocked.awaitWriteInterest();

        final int readIdleMillis = 2000;
        final AtomicLong lastReadTime = new AtomicLong(System.currentTimeMillis());
        final BlockingQueue<Integer> heartbeats = new LinkedBlockingQueue<>();
        final AtomicBoolean readerIdle = new AtomicBoolean();
        when(blocked._engine.getLastReadTime()).thenAnswer(invocation -> lastReadTime.get());
        doAnswer(invocation ->
        {
            final QpidByteBuffer input = invocation.getArgument(0);
            lastReadTime.set(System.currentTimeMillis());
            while (input.hasRemaining())
            {
                heartbeats.add((int) input.get());
            }
            return null;
        }).when(blocked._engine).received(any());
        doAnswer(invocation ->
        {
            readerIdle.set(true);
            blocked._connection.close();
            return null;
        }).when(blocked._engine).readerIdle();

        final AggregateTicker ticker = new AggregateTicker();
        ticker.addTicker(new ServerIdleReadTimeoutTicker(blocked._connection, blocked._engine, readIdleMillis));
        when(blocked._engine.getAggregateTicker()).thenReturn(ticker);
        blocked._engine.notifyWork();
        blocked.awaitWriteInterest();

        final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2L * readIdleMillis);
        do
        {
            blocked._client.getOutputStream().write(42);
            assertEquals(Integer.valueOf(42), heartbeats.poll(readIdleMillis, TimeUnit.MILLISECONDS),
                    "Incoming heartbeats must be consumed while the peer is not reading output");
            assertFalse(readerIdle.get(), "A peer sending heartbeats must not be considered idle");
            blocked.awaitWriteInterest();
            Thread.sleep(readIdleMillis / 4);
        }
        while (System.nanoTime() < end);

        assertTrue(blocked._connection.wantsWrite(), "Output must remain blocked throughout the heartbeat checks");
        blocked._closed.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(readerIdle.get(), "The read timeout must still close the connection after heartbeats stop");
    }

    private void startScheduler(final int workers)
    {
        _scheduler = new NetworkConnectionScheduler(getTestName(), 1, workers, 1L, task ->
        {
            final Thread thread = new Thread(task, getTestName() + "-" + _threads.size());
            _threads.add(thread);
            return thread;
        });
        _scheduler.start();
    }

    private TestConnection createConnection(final int networkBufferSize) throws Exception
    {
        final TestConnection connection = new TestConnection(_scheduler, networkBufferSize);
        _connections.add(connection);
        return connection;
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if (_scheduler != null)
        {
            try
            {
                for (final TestConnection connection : _connections)
                {
                    connection._connection.close();
                }
                for (final TestConnection connection : _connections)
                {
                    connection._closed.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                }
            }
            finally
            {
                for (final TestConnection connection : _connections)
                {
                    connection._client.close();
                    connection._channel.close();
                }
                _scheduler.close();
                try
                {
                    for (final Thread thread : _threads)
                    {
                        thread.join(TIMEOUT_MILLIS);
                        assertFalse(thread.isAlive(), "Scheduler worker did not terminate: " + thread.getName());
                    }
                }
                finally
                {
                    for (final Thread thread : _threads)
                    {
                        thread.interrupt();
                    }
                    for (final Thread thread : _threads)
                    {
                        thread.join(TIMEOUT_MILLIS);
                    }
                }
            }
        }
    }

    private static final class TestConnection
    {
        private final AtomicBoolean _hasWork = new AtomicBoolean();
        private final AtomicBoolean _blocked = new AtomicBoolean();
        private final AtomicReference<Iterator<Runnable>> _pending =
                new AtomicReference<>(Collections.emptyIterator());
        private final CompletableFuture<Void> _closed = new CompletableFuture<>();
        private final Socket _client = new Socket();
        private final SocketChannel _channel;
        private final ProtocolEngine _engine = mock(ProtocolEngine.class);
        private final NonBlockingConnection _connection;

        private TestConnection(final NetworkConnectionScheduler scheduler, final int networkBufferSize) throws Exception
        {
            try (final ServerSocketChannel listener = ServerSocketChannel.open())
            {
                listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                _client.setReceiveBufferSize(4096);
                _client.setSoTimeout(TIMEOUT_MILLIS);
                _client.connect(listener.getLocalAddress(), TIMEOUT_MILLIS);
                _channel = listener.accept();
            }
            _channel.configureBlocking(false);
            _channel.socket().setSendBufferSize(4096);
            final AggregateTicker ticker = mock(AggregateTicker.class);
            when(ticker.getTimeToNextTick(anyLong())).thenReturn(Integer.MAX_VALUE);
            when(_engine.getAggregateTicker()).thenReturn(ticker);
            when(_engine.hasWork()).thenAnswer(invocation -> _hasWork.get());
            when(_engine.processPendingIterator()).thenAnswer(invocation -> _pending.get());
            doAnswer(invocation ->
            {
                _hasWork.set(false);
                return null;
            }).when(_engine).clearWork();
            doAnswer(invocation ->
            {
                final boolean blocked = invocation.getArgument(0);
                if (_blocked.getAndSet(blocked) && !blocked)
                {
                    _engine.notifyWork();
                }
                return null;
            }).when(_engine).setTransportBlockedForWriting(anyBoolean());
            doAnswer(invocation ->
            {
                _closed.complete(null);
                return null;
            }).when(_engine).closed();
            final AmqpPort<?> port = mock(AmqpPort.class);
            when(port.getNetworkBufferSize()).thenReturn(networkBufferSize);
            when(port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(1);
            when(port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(0L);
            when(port.getContextValue(Integer.class, AmqpPort.MAX_GATHERING_WRITE_BUFFERS)).thenReturn(1024);
            final Broker<?> broker = mock(Broker.class);
            when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
            doReturn(broker).when(port).getParent();
            final AtomicReference<Action<ProtocolEngine>> workListener = new AtomicReference<>();
            doAnswer(invocation ->
            {
                workListener.set(invocation.getArgument(0));
                return null;
            }).when(_engine).setWorkListener(any());
            doAnswer(invocation ->
            {
                _hasWork.set(true);
                workListener.get().performAction(_engine);
                return null;
            }).when(_engine).notifyWork();
            _connection = new NonBlockingConnection(_channel, _engine, Set.of(TransportEncryption.NONE),
                    () -> { }, scheduler, port);
            scheduler.addConnection(_connection);
        }

        private void submit(final List<Runnable> work)
        {
            _pending.set(work.iterator());
            _engine.notifyWork();
        }

        private void send(final byte[] bytes)
        {
            try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(bytes))
            {
                _connection.send(buffer);
            }
        }

        private void awaitWriteInterest() throws InterruptedException
        {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);
            while (System.nanoTime() < deadline)
            {
                final SelectionKey key = _channel.keyFor(_connection.getSelectionTask().getSelector());
                final int expectedOps = SelectionKey.OP_WRITE | (_connection.wantsRead() ? SelectionKey.OP_READ : 0);
                if (_connection.wantsWrite() && !_connection.isStateChanged() && key != null &&
                        key.isValid() && key.interestOps() == expectedOps)
                {
                    return;
                }
                Thread.sleep(10);
            }
            fail("A socket with pending output never returned to write-readiness selection");
        }
    }
}
