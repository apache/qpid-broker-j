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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
    private static final int NETWORK_BUFFER_SIZE = 64 * 1024;
    private static final int BUFFER_SIZE = 4096;

    private final List<Thread> _threads = new CopyOnWriteArrayList<>();
    private final List<TestConnection> _connections = new ArrayList<>();
    private NetworkConnectionScheduler _scheduler;
    private SSLContext _sslContext;

    @BeforeAll
    void createTlsContext() throws Exception
    {
        _sslContext = TlsTestContext.create();
    }

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

    @ParameterizedTest
    @CsvSource({"TLSv1.2, 2", "TLSv1.2, 4", "TLSv1.3, 2", "TLSv1.3, 4"})
    void blockedTlsSocketReturnsWorkersAndRecoversPendingOutput(final String protocol,
                                                                final int workers) throws Exception
    {
        startScheduler(workers);
        final TestConnection blocked = createTlsConnection(protocol);
        final TestConnection independent = createTlsConnection(protocol);
        final byte[] payload = new byte[2 * 1024 * 1024];
        for (int i = 0; i < payload.length; i++)
        {
            payload[i] = (byte) i;
        }
        final byte[] marker = new byte[]{11, 22, 33};
        for (int round = 0; round < 3; round++)
        {
            blocked.submit(List.of(() -> blocked.send(payload), () -> blocked.send(marker)));
            blocked.awaitWriteInterest();
            assertTrue(blocked._blocked.get());

            independent.submit(List.of(() -> independent.send(marker)));
            assertArrayEquals(marker, independent._client.getInputStream().readNBytes(marker.length),
                    "An independent TLS connection must progress while the first socket is blocked");

            blocked._client.getOutputStream().write(42 + round);
            assertEquals(Integer.valueOf(42 + round), blocked._received.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    "TLS input must still be decrypted while encrypted output is blocked");
            blocked.awaitWriteInterest();
            assertTrue(blocked._blocked.get(), "Reading peer input must not briefly resume blocked consumers");

            assertArrayEquals(payload, blocked._client.getInputStream().readNBytes(payload.length));
            assertArrayEquals(marker, blocked._client.getInputStream().readNBytes(marker.length),
                    "Pending output must resume without losing or reordering application bytes");
            blocked.awaitSelection(false);
            assertFalse(blocked._blocked.get(), "Draining encrypted output must resume consumers");
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
        // Keep wall-clock scheduling timestamps out of the idle calculation so only the test advances time.
        blocked._ticker.addTicker(new ServerIdleReadTimeoutTicker(mock(ServerNetworkConnection.class),
                blocked._engine, readIdleMillis)
        {
            @Override
            public int getTimeToNextTick(final long ignored)
            {
                return super.getTimeToNextTick(blocked._currentTime.get());
            }
        });
        blocked.awaitWriteInterest();

        for (int i = 0; i < 8; i++)
        {
            blocked._currentTime.addAndGet(readIdleMillis / 4);
            blocked._client.getOutputStream().write(42);
            assertEquals(Integer.valueOf(42), blocked._received.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    "Incoming heartbeats must be consumed while the peer is not reading output");
            blocked.awaitWriteInterest();
            assertFalse(blocked._readerIdle.get(), "A peer sending heartbeats must not be considered idle");
        }

        assertTrue(blocked._connection.wantsWrite(), "Output must remain blocked throughout the heartbeat checks");
        blocked._currentTime.addAndGet(readIdleMillis - 1);
        blocked.awaitWriteInterest();
        assertFalse(blocked._readerIdle.get(), "The connection must remain open until the read deadline");

        blocked._currentTime.incrementAndGet();
        blocked._connection.getSelectionTask().wakeup();
        blocked._closed.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(blocked._readerIdle.get(), "The read timeout must still close the connection after heartbeats stop");
    }

    @ParameterizedTest
    @CsvSource({"TLSv1.2, 2", "TLSv1.2, 4", "TLSv1.3, 2", "TLSv1.3, 4"})
    void incompleteTlsRecordReturnsToSelectorAndResumesOnReadiness(final String protocol,
                                                                   final int workers) throws Exception
    {
        startScheduler(workers);
        final TestConnection connection = createTlsEngineConnection();
        final TlsTestPeer peer = new TlsTestPeer(_sslContext, protocol, connection._client);
        peer.send(peer.wrap(new byte[]{42}));
        assertEquals(Integer.valueOf(42), connection._received.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        connection.awaitSelection(false);
        final TestConnection independent = createConnection(1024);

        for (int round = 0; round < 3; round++)
        {
            final byte[] record = peer.wrap(new byte[]{11, 22, 33});
            final int prefix = record.length - 1;
            final long expectedRead = connection._networkBytesRead.get() + prefix;
            peer.send(Arrays.copyOf(record, prefix));
            connection.awaitNetworkBytes(expectedRead);

            connection.awaitSelection(false);
            assertTrue(connection._connection.wantsRead());
            assertTrue(connection._received.isEmpty(), "An incomplete record must not expose plaintext");
            independent.submit(List.of(() -> independent.send(new byte[]{44})));
            assertEquals(44, independent._client.getInputStream().read());
            connection.awaitSelection(false);

            peer.send(Arrays.copyOfRange(record, prefix, record.length));
            for (int expected : new int[]{11, 22, 33})
            {
                assertEquals(Integer.valueOf(expected), connection._received.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                        "Read readiness must resume the buffered record in order");
            }
            connection.awaitSelection(false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void incompleteTlsRecordStillHonorsReadIdleTimeout(final String protocol) throws Exception
    {
        startScheduler(2);
        final TestConnection connection = createTlsEngineConnection();
        final TlsTestPeer peer = new TlsTestPeer(_sslContext, protocol, connection._client);
        peer.send(peer.wrap(new byte[]{42}));
        assertEquals(Integer.valueOf(42), connection._received.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        connection.awaitSelection(false);
        final long expectedRead = connection._networkBytesRead.get() + 5;
        peer.send(Arrays.copyOf(peer.wrap(new byte[]{1, 2, 3}), 5));
        connection.awaitNetworkBytes(expectedRead);
        connection.awaitSelection(false);

        connection._ticker.addTicker(new ServerIdleReadTimeoutTicker(mock(), connection._engine, 2000)
        {
            @Override
            public int getTimeToNextTick(final long ignored)
            {
                return super.getTimeToNextTick(connection._currentTime.get());
            }
        });
        connection.awaitSelection(false);
        assertFalse(connection._readerIdle.get());
        connection._currentTime.set(2000);
        connection._connection.getSelectionTask().wakeup();
        connection._closed.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(connection._readerIdle.get(), "Waiting for record completion must not disable timeout processing");
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

    private TestConnection createTlsConnection(final String protocol) throws Exception
    {
        final SSLSocket client = (SSLSocket) _sslContext.getSocketFactory().createSocket();
        client.setEnabledProtocols(new String[]{protocol});
        final TestConnection connection = new TestConnection(_scheduler, NETWORK_BUFFER_SIZE, client, _sslContext);
        _connections.add(connection);
        client.startHandshake();
        assertEquals(protocol, client.getSession().getProtocol());
        return connection;
    }

    private TestConnection createTlsEngineConnection() throws Exception
    {
        final TestConnection connection = new TestConnection(_scheduler, NETWORK_BUFFER_SIZE, new Socket(), _sslContext);
        _connections.add(connection);
        return connection;
    }

    @AfterEach
    void tearDown() throws Exception
    {
        try
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
        finally
        {
            _connections.clear();
            _threads.clear();
            _scheduler = null;
        }
    }

    private static final class TestConnection
    {
        private final AtomicBoolean _hasWork = new AtomicBoolean();
        private final AtomicBoolean _blocked = new AtomicBoolean();
        private final AtomicBoolean _readerIdle = new AtomicBoolean();
        private final AtomicLong _currentTime = new AtomicLong();
        private final AtomicLong _lastReadTime = new AtomicLong();
        private final AtomicReference<Iterator<Runnable>> _pending =
                new AtomicReference<>(Collections.emptyIterator());
        private final AtomicReference<SelectionInterest> _selectionInterest = new AtomicReference<>();
        private final BlockingQueue<Integer> _received = new LinkedBlockingQueue<>();
        private final AtomicLong _networkBytesRead = new AtomicLong();
        private final BlockingQueue<Long> _networkReads = new LinkedBlockingQueue<>();
        private final AggregateTicker _ticker = new AggregateTicker()
        {
            @Override
            public void resetModified()
            {
                super.resetModified();
                signalSelection();
            }
        };
        private final CompletableFuture<Void> _closed = new CompletableFuture<>();
        private final Socket _client;
        private final SocketChannel _channel;
        private final ProtocolEngine _engine = mock(ProtocolEngine.class);
        private final NonBlockingConnection _connection;

        private TestConnection(final NetworkConnectionScheduler scheduler, final int networkBufferSize) throws Exception
        {
            this(scheduler, networkBufferSize, new Socket(), null);
        }

        private TestConnection(final NetworkConnectionScheduler scheduler,
                               final int networkBufferSize,
                               final Socket client,
                               final SSLContext sslContext) throws Exception
        {
            _client = client;
            try (final ServerSocketChannel listener = ServerSocketChannel.open())
            {
                listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                _client.setReceiveBufferSize(BUFFER_SIZE);
                _client.setSoTimeout(TIMEOUT_MILLIS);
                _client.connect(listener.getLocalAddress(), TIMEOUT_MILLIS);
                _channel = listener.accept();
            }
            _channel.configureBlocking(false);
            _channel.socket().setSendBufferSize(BUFFER_SIZE);
            when(_engine.getAggregateTicker()).thenReturn(_ticker);
            when(_engine.hasWork()).thenAnswer(invocation -> _hasWork.get());
            when(_engine.processPendingIterator()).thenAnswer(invocation -> _pending.get());
            when(_engine.getLastReadTime()).thenAnswer(invocation -> _lastReadTime.get());
            doAnswer(invocation ->
            {
                final QpidByteBuffer input = invocation.getArgument(0);
                _lastReadTime.set(_currentTime.get());
                while (input.hasRemaining())
                {
                    _received.add((int) input.get());
                }
                return null;
            }).when(_engine).received(any());
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
            when(port.getSSLContext()).thenReturn(sslContext);
            when(port.getContextValue(Boolean.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING)).thenReturn(false);
            when(port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_WARN_THRESHOLD))
                    .thenReturn(1000);
            when(port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_BREAK_THRESHOLD))
                    .thenReturn(10000);
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
            final TransportEncryption transportEncryption = sslContext == null
                    ? TransportEncryption.NONE
                    : TransportEncryption.TLS;
            final Set<TransportEncryption> encryptionSet = Set.of(transportEncryption);
            _connection = new NonBlockingConnection(_channel, _engine, encryptionSet, () -> { }, scheduler, port)
            {
                @Override
                protected long readFromNetwork() throws IOException
                {
                    final long count = super.readFromNetwork();
                    if (count > 0)
                    {
                        _networkReads.add(_networkBytesRead.addAndGet(count));
                    }
                    return count;
                }
            };
            doAnswer(invocation ->
            {
                _readerIdle.set(true);
                _connection.close();
                return null;
            }).when(_engine).readerIdle();
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

        private void awaitNetworkBytes(final long expected) throws InterruptedException
        {
            while (_networkBytesRead.get() < expected)
            {
                assertNotNull(_networkReads.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                        "The connection did not read the supplied TLS fragment");
            }
        }

        private void awaitWriteInterest() throws Exception
        {
            awaitSelection(true);
        }

        private void awaitSelection(final boolean wantsWrite) throws Exception
        {
            final SelectionInterest interest = new SelectionInterest(wantsWrite, new CompletableFuture<>());
            _selectionInterest.set(interest);
            _connection.getSelectionTask().wakeup();
            try
            {
                interest.ready().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e)
            {
                fail("Socket never returned to readiness selection with wantsWrite=" + wantsWrite, e);
            }
            finally
            {
                _selectionInterest.compareAndSet(interest, null);
            }
        }

        private void signalSelection()
        {
            final SelectionInterest interest = _selectionInterest.get();
            if (interest != null)
            {
                final SelectionKey key = _channel.keyFor(_connection.getSelectionTask().getSelector());
                final int expectedOps = (interest.wantsWrite() ? SelectionKey.OP_WRITE : 0) |
                        (_connection.wantsRead() ? SelectionKey.OP_READ : 0);
                if (_connection.wantsWrite() == interest.wantsWrite() && !_connection.isStateChanged() &&
                        !_connection.isPartialRead() && key != null &&
                        key.isValid() && key.interestOps() == expectedOps)
                {
                    interest.ready().complete(null);
                }
            }
        }

        private record SelectionInterest(boolean wantsWrite, CompletableFuture<Void> ready)
        {
        }
    }
}
