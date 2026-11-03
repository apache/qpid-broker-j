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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionTLSTest extends UnitTestBase
{
    private static final int NETWORK_BUFFER_SIZE = 64 * 1024;
    private SSLContext _context;

    @BeforeAll
    void createTlsContext() throws Exception
    {
        _context = TlsTestContext.create();
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void handshakeWaitsForEncryptedOutputBeforePeerInputAndRestoresWriteInterest(final String protocol) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            final byte[] payload = new byte[]{1, 2, 3};
            tls.send(payload);
            tls._writeAllowance = 0;
            tls.wrapPeer(ByteBuffer.allocate(0));
            tls._connection.doWork();

            assertTrue(tls._connection.wantsWrite(), "The blocked server handshake must select for writing");
            tls._connection.doWork();
            assertFalse(tls._connection.isStateChanged(), "Blocked encrypted output must not cause a busy loop");
            assertFalse(tls._connection.isPartialRead());

            tls._writeAllowance = 1;
            tls._connection.doWork();
            assertEquals(1, tls._toPeer.position(), "Only the explicitly granted write capacity may be consumed");
            assertTrue(tls._connection.wantsWrite(), "A partial handshake write must retain write interest");

            tls._writeAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            assertEquals(SSLEngineResult.HandshakeStatus.NEED_UNWRAP, tls._server.getHandshakeStatus());
            assertTrue(tls._connection.wantsRead());
            assertFalse(tls._connection.wantsWrite(), "Pending application output must wait for the peer handshake");
            assertFalse(tls._connection.isStateChanged());

            tls.finishHandshake();
            assertArrayEquals(payload, tls._peerReceived.toByteArray());
            tls._peerReceived.reset();

            tls._writeAllowance = 0;
            tls.send(payload);
            tls._connection.doWork();
            assertTrue(tls._connection.wantsWrite(), "NEED_UNWRAP must not remain cached after the handshake");
            assertFalse(tls._connection.isStateChanged());

            tls._writeAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            tls.readAtPeer();
            assertArrayEquals(payload, tls._peerReceived.toByteArray());
            assertFalse(tls._connection.wantsWrite());
            assertFalse(tls._blocked);
        }
    }

    @ParameterizedTest
    @CsvSource({"TLSv1.2, false", "TLSv1.2, true", "TLSv1.3, false", "TLSv1.3, true"})
    void applicationReadRequestsContinuationOnlyWhenOutputIsDrained(final String protocol,
                                                                    final boolean blocked) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            tls.finishHandshake();
            final byte[] outbound = new byte[]{11, 12, 13};
            final byte[] inbound = new byte[]{21, 22, 23};
            tls._writeAllowance = blocked ? 0 : Integer.MAX_VALUE;
            tls.send(outbound);
            tls.wrapPeer(ByteBuffer.wrap(inbound));

            tls._connection.doWork();

            assertArrayEquals(inbound, tls._received.toByteArray(), "The real TLS engine must decrypt peer input");
            assertEquals(blocked, tls._connection.wantsWrite());
            assertEquals(!blocked, tls._connection.isStateChanged(),
                    "Decrypted input must request a continuation only when encrypted output has drained");

            tls._connection.doWork();
            assertFalse(tls._connection.isStateChanged(), "A pass without new input must not keep rescheduling");
            assertFalse(tls._connection.isPartialRead());
            tls._writeAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            tls.readAtPeer();
            assertArrayEquals(outbound, tls._peerReceived.toByteArray());
            assertFalse(tls._connection.wantsWrite());
        }
    }

    @ParameterizedTest
    @CsvSource({"TLSv1.2, false, 1", "TLSv1.2, false, 5", "TLSv1.2, false, -1",
                "TLSv1.2, true, 1", "TLSv1.2, true, 5", "TLSv1.2, true, -1",
                "TLSv1.3, false, 1", "TLSv1.3, false, 5", "TLSv1.3, false, -1",
                "TLSv1.3, true, 1", "TLSv1.3, true, 5", "TLSv1.3, true, -1"})
    void incompleteTlsInputWaitsForMoreNetworkData(final String protocol,
                                                   final boolean blocked,
                                                   final int prefixLength) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            tls.finishHandshake();
            final byte[] inbound = new byte[]{31, 32, 33};
            final byte[] outbound = new byte[]{41, 42, 43};
            tls.wrapPeer(ByteBuffer.wrap(inbound));
            tls._readAllowance = prefixLength < 0 ? tls._fromPeer.position() - 1 : prefixLength;
            tls._writeAllowance = blocked ? 0 : Integer.MAX_VALUE;
            tls.send(outbound);
            tls._connection.doWork();

            assertEquals(0, tls._received.size());
            assertEquals(blocked, tls._connection.wantsWrite());
            assertFalse(tls._connection.isStateChanged(), "An incomplete TLS record cannot make progress locally");
            tls._connection.doWork();
            assertFalse(tls._connection.isStateChanged());
            assertFalse(tls._connection.isPartialRead());

            tls._writeAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            assertFalse(tls._connection.wantsWrite());
            assertFalse(tls._connection.isPartialRead(), "No new socket bytes were supplied during the drain");
            tls.assertWaitingForInput();

            tls._readAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            assertArrayEquals(inbound, tls._received.toByteArray());
            tls._connection.doWork();
            assertFalse(tls._connection.isStateChanged(), "Consuming the buffered record must end the continuation");
            tls.readAtPeer();
            assertArrayEquals(outbound, tls._peerReceived.toByteArray());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void completeRecordBeforeIncompleteRecordRequestsOnlyOneContinuation(final String protocol) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            tls.finishHandshake();
            final byte[] first = new byte[]{1, 2, 3};
            final byte[] second = new byte[]{4, 5, 6};
            for (int round = 0; round < 3; round++)
            {
                tls._received.reset();
                tls.wrapPeer(ByteBuffer.wrap(first));
                final int completeRecordLength = tls._fromPeer.position();
                tls.wrapPeer(ByteBuffer.wrap(second));
                tls._readAllowance = completeRecordLength + 5;

                tls._connection.doWork();

                assertArrayEquals(first, tls._received.toByteArray());
                assertTrue(tls._connection.isStateChanged(), "New plaintext still requests a continuation");
                tls.assertWaitingForInput();

                tls._readAllowance = Integer.MAX_VALUE;
                tls._connection.doWork();
                assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, tls._received.toByteArray());
                tls.assertWaitingForInput();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"TLSv1.2, 1", "TLSv1.2, 5", "TLSv1.2, 6",
                "TLSv1.3, 1", "TLSv1.3, 5", "TLSv1.3, 6"})
    void incompleteClientHelloWaitsForMoreNetworkData(final String protocol, final int prefixLength) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            tls.wrapPeer(ByteBuffer.allocate(0));
            tls._readAllowance = prefixLength;
            tls._connection.doWork();
            tls.assertWaitingForInput();

            tls._readAllowance = Integer.MAX_VALUE;
            tls.finishHandshake();
            tls.wrapPeer(ByteBuffer.wrap(new byte[]{7, 8, 9}));
            tls._connection.doWork();
            assertArrayEquals(new byte[]{7, 8, 9}, tls._received.toByteArray());
            tls.assertWaitingForInput();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void incompleteRecordDoesNotBlockExplicitProtocolOutput(final String protocol) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            tls.finishHandshake();
            final byte[] inbound = new byte[]{1, 2, 3};
            final byte[] outbound = new byte[]{4, 5, 6};
            tls.wrapPeer(ByteBuffer.wrap(inbound));
            tls._readAllowance = 5;
            tls._connection.doWork();
            tls.assertWaitingForInput();

            when(tls._engine.processPendingIterator()).thenReturn(List.<Runnable>of(() -> tls.send(outbound)).iterator());
            tls._engine.notifyWork();
            tls._connection.doWork();
            tls.readAtPeer();
            assertArrayEquals(outbound, tls._peerReceived.toByteArray());
            tls.assertWaitingForInput();

            tls._readAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            assertArrayEquals(inbound, tls._received.toByteArray());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void recordRecoveryPreservesPostHandshakeWork(final String protocol) throws Exception
    {
        try (final TlsConnection tls = new TlsConnection(protocol))
        {
            tls.finishHandshake();
            tls.wrapPeer(ByteBuffer.wrap(new byte[]{1, 2, 3}));
            tls._readAllowance = 5;
            tls._connection.doWork();
            tls.assertWaitingForInput();
            tls._readAllowance = Integer.MAX_VALUE;
            tls._connection.doWork();
            assertArrayEquals(new byte[]{1, 2, 3}, tls._received.toByteArray());

            // Renegotiation in TLS 1.2, and key update in TLS 1.3.
            tls._peer.beginHandshake();
            tls.finishHandshake();
            tls.wrapPeer(ByteBuffer.wrap(new byte[]{4, 5, 6}));
            tls._connection.doWork();
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, tls._received.toByteArray());
            tls.assertWaitingForInput();
        }
    }

    /** Real TLS engines with explicitly granted socket capacity; all work runs on the test thread */
    private final class TlsConnection implements AutoCloseable
    {
        private final SSLEngine _server = _context.createSSLEngine();
        private final SSLEngine _peer = _context.createSSLEngine();
        private final ByteBuffer _fromPeer = ByteBuffer.allocate(4 * NETWORK_BUFFER_SIZE);
        private final ByteBuffer _toPeer = ByteBuffer.allocate(4 * NETWORK_BUFFER_SIZE);
        private final ByteArrayOutputStream _received = new ByteArrayOutputStream();
        private final ByteArrayOutputStream _peerReceived = new ByteArrayOutputStream();
        private final ProtocolEngine _engine = mock(ProtocolEngine.class);
        private final NonBlockingConnection _connection;
        private int _writeAllowance = Integer.MAX_VALUE;
        private int _readAllowance = Integer.MAX_VALUE;
        private boolean _hasWork;
        private boolean _blocked;

        private TlsConnection(final String protocol) throws Exception
        {
            _peer.setUseClientMode(true);
            _peer.setEnabledProtocols(new String[]{protocol});
            _peer.beginHandshake();
            final Socket socket = mock(Socket.class);
            when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("localhost", 1000));
            when(socket.getLocalSocketAddress()).thenReturn(new InetSocketAddress("localhost", 5672));
            final SocketChannel channel = mock(SocketChannel.class);
            when(channel.socket()).thenReturn(socket);
            when(channel.read(any(ByteBuffer.class))).thenAnswer(invocation ->
            {
                _fromPeer.flip();
                final int read = transfer(_fromPeer, invocation.getArgument(0), _readAllowance);
                _fromPeer.compact();
                _readAllowance -= read;
                return read;
            });
            when(channel.write(any(ByteBuffer[].class))).thenAnswer(invocation ->
            {
                long written = 0;
                for (final ByteBuffer buffer : (ByteBuffer[]) invocation.getArgument(0))
                {
                    final int count = transfer(buffer, _toPeer, _writeAllowance);
                    written += count;
                    _writeAllowance -= count;
                }
                return written;
            });
            final ProtocolEngine engine = _engine;
            when(engine.getAggregateTicker()).thenReturn(new AggregateTicker());
            when(engine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());
            when(engine.hasWork()).thenAnswer(invocation -> _hasWork);
            doAnswer(invocation ->
            {
                _hasWork = false; return null;
            }).when(engine).clearWork();
            doAnswer(invocation ->
            {
                _hasWork = true; return null;
            }).when(engine).notifyWork();
            doAnswer(invocation ->
            {
                _blocked = invocation.getArgument(0); return null;
            }).when(engine).setTransportBlockedForWriting(anyBoolean());
            doAnswer(invocation ->
            {
                final QpidByteBuffer input = invocation.getArgument(0);
                final byte[] bytes = new byte[input.remaining()];
                input.get(bytes);
                _received.writeBytes(bytes);
                return null;
            }).when(engine).received(any());
            final SSLContext serverContext = mock(SSLContext.class);
            when(serverContext.createSSLEngine()).thenReturn(_server);
            final AmqpPort<?> port = mock(AmqpPort.class);
            when(port.getNetworkBufferSize()).thenReturn(NETWORK_BUFFER_SIZE);
            when(port.getSSLContext()).thenReturn(serverContext);
            when(port.getTlsProtocolAllowList()).thenReturn(List.of(protocol));
            when(port.getContextValue(Boolean.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING)).thenReturn(false);
            when(port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_WARN_THRESHOLD))
                    .thenReturn(1000);
            when(port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_BREAK_THRESHOLD))
                    .thenReturn(10000);
            when(port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(1);
            when(port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(0L);
            when(port.getContextValue(Integer.class, AmqpPort.MAX_GATHERING_WRITE_BUFFERS)).thenReturn(1024);
            final Broker<?> broker = mock(Broker.class);
            when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
            doReturn(broker).when(port).getParent();
            final Set<TransportEncryption> encryptionSet = Set.of(TransportEncryption.TLS);
            _connection = new NonBlockingConnection(channel, engine, encryptionSet, () -> { }, mock(), port);
            _connection.setSelectionTask(mock(SelectorThread.SelectionTask.class));
        }

        private void finishHandshake() throws Exception
        {
            for (int step = 0; step < 100; step++)
            {
                readAtPeer();
                if (_peer.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP)
                {
                    wrapPeer(ByteBuffer.allocate(0));
                }
                _connection.doWork();
                if (_peer.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING &&
                        _server.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING &&
                        _fromPeer.position() == 0 && _toPeer.position() == 0)
                {
                    assertEquals(_peer.getSession().getProtocol(), _server.getSession().getProtocol());
                    assertFalse(_connection.wantsWrite());
                    return;
                }
            }
            fail("TLS handshake did not complete: peer=" + _peer.getHandshakeStatus() + ", server=" +
                    _server.getHandshakeStatus());
        }

        private void wrapPeer(final ByteBuffer application) throws Exception
        {
            do
            {
                final SSLEngineResult result = _peer.wrap(application, _fromPeer);
                assertEquals(SSLEngineResult.Status.OK, result.getStatus());
                runPeerTasks();
                assertTrue(result.bytesConsumed() > 0 || result.bytesProduced() > 0,
                        "Peer wrap made no progress");
            }
            while (application.hasRemaining() || _peer.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP);
        }

        private void readAtPeer() throws Exception
        {
            _toPeer.flip();
            while (_toPeer.hasRemaining())
            {
                final ByteBuffer application = ByteBuffer.allocate(NETWORK_BUFFER_SIZE);
                final SSLEngineResult result = _peer.unwrap(_toPeer, application);
                runPeerTasks();
                _peerReceived.write(application.array(), 0, application.position());
                if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW ||
                        _peer.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP)
                {
                    break;
                }
                assertEquals(SSLEngineResult.Status.OK, result.getStatus());
                assertTrue(result.bytesConsumed() > 0 || result.bytesProduced() > 0, "Peer unwrap made no progress");
            }
            _toPeer.compact();
        }

        private void runPeerTasks()
        {
            Runnable task;
            while ((task = _peer.getDelegatedTask()) != null)
            {
                task.run();
            }
        }

        private void send(final byte[] bytes)
        {
            try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(bytes))
            {
                _connection.send(buffer);
            }
        }

        private void assertWaitingForInput()
        {
            final int withheld = _fromPeer.position();
            for (int pass = 0; pass < 16; pass++)
            {
                assertFalse(_connection.doWork(), "Waiting for input must keep the connection open");
                assertFalse(_connection.isStateChanged(), "A zero-read pass must not request another worker turn");
                assertFalse(_connection.isPartialRead(), "The socket supplied no new bytes");
                assertTrue(_connection.wantsRead(), "The selector must continue watching for input");
                assertFalse(_connection.wantsWrite());
                assertEquals(withheld, _fromPeer.position(), "The remainder must remain withheld");
            }
        }

        @Override
        public void close()
        {
            _writeAllowance = Integer.MAX_VALUE;
            _connection.close();
            _connection.doWork();
            _peer.closeOutbound();
        }
    }

    private static int transfer(final ByteBuffer source, final ByteBuffer destination, final int allowance)
    {
        final int length = Math.min(allowance, Math.min(source.remaining(), destination.remaining()));
        final ByteBuffer part = source.duplicate();
        part.limit(part.position() + length);
        destination.put(part);
        source.position(source.position() + length);
        return length;
    }
}
