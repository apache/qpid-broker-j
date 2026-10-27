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

import static org.apache.qpid.server.transport.NonBlockingConnectionTestHelper.createPort;
import static org.apache.qpid.server.transport.NonBlockingConnectionTestHelper.createSocketChannel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionCloseTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 1024;
    private final AtomicBoolean _writable = new AtomicBoolean();
    private AggregateTicker _ticker;
    private SocketChannel _socket;
    private ProtocolEngine _engine;
    private NetworkConnectionScheduler _scheduler;
    private SelectorThread.SelectionTask _selectionTask;
    private AmqpPort<?> _port;
    private SSLEngine _sslEngine;
    private NonBlockingConnection _connection;

    @BeforeEach
    void setUp() throws Exception
    {
        _ticker = new AggregateTicker();
        _writable.set(false);
        _connection = null;
        _sslEngine = null;
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, 16, 0.5);
        _socket = createSocketChannel(_writable::get);
        _engine = mock(ProtocolEngine.class);
        when(_engine.getAggregateTicker()).thenReturn(_ticker);
        when(_engine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());
        _scheduler = mock(NetworkConnectionScheduler.class);
        _selectionTask = mock(SelectorThread.SelectionTask.class);
        _port = createPort(BUFFER_SIZE, 1000L);
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            if (_connection != null)
            {
                _connection.abort();
                _connection.doWork();
            }
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
        }
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void expiredDeadlineDiscardsOutputWithoutFinalWrite(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        fillOutput();
        _ticker.addTicker(new ConnectionClosingTicker(0, _connection));
        clearInvocations(_socket, _engine, _selectionTask);

        assertTrue(_connection.doWork());
        assertTrue(_connection.doWork(), "Shutdown must be idempotent");

        verify(_socket, never()).write(any(ByteBuffer[].class));
        verify(_engine, never()).processPendingIterator();
        verify(_selectionTask).wakeup();
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void deadlineExpiresDuringPendingWork(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        _writable.set(true);
        final AtomicBoolean expired = new AtomicBoolean();
        installDeadline(expired);
        final Runnable pending = mock(Runnable.class);
        final List<Runnable> work = List.of(() ->
        {
            queueOutput();
            expired.set(true);
        }, pending);
        when(_engine.processPendingIterator()).thenReturn(work.iterator());

        assertTrue(_connection.doWork(), "Deadline expiry must stop the active pending-work loop");

        verify(pending, never()).run();
        verify(_socket, never()).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void deadlineInstalledByLastPendingTaskDiscardsOutput(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        _writable.set(true);
        final List<Runnable> work = List.of(() ->
        {
            queueOutput();
            _connection.setCloseDeadline(new ConnectionClosingTicker(0, _connection));
        });
        when(_engine.processPendingIterator()).thenReturn(work.iterator());

        assertTrue(_connection.doWork(), "A deadline installed during processing must prevent the final pending write");

        verify(_socket, never()).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @CsvSource({"NONE, true", "NONE, false", "TLS, true", "TLS, false"})
    void deadlineExpiresDuringSuccessfulWrite(final TransportEncryption encryption,
                                              final boolean pendingWork) throws Exception
    {
        createConnection(encryption);
        final AtomicBoolean expired = new AtomicBoolean();
        installDeadline(expired);
        queueOutput();
        final Runnable pending = mock(Runnable.class);
        when(_engine.processPendingIterator()).thenReturn(pendingWork ?
                List.of(pending).iterator() : Collections.emptyIterator());
        when(_socket.write(any(ByteBuffer[].class))).thenAnswer(invocation ->
        {
            long written = 0;
            for (final ByteBuffer buffer : (ByteBuffer[]) invocation.getArgument(0))
            {
                written += buffer.remaining();
                buffer.position(buffer.limit());
            }
            expired.set(true);
            return written;
        });

        assertTrue(_connection.doWork(), "Successful writes must not postpone deadline enforcement");

        verify(pending, never()).run();
        verify(_socket).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void abortDuringPendingWorkStopsFurtherTasks(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        _writable.set(true);
        final Runnable pending = mock(Runnable.class);
        final List<Runnable> work = List.of(() ->
        {
            queueOutput();
            _connection.abort();
        }, pending);
        when(_engine.processPendingIterator()).thenReturn(work.iterator());

        assertTrue(_connection.doWork());

        verify(pending, never()).run();
        verify(_socket, never()).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @Test
    void deadlineExpiresAfterLastTlsWriteSkipsFurtherIo() throws Exception
    {
        createConnection(TransportEncryption.TLS);
        final AtomicBoolean expired = new AtomicBoolean();
        installDeadline(expired);
        try (final QpidByteBuffer output = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
        {
            _connection.send(output);
        }
        when(_socket.write(any(ByteBuffer[].class))).thenAnswer(invocation ->
        {
            long written = 0;
            for (final ByteBuffer buffer : (ByteBuffer[]) invocation.getArgument(0))
            {
                written += buffer.remaining();
                buffer.position(buffer.limit());
            }
            expired.set(true);
            // Only wrapping after expiry matters, regardless of how the output was batched.
            clearInvocations(_sslEngine);
            return written;
        });

        assertTrue(_connection.doWork());

        verify(_socket).write(any(ByteBuffer[].class));
        verify(_sslEngine, never()).wrap(any(ByteBuffer[].class), any(ByteBuffer.class));
        verify(_engine, never()).setTransportBlockedForWriting(anyBoolean());
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void gracefulCloseDuringPendingWorkFlushesOutput(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        _writable.set(true);
        final Runnable pending = mock(Runnable.class);
        final List<Runnable> work = List.of(() ->
        {
            queueOutput();
            _connection.close();
        }, pending);
        when(_engine.processPendingIterator()).thenReturn(work.iterator());

        assertTrue(_connection.doWork());

        verify(pending, never()).run();
        verify(_socket).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @Test
    void expiryDoesNotWaitForTlsUnwrap() throws Exception
    {
        createConnection(TransportEncryption.TLS);
        when(_sslEngine.getHandshakeStatus()).thenReturn(SSLEngineResult.HandshakeStatus.NEED_UNWRAP);
        fillOutput();
        _ticker.addTicker(new ConnectionClosingTicker(0, _connection));
        clearInvocations(_socket, _sslEngine);

        assertTrue(_connection.doWork());

        verify(_socket, never()).write(any(ByteBuffer[].class));
        verify(_sslEngine, never()).wrap(any(ByteBuffer[].class), any(ByteBuffer.class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void abortCanOvertakeGracefulTransportClose(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        fillOutput();
        _connection.close();
        _connection.abort();
        clearInvocations(_socket);

        assertTrue(_connection.doWork());

        verify(_socket, never()).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void finalWriteRetriesCannotOutliveProtocolDeadline(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        fillOutput();
        final Ticker deadline = mock(Ticker.class);
        when(deadline.getTimeToNextTick(anyLong())).thenReturn(1, 0);
        _connection.setCloseDeadline(deadline);
        _connection.close();
        clearInvocations(_socket);

        assertTrue(_connection.doWork());

        verify(_socket).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @Test
    void repeatedDeadlineDoesNotExtendFinalWrite() throws Exception
    {
        createConnection(TransportEncryption.NONE);
        fillOutput();
        _connection.setCloseDeadline(new ConnectionClosingTicker(0, _connection));
        _connection.setCloseDeadline(new ConnectionClosingTicker(30_000, _connection));
        _connection.close();
        clearInvocations(_socket);

        assertTrue(_connection.doWork());

        verify(_socket, never()).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void gracefulTransportCloseStillFlushesOutput(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        fillOutput();
        _writable.set(true);
        clearInvocations(_socket);
        _connection.close();

        assertTrue(_connection.doWork());

        verify(_socket).write(any(ByteBuffer[].class));
        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void directTransportCloseIsBounded(final TransportEncryption encryption) throws Exception
    {
        when(_port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(0L);
        createConnection(encryption);
        fillOutput();
        _connection.close();

        assertTrue(_connection.doWork(), "Direct native close must reach the final-write timeout");

        assertClosedAndReleased();
    }

    @ParameterizedTest
    @EnumSource(TransportEncryption.class)
    void abortReleasesOutputEvenIfProtocolCleanupFails(final TransportEncryption encryption) throws Exception
    {
        createConnection(encryption);
        fillOutput();
        doThrow(new IllegalStateException("Protocol cleanup failed")).when(_engine).closed();
        _connection.abort();

        assertThrows(IllegalStateException.class, _connection::doWork);

        assertClosedAndReleased();
    }

    private void assertClosedAndReleased() throws IOException
    {
        verify(_socket).close();
        verify(_engine).closed();
        verify(_scheduler).removeConnection(_connection);
        assertEquals(0, QpidByteBuffer.getNumberOfBuffersInUse(), "Shutdown must release buffers");
    }

    private void createConnection(final TransportEncryption encryption) throws Exception
    {
        if (encryption == TransportEncryption.TLS)
        {
            configureTls();
        }
        _connection = new NonBlockingConnection(_socket, _engine, Set.of(encryption), () -> { }, _scheduler, _port);
        _connection.setSelectionTask(_selectionTask);
    }

    private void configureTls() throws Exception
    {
        final SSLContext sslContext = mock(SSLContext.class);
        _sslEngine = mock(SSLEngine.class);
        final SSLSession session = mock(SSLSession.class);
        when(_port.getSSLContext()).thenReturn(sslContext);
        when(sslContext.createSSLEngine()).thenReturn(_sslEngine);
        when(_sslEngine.getSession()).thenReturn(session);
        when(_sslEngine.getEnabledProtocols()).thenReturn(new String[]{"TLSv1.3"});
        when(_sslEngine.getSupportedProtocols()).thenReturn(new String[]{"TLSv1.3"});
        when(_sslEngine.getEnabledCipherSuites()).thenReturn(new String[]{"TLS_AES_128_GCM_SHA256"});
        when(_sslEngine.getSupportedCipherSuites()).thenReturn(new String[]{"TLS_AES_128_GCM_SHA256"});
        when(session.getPacketBufferSize()).thenReturn(BUFFER_SIZE);
        when(session.getApplicationBufferSize()).thenReturn(BUFFER_SIZE);
        when(_port.getContextValue(Boolean.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING)).thenReturn(false);
        when(_port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_WARN_THRESHOLD))
                .thenReturn(0);
        when(_port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_BREAK_THRESHOLD))
                .thenReturn(0);
        when(_sslEngine.getHandshakeStatus()).thenReturn(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING);
        when(_sslEngine.wrap(any(ByteBuffer[].class), any(ByteBuffer.class))).thenAnswer(invocation ->
        {
            final ByteBuffer[] sources = invocation.getArgument(0);
            final ByteBuffer target = invocation.getArgument(1);
            int count = 0;
            for (int i = 0; i < sources.length && target.hasRemaining(); i++)
            {
                while (sources[i].hasRemaining() && target.hasRemaining())
                {
                    target.put(sources[i].get());
                    count++;
                }
            }
            final SSLEngineResult.HandshakeStatus handshakeStatus = SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
            return new SSLEngineResult(SSLEngineResult.Status.OK, handshakeStatus, count, count);
        });
    }

    private void fillOutput()
    {
        queueOutput();
        assertFalse(_connection.doWork());
        assertTrue(_connection.wantsWrite() || _connection.wantsRead(),
                "Blocked output must wait for socket capacity or TLS peer input");
        assertTrue(QpidByteBuffer.getNumberOfBuffersInUse() > 0);
    }

    private void queueOutput()
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(2 * BUFFER_SIZE))
        {
            _connection.send(buffer);
        }
    }

    private void installDeadline(final AtomicBoolean expired)
    {
        final Ticker deadline = mock(Ticker.class);
        when(deadline.getTimeToNextTick(anyLong())).thenAnswer(invocation -> expired.get() ? 0 : 1000);
        _connection.setCloseDeadline(deadline);
        _ticker.addTicker(deadline);
    }
}
