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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;

class WriteJobTest extends WebSocketTestBase
{
    @Test
    void writeSubmissionsAreCoalescedWhileQueued()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }

        for (int i = 0; i < SUBMISSION_COUNT; i++)
        {
            connection.doWrite();
        }

        assertEquals(1, _threadPool.getQueueSize());
        _threadPool.runNext();

        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(any(ByteBuffer.class), callbackCaptor.capture());
        callbackCaptor.getValue().succeed();
        assertEquals(0, _threadPool.getQueueSize());
    }

    @Test
    void flushPreservesQueuedWriteBoundaries()
    {
        final List<byte[]> messages = new ArrayList<>();
        final Session session = mock(Session.class);
        doAnswer(invocation ->
        {
            final ByteBuffer data = invocation.getArgument(0);
            final byte[] message = new byte[data.remaining()];
            data.get(message);
            messages.add(message);
            final Callback callback = invocation.getArgument(1);
            callback.succeed();
            return null;
        }).when(session).sendBinary(any(ByteBuffer.class), any(Callback.class));

        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        send(connection, new byte[] {1});
        connection.flush();
        connection.flush();
        connection.doWrite();

        send(connection, new byte[] {2});
        connection.flush();
        send(connection, new byte[] {3});
        connection.flush();
        send(connection, new byte[] {4});
        send(connection, new byte[] {5});
        connection.flush();
        final byte[] split = new byte[WRITE_BATCH_SIZE + 1];
        split[0] = 6;
        split[WRITE_BATCH_SIZE] = 7;
        send(connection, split);
        connection.flush();
        send(connection, new byte[] {8});

        _threadPool.runAll();

        assertEquals(7, messages.size());
        assertArrayEquals(new byte[] {1}, messages.get(0));
        assertArrayEquals(new byte[] {2}, messages.get(1));
        assertArrayEquals(new byte[] {3}, messages.get(2));
        assertArrayEquals(new byte[] {4, 5}, messages.get(3));
        assertArrayEquals(Arrays.copyOf(split, WRITE_BATCH_SIZE), messages.get(4));
        assertArrayEquals(new byte[] {7}, messages.get(5));
        assertArrayEquals(new byte[] {8}, messages.get(6));
    }

    @Test
    void copyFailureReleasesEveryReservedBuffer()
    {
        final Session session = mock();
        final MutableTicker mutableTicker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(mutableTicker, session);
        _connectionScheduler.registerConnection(connection);
        final QpidByteBuffer message = mock();
        final QpidByteBuffer first = mock();
        final QpidByteBuffer second = mock();
        when(message.remaining()).thenReturn(1);
        when(message.duplicate()).thenReturn(first, second);
        when(first.remaining()).thenReturn(1);
        when(second.remaining()).thenReturn(1);
        when(first.get(any(byte[].class), eq(0), eq(1))).thenThrow(new IllegalStateException("Copy failed"));
        connection.send(message);
        connection.send(message);
        connection.doWrite();
        _threadPool.runAll();
        scheduleDueConnections();
        _threadPool.runAll();

        verify(first).dispose();
        verify(second).dispose();
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(endPointFor(session)).closeSocket();
    }

    @Test
    void writeStorageIsReusedOnlyAfterCompletion()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        send(connection, new byte[] {1, 2, 3, 4});
        connection.doWrite();
        _threadPool.runAll();

        final ArgumentCaptor<ByteBuffer> dataCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(dataCaptor.capture(), callbackCaptor.capture());
        final ByteBuffer firstWrite = dataCaptor.getValue();

        send(connection, new byte[] {5, 6});
        connection.doWrite();
        _threadPool.runAll();
        verify(session).sendBinary(any(ByteBuffer.class), any(Callback.class));
        final byte[] firstBytes = new byte[firstWrite.remaining()];
        firstWrite.get(firstBytes);
        assertArrayEquals(new byte[] {1, 2, 3, 4}, firstBytes);

        callbackCaptor.getValue().succeed();
        _threadPool.runAll();
        verify(session, times(2)).sendBinary(dataCaptor.capture(), callbackCaptor.capture());
        final ByteBuffer secondWrite = dataCaptor.getValue();
        assertSame(firstWrite, secondWrite);
        final byte[] secondBytes = new byte[secondWrite.remaining()];
        secondWrite.get(secondBytes);
        assertArrayEquals(new byte[] {5, 6}, secondBytes);
        callbackCaptor.getValue().succeed();
    }

    @Test
    void closedConnectionDoesNotAlterPendingWriteStorage()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        send(connection, new byte[] {1, 2, 3, 4});
        connection.doWrite();
        _threadPool.runAll();

        final ArgumentCaptor<ByteBuffer> dataCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(dataCaptor.capture(), callbackCaptor.capture());
        final ByteBuffer pendingWrite = dataCaptor.getValue();
        connection.webSocketClosed(mock(Runnable.class));
        send(connection, new byte[] {5, 6});
        callbackCaptor.getValue().succeed();
        _threadPool.runAll();

        final byte[] bytes = new byte[pendingWrite.remaining()];
        pendingWrite.get(bytes);
        assertArrayEquals(new byte[] {1, 2, 3, 4}, bytes);
        verify(session).sendBinary(any(ByteBuffer.class), any(Callback.class));
    }

    @Test
    void rejectedWriteSubmissionForcesConnectionClose()
    {
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }
        _connectionScheduler.registerConnection(connection);
        _threadPool.rejectNext();

        connection.doWrite();

        assertTrue(connection.isForceClosing());
        assertEquals(0, _threadPool.getQueueSize());
        assertEquals(Long.MAX_VALUE, scheduleDueConnections());
        verify(endPointFor(session)).closeSocket();
    }

    @Test
    void finalWritesAreBoundedAndSentOneAtATime()
    {
        final byte[] expected = new byte[WRITE_BATCH_SIZE + 1];
        expected[WRITE_BATCH_SIZE] = 1;
        final Session session = mock(Session.class);
        final MutableTicker ticker = new MutableTicker(FAR_FUTURE_TICK_DELAY_MILLIS);
        final WebSocketConnection connection = createConnection(ticker, session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(expected))
        {
            connection.send(buffer);
        }
        connection.close();

        connection.doWrite();
        _threadPool.runNext();

        final ArgumentCaptor<ByteBuffer> firstDataCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        final ArgumentCaptor<Callback> firstCallbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session).sendBinary(firstDataCaptor.capture(), firstCallbackCaptor.capture());
        assertEquals(WRITE_BATCH_SIZE, firstDataCaptor.getValue().remaining());
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        firstCallbackCaptor.getValue().succeed();
        _threadPool.runNext();

        final ArgumentCaptor<ByteBuffer> dataCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(session, times(2)).sendBinary(dataCaptor.capture(), callbackCaptor.capture());
        assertSame(firstDataCaptor.getValue(), dataCaptor.getAllValues().get(1));
        assertEquals(1, dataCaptor.getAllValues().get(1).remaining());
        assertEquals((byte) 1, dataCaptor.getAllValues().get(1).get(0));
        verify(session, never()).close(anyInt(), isNull(), any(Callback.class));

        callbackCaptor.getAllValues().get(1).succeed();
        _threadPool.runNext();

        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, AmqpPort.DEFAULT_FINAL_WRITE_TIMEOUT})
    void synchronousFinalWritesPrecedeNormalClose(final long closeTimeoutMillis)
    {
        _connectionScheduler = new WebSocketConnectionScheduler(getTestName(), _currentTime::get, _nanoTime::get,
                TimeUnit.MILLISECONDS.toNanos(closeTimeoutMillis));
        final Session session = mock(Session.class);
        doAnswer(invocation ->
        {
            final Callback callback = invocation.getArgument(1);
            callback.succeed();
            return null;
        }).when(session).sendBinary(any(ByteBuffer.class), any(Callback.class));

        final MultiVersionProtocolEngine protocolEngine = mock();
        when(protocolEngine.isProtocolCloseComplete()).thenReturn(true);
        final WebSocketConnection connection = createConnection(protocolEngine, session);
        send(connection, new byte[WRITE_BATCH_SIZE + 1]);

        connection.doWrite();
        _threadPool.runAll();

        final InOrder writes = inOrder(session);
        writes.verify(session, times(2)).sendBinary(any(ByteBuffer.class), any(Callback.class));
        writes.verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
        assertEquals(0, _threadPool.getQueueSize());
    }
}
