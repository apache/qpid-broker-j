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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

class WriteJobTest extends WebSocketTestBase
{
    @Test
    void writeSubmissionsAreCoalescedWhileQueued()
    {
        final Session session = mock(Session.class);
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
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

        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
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

        _threadPool.runAll();

        assertEquals(4, messages.size());
        assertArrayEquals(new byte[] {1}, messages.get(0));
        assertArrayEquals(new byte[] {2}, messages.get(1));
        assertArrayEquals(new byte[] {3}, messages.get(2));
        assertArrayEquals(new byte[] {4, 5}, messages.get(3));
    }

    @Test
    void writeStorageIsReusedOnlyAfterCompletion()
    {
        final Session session = mock(Session.class);
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
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
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
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
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[] {1}))
        {
            connection.send(buffer);
        }
        _connectionScheduler.registerConnection(connection);
        _threadPool.rejectNext();

        connection.doWrite();

        assertTrue(connection.isForceClosing());
        assertEquals(0, _threadPool.getQueueSize());
        assertEquals(1_000L, scheduleDueConnections());
        verify(session).disconnect();
    }

    @Test
    void finalWritesAreBoundedAndSentOneAtATime()
    {
        final byte[] expected = new byte[WRITE_BATCH_SIZE + 1];
        expected[WRITE_BATCH_SIZE] = 1;
        final Session session = mock(Session.class);
        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
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

    @Test
    void synchronousWriteCompletionSchedulesRemainingOutput()
    {
        final Session session = mock(Session.class);
        doAnswer(invocation ->
        {
            final Callback callback = invocation.getArgument(1);
            callback.succeed();
            return null;
        }).when(session).sendBinary(any(ByteBuffer.class), any(Callback.class));

        final WebSocketConnection connection = createConnection(new MutableTicker(37_000), session);
        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(new byte[WRITE_BATCH_SIZE + 1]))
        {
            connection.send(buffer);
        }
        connection.close();

        connection.doWrite();
        _threadPool.runAll();

        verify(session, times(2)).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session).close(eq(StatusCode.NORMAL), isNull(), any(Callback.class));
        assertEquals(0, _threadPool.getQueueSize());
    }
}
