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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionExceptionTest extends UnitTestBase
{
    private AmqpPort<?> _port;
    private NetworkConnectionScheduler _scheduler;

    @BeforeEach
    void setUp()
    {
        final Broker<?> broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        _port = mock(AmqpPort.class);
        doReturn(broker).when(_port).getParent();
        when(_port.getNetworkBufferSize()).thenReturn(128);
        when(_port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(100);
        when(_port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(100L);
        when(_port.getContextValue(Integer.class, AmqpPort.MAX_GATHERING_WRITE_BUFFERS)).thenReturn(1024);
        _scheduler = mock(NetworkConnectionScheduler.class);
    }

    @Test
    void testContainedParsingFailureClosesOnlyItsConnection() throws Exception
    {
        final ProtocolEngine failingEngine = mock(ProtocolEngine.class);
        final ProtocolEngine healthyEngine = mock(ProtocolEngine.class);
        final SocketChannel failingChannel = mock(SocketChannel.class);
        final SocketChannel healthyChannel = mock(SocketChannel.class);
        final NonBlockingConnection failing = createConnection(failingEngine, failingChannel);
        final NonBlockingConnection healthy = createConnection(healthyEngine, healthyChannel);
        final ArithmeticException failure = new ArithmeticException("Injected decoding failure");
        doThrow(new ConnectionScopedRuntimeException(failure)).when(failingEngine).received(any(QpidByteBuffer.class));

        try
        {
            assertTrue(failing.doWork());
            verify(failingEngine).closed();
            verify(failingChannel).close();
            verify(_scheduler).removeConnection(failing);

            assertFalse(healthy.doWork());
            verify(healthyEngine).received(any(QpidByteBuffer.class));
        }
        finally
        {
            failing.close();
            failing.doWork();
            healthy.close();
            healthy.doWork();
        }
    }

    private NonBlockingConnection createConnection(final ProtocolEngine engine, final SocketChannel channel)
            throws Exception
    {
        final Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("localhost", 1000));
        when(socket.getLocalSocketAddress()).thenReturn(new InetSocketAddress("localhost", 5672));
        when(channel.socket()).thenReturn(socket);
        when(channel.read(any(ByteBuffer.class))).thenAnswer(invocation ->
        {
            final ByteBuffer input = invocation.getArgument(0);
            input.put((byte) 0);
            return 1;
        });
        when(engine.getAggregateTicker()).thenReturn(new AggregateTicker());
        when(engine.processPendingIterator()).thenAnswer(invocation -> Collections.emptyIterator());
        final NonBlockingConnection connection = new NonBlockingConnection(channel, engine,
                Set.of(TransportEncryption.NONE), mock(Runnable.class), _scheduler, _port);
        connection.setSelectionTask(mock(SelectorThread.SelectionTask.class));
        return connection;
    }
}
