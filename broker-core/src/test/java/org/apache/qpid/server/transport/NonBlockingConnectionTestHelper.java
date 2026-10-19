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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.BooleanSupplier;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.AmqpPort;

final class NonBlockingConnectionTestHelper
{
    private NonBlockingConnectionTestHelper()
    {
    }

    static AmqpPort<?> createPort(final int bufferSize, final long finalWriteTimeout)
    {
        final Broker<?> broker = mock(Broker.class);
        when(broker.getEventLogger()).thenReturn(new EventLogger());
        return createPort(broker, bufferSize, finalWriteTimeout);
    }

    static AmqpPort<?> createPort(final Broker<?> broker, final int bufferSize, final long finalWriteTimeout)
    {
        final AmqpPort<?> port = mock(AmqpPort.class);
        doReturn(broker).when(port).getParent();
        when(port.getNetworkBufferSize()).thenReturn(bufferSize);
        when(port.getContextValue(Integer.class, AmqpPort.FINAL_WRITE_THRESHOLD)).thenReturn(1);
        when(port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT)).thenReturn(finalWriteTimeout);
        when(port.getContextValue(Integer.class, AmqpPort.MAX_GATHERING_WRITE_BUFFERS)).thenReturn(1024);
        return port;
    }

    static SocketChannel createSocketChannel(final BooleanSupplier writable) throws IOException
    {
        final Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("localhost", 10000));
        when(socket.getLocalSocketAddress()).thenReturn(new InetSocketAddress("localhost", 5672));
        final SocketChannel channel = mock(SocketChannel.class);
        when(channel.socket()).thenReturn(socket);
        when(channel.write(any(ByteBuffer[].class))).thenAnswer(invocation ->
        {
            long written = 0;
            if (writable.getAsBoolean())
            {
                for (final ByteBuffer buffer : (ByteBuffer[]) invocation.getArgument(0))
                {
                    written += buffer.remaining();
                    buffer.position(buffer.limit());
                }
            }
            return written;
        });
        return channel;
    }
}
