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
package org.apache.qpid.server.protocol.v0_10;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStartOk;
import org.apache.qpid.server.protocol.v0_10.transport.Frame;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolEvent;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;
import org.apache.qpid.server.protocol.v0_10.transport.Type;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerAssemblerNestingTest extends UnitTestBase
{
    private static final int MAX_NESTED_OBJECTS = 2;

    @Test
    void segmentedConnectionStartOkRejectedAboveConfiguredLimit()
    {
        final ServerConnection connection = mockConnection();
        final ServerAssembler assembler = new ServerAssembler(connection, Integer.MAX_VALUE, Integer.MAX_VALUE);
        final ServerFrame[] frames = segmentedFrames(connectionStartOkWithNestedLists(2));

        assembler.frame(frames[0]);
        assertThrows(IllegalArgumentException.class, () -> assembler.frame(frames[1]));

        verify(connection, never()).received(any(ProtocolEvent.class));
    }

    @Test
    void segmentedConnectionStartOkAcceptedAtConfiguredLimit()
    {
        final ServerConnection connection = mockConnection();
        final ServerAssembler assembler = new ServerAssembler(connection, Integer.MAX_VALUE, Integer.MAX_VALUE);
        final ServerFrame[] frames = segmentedFrames(connectionStartOkWithNestedLists(1));

        assembler.frame(frames[0]);
        assembler.frame(frames[1]);

        verify(connection).received(isA(ConnectionStartOk.class));
    }

    private static ServerConnection mockConnection()
    {
        final ServerConnection connection = mock(ServerConnection.class);
        final AMQPConnection_0_10Impl amqpConnection = mock(AMQPConnection_0_10Impl.class);
        when(connection.getAmqpConnection()).thenReturn(amqpConnection);
        when(connection.getMaxMessageSize()).thenReturn(Integer.MAX_VALUE);
        when(amqpConnection.getMaxNestedObjects()).thenReturn(MAX_NESTED_OBJECTS);
        return connection;
    }

    private static ServerFrame[] segmentedFrames(final byte[] encoded)
    {
        final int split = encoded.length / 2;
        final byte commonFlags = (byte) (ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG);
        final ServerFrame first = new ServerFrame((byte) (commonFlags | ServerFrame.FIRST_FRAME), SegmentType.COMMAND,
                Frame.L1, 0, QpidByteBuffer.wrap(Arrays.copyOfRange(encoded, 0, split)));
        final ServerFrame last = new ServerFrame((byte) (commonFlags | ServerFrame.LAST_FRAME), SegmentType.COMMAND,
                Frame.L1, 0, QpidByteBuffer.wrap(Arrays.copyOfRange(encoded, split, encoded.length)));
        return new ServerFrame[] {first, last};
    }

    private static byte[] connectionStartOkWithNestedLists(final int listDepth)
    {
        final ByteBuffer nestedLists = nestedLists(listDepth);
        final int mapSize = Integer.BYTES + Byte.BYTES + Byte.BYTES + Byte.BYTES + nestedLists.remaining();
        final ByteBuffer encoded =
                ByteBuffer.allocate(Short.BYTES + Short.BYTES + Short.BYTES + Integer.BYTES + mapSize);
        encoded.putShort((short) ConnectionStartOk.TYPE);
        encoded.putShort((short) 0x0100);
        encoded.putShort((short) 0x0100);
        encoded.putInt(mapSize);
        encoded.putInt(1);
        encoded.put((byte) 1);
        encoded.put((byte) 'x');
        encoded.put(Type.LIST.getCode());
        encoded.put(nestedLists);
        return encoded.array();
    }

    private static ByteBuffer nestedLists(final int depth)
    {
        final ByteBuffer encoded = ByteBuffer.allocate(8 + 9 * (depth - 1));
        for (int remainingDepth = depth; remainingDepth > 1; remainingDepth--)
        {
            encoded.putInt(4 + 9 * (remainingDepth - 1));
            encoded.putInt(1);
            encoded.put(Type.LIST.getCode());
        }
        encoded.putInt(4);
        encoded.putInt(0);
        encoded.flip();
        return encoded;
    }
}
