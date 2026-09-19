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
package org.apache.qpid.server.protocol.v0_8.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.AMQType;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.test.utils.UnitTestBase;

class ClientFieldTableBodyDisposalTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 64;

    @BeforeEach
    void setUp()
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, 20, 1.0);
    }

    @AfterEach
    void tearDown()
    {
        QpidByteBuffer.deinitialisePool();
    }

    @Test
    void connectionStartDisposesServerPropertiesWhenLaterFieldDecodeFails()
    {
        final ClientMethodProcessor<ClientChannelMethodProcessor> dispatcher = mock(ClientMethodProcessor.class);
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        in.putUnsignedByte((short) 0);               // version major
        in.putUnsignedByte((short) 9);               // version minor
        writeValidFieldTable(in);
        in.putInt(0x20000000);                       // mechanisms length
        in.flip();

        assertThrows(IllegalArgumentException.class, () -> ConnectionStartBody.process(in, dispatcher));

        in.dispose();
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Malformed connection.start leaked server properties");
    }

    @Test
    void channelAlertDisposesDetailsWhenProcessorFails()
    {
        final ClientChannelMethodProcessor dispatcher = mock(ClientChannelMethodProcessor.class);
        doThrow(new IllegalStateException("processor failure"))
                .when(dispatcher)
                .receiveChannelAlert(anyInt(), any(AMQShortString.class), any(FieldTable.class));

        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        in.putUnsignedShort(500);
        in.putUnsignedByte((short) 1);
        in.put((byte) 'x');
        writeValidFieldTable(in);
        in.flip();

        assertThrows(IllegalStateException.class, () -> ChannelAlertBody.process(in, dispatcher));

        in.dispose();
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "A channel.alert processor failure leaked details");
    }

    private static void writeValidFieldTable(final QpidByteBuffer buffer)
    {
        buffer.putInt(3);
        buffer.putUnsignedByte((short) 1);
        buffer.put((byte) 'x');
        buffer.put(AMQType.VOID.identifier());
    }
}
