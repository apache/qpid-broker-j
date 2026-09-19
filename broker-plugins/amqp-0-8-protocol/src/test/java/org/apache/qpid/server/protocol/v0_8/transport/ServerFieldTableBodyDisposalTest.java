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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerFieldTableBodyDisposalTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 64;

    @FunctionalInterface
    private interface Decoder
    {
        void process(final QpidByteBuffer buffer, final ServerChannelMethodProcessor dispatcher) throws Exception;
    }

    @FunctionalInterface
    private interface ArgWriter
    {
        void write(final QpidByteBuffer buffer);
    }

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
    void basicConsumeDisposesArgumentsOnMalformedTable()
    {
        assertNoLeak(buffer ->
        {
            buffer.putShort((short) 0);           // ticket
            writeShortString(buffer, "q");        // queue
            writeShortString(buffer, "tag");      // consumerTag
            buffer.put((byte) 0);                 // bitfield
            writeMalformedFieldTable(buffer);
        }, BasicConsumeBody::process);
    }

    @Test
    void exchangeDeclareDisposesArgumentsOnMalformedTable()
    {
        assertNoLeak(buffer ->
        {
            buffer.putShort((short) 0);           // ticket
            writeShortString(buffer, "ex");       // exchange
            writeShortString(buffer, "direct");   // type
            buffer.put((byte) 0);                 // bitfield
            writeMalformedFieldTable(buffer);
        }, ExchangeDeclareBody::process);
    }

    @Test
    void queueDeclareDisposesArgumentsOnMalformedTable()
    {
        assertNoLeak(buffer ->
        {
            buffer.putShort((short) 0);           // ticket
            writeShortString(buffer, "q");        // queue
            buffer.put((byte) 0);                 // bitfield
            writeMalformedFieldTable(buffer);
        }, QueueDeclareBody::process);
    }

    @Test
    void queueBindDisposesArgumentsOnMalformedTable()
    {
        assertNoLeak(buffer ->
        {
            buffer.putShort((short) 0);           // ticket
            writeShortString(buffer, "q");        // queue
            writeShortString(buffer, "ex");       // exchange
            writeShortString(buffer, "rk");       // bindingKey
            buffer.put((byte) 0);                 // nowait
            writeMalformedFieldTable(buffer);
        }, QueueBindBody::process);
    }

    @Test
    void queueUnbindDisposesArgumentsOnMalformedTable()
    {
        assertNoLeak(buffer ->
        {
            buffer.putShort((short) 0);           // ticket
            writeShortString(buffer, "q");        // queue
            writeShortString(buffer, "ex");       // exchange
            writeShortString(buffer, "rk");       // routingKey
            writeMalformedFieldTable(buffer);
        }, QueueUnbindBody::process);
    }

    private void assertNoLeak(final ArgWriter argWriter, final Decoder decoder)
    {
        final ServerChannelMethodProcessor dispatcher = mock(ServerChannelMethodProcessor.class);
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        argWriter.write(in);
        in.flip();

        assertThrows(IllegalArgumentException.class, () -> decoder.process(in, dispatcher));

        in.dispose();
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Malformed field-table argument leaked a pooled direct buffer");
    }

    private static void writeShortString(final QpidByteBuffer buffer, final String value)
    {
        final byte[] bytes = value.getBytes(US_ASCII);
        buffer.putUnsignedByte((short) bytes.length);
        buffer.put(bytes);
    }

    // field table (length 7): entry key "s", value type 'S', long-string length 0x80000000
    private static void writeMalformedFieldTable(final QpidByteBuffer buffer)
    {
        buffer.putInt(7);
        buffer.putUnsignedByte((short) 1);
        buffer.put((byte) 's');
        buffer.put((byte) 'S');
        buffer.putInt(0x80000000);
    }
}
