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
package org.apache.qpid.tests.protocol.v0_8;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQPConnection_0_8;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class MalformedFrameTest extends BrokerAdminUsingTestBase
{
    private static final byte FRAME_END = (byte) 0xCE;
    private static final int MAX_NESTED_OBJECTS = Integer.getInteger(AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS,
            AMQPConnection_0_8.DEFAULT_CODEC_MAX_NESTED_OBJECTS);

    @Test
    public void secureOkWithOversizedResponseDoesNotCrashBroker() throws Exception
    {
        assertMalformedFrameClosesOnlyConnection(secureOkWithOversizedResponse());
    }

    @Test
    public void startOkWithOversizedLongStringDoesNotCrashBroker() throws Exception
    {
        assertMalformedFrameClosesOnlyConnection(startOkWithOversizedLongString());
    }

    @Test
    public void startOkWithOversizedLongStringInNestedTableDoesNotCrashBroker() throws Exception
    {
        assertMalformedFrameClosesOnlyConnection(startOkWithOversizedLongStringInNestedTable());
    }

    @Test
    public void startOkExceedingNestingLimitClosesWithoutResponse() throws Exception
    {
        assertMalformedFrameClosesOnlyConnection(startOkWithExcessiveNesting());
    }

    private void assertMalformedFrameClosesOnlyConnection(final byte[] malformedFrame) throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                    .consumeResponse(ConnectionStartBody.class)
                    .sendPerformative(rawFrame(malformedFrame))
                    .consumeResponse()
                    .getLatestResponse(ChannelClosedResponse.class);
        }

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ConnectionStartBody start = interaction.negotiateProtocol()
                    .consumeResponse()
                    .getLatestResponse(ConnectionStartBody.class);
            assertThat(start.getVersionMajor(), is(equalTo((short) transport.getProtocolVersion().getMajorVersion())));
        }
    }

    private static AMQDataBlock rawFrame(final byte[] bytes)
    {
        return new AMQDataBlock()
        {
            @Override
            public long getSize()
            {
                return bytes.length;
            }

            @Override
            public long writePayload(final ByteBufferSender sender)
            {
                sender.send(QpidByteBuffer.wrap(bytes));
                return bytes.length;
            }
        };
    }

    private static byte[] secureOkWithOversizedResponse()
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(18))
        {
            writeFrameHeader(buffer, 10);
            buffer.putInt(0x000A0015);              // connection.secure-ok
            buffer.putUnsignedInt(0x20000000L);     // response length
            buffer.putUnsignedShort(4);             // insufficient response bytes
            buffer.put(FRAME_END);
            return readBytes(buffer);
        }
    }

    private static byte[] startOkWithOversizedLongString()
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(44))
        {
            writeFrameHeader(buffer, 36);
            buffer.putInt(0x000A000B);              // connection.start-ok

            buffer.putUnsignedInt(7);               // client-properties field-table length
            buffer.putUnsignedByte((short) 1);
            buffer.put((byte) 's');
            buffer.put((byte) 'S');                 // long-string field value
            buffer.putUnsignedInt(0x80000000L);     // field value length

            writeShortString(buffer, "PLAIN");      // mechanism
            writeLongString(buffer, "guest");       // response
            writeShortString(buffer, "en_US");      // locale
            buffer.put(FRAME_END);
            return readBytes(buffer);
        }
    }

    private static byte[] startOkWithOversizedLongStringInNestedTable()
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(57))
        {
            writeFrameHeader(buffer, 49);
            buffer.putInt(0x000A000B);              // connection.start-ok

            buffer.putUnsignedInt(20);              // client-properties field-table length
            buffer.putUnsignedByte((short) 7);
            buffer.put("ignored".getBytes(US_ASCII));
            buffer.put((byte) 'F');                 // nested field table
            buffer.putUnsignedInt(7);
            buffer.putUnsignedByte((short) 1);
            buffer.put((byte) 's');
            buffer.put((byte) 'S');                 // long-string field value
            buffer.putUnsignedInt(0x80000000L);     // field value length

            writeShortString(buffer, "PLAIN");      // mechanism
            writeLongString(buffer, "guest");       // response
            writeShortString(buffer, "en_US");      // locale
            buffer.put(FRAME_END);
            return readBytes(buffer);
        }
    }

    private static byte[] startOkWithExcessiveNesting()
    {
        final byte[] clientProperties = nestedFieldTable(MAX_NESTED_OBJECTS + 1);
        final int bodySize = Integer.BYTES + Integer.BYTES + clientProperties.length + shortStringSize("PLAIN") +
                longStringSize("guest") + shortStringSize("en_US");
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(8 + bodySize))
        {
            writeFrameHeader(buffer, bodySize);
            buffer.putInt(0x000A000B); // connection.start-ok
            buffer.putUnsignedInt(clientProperties.length);
            buffer.put(clientProperties);
            writeShortString(buffer, "PLAIN");
            writeLongString(buffer, "guest");
            writeShortString(buffer, "en_US");
            buffer.put(FRAME_END);
            return readBytes(buffer);
        }
    }

    private static byte[] nestedFieldTable(final int depth)
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(7 * depth))
        {
            for (int remainingDepth = depth; remainingDepth > 1; remainingDepth--)
            {
                buffer.putUnsignedByte((short) 1);
                buffer.put((byte) 'n');
                buffer.put((byte) 'F');
                buffer.putUnsignedInt(7L * (remainingDepth - 1));
            }
            buffer.putUnsignedByte((short) 1);
            buffer.put((byte) 'v');
            buffer.put((byte) 'I');
            buffer.putInt(42);
            return readBytes(buffer);
        }
    }

    private static void writeFrameHeader(final QpidByteBuffer buffer, final long bodySize)
    {
        buffer.put((byte) 1);
        buffer.putUnsignedShort(0); // connection methods use channel zero
        buffer.putUnsignedInt(bodySize);
    }

    private static void writeShortString(final QpidByteBuffer buffer, final String value)
    {
        final byte[] bytes = value.getBytes(US_ASCII);
        buffer.putUnsignedByte((short) bytes.length);
        buffer.put(bytes);
    }

    private static void writeLongString(final QpidByteBuffer buffer, final String value)
    {
        final byte[] bytes = value.getBytes(US_ASCII);
        buffer.putUnsignedInt(bytes.length);
        buffer.put(bytes);
    }

    private static int shortStringSize(final String value)
    {
        return Byte.BYTES + value.getBytes(US_ASCII).length;
    }

    private static int longStringSize(final String value)
    {
        return Integer.BYTES + value.getBytes(US_ASCII).length;
    }

    private static byte[] readBytes(final QpidByteBuffer buffer)
    {
        buffer.flip();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
