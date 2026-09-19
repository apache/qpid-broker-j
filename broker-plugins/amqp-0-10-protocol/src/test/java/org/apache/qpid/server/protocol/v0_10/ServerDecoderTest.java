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

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionSecureOk;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStartOk;
import org.apache.qpid.server.protocol.v0_10.transport.Type;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerDecoderTest extends UnitTestBase
{
    private static final int OVERSIZED_LENGTH = 1_000_000;

    @Test
    void connectionStartOkRejectsForgedResponseLength()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
        encoded.putShort((short) 1024);
        encoded.putInt(OVERSIZED_LENGTH);

        assertMalformed(encoded, decoder -> new ConnectionStartOk().read(decoder));
    }

    @Test
    void connectionStartOkRejectsForgedClientPropertyLength()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(Short.BYTES + 3 * Integer.BYTES + 3);
        encoded.putShort((short) 256);
        encoded.putInt(2 * Integer.BYTES + 3);
        encoded.putInt(1);
        encoded.put((byte) 1);
        encoded.put((byte) 'x');
        encoded.put(Type.VBIN32.getCode());
        encoded.putInt(OVERSIZED_LENGTH);

        assertMalformed(encoded, decoder -> new ConnectionStartOk().read(decoder));
    }

    @Test
    void connectionSecureOkRejectsUnsignedResponseLengthGreaterThanIntegerMax()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
        encoded.putShort((short) 256);
        encoded.putInt(-1);

        assertMalformed(encoded, decoder -> new ConnectionSecureOk().read(decoder));
    }

    private void assertMalformed(final ByteBuffer encoded, final Consumer<ServerDecoder> decode)
    {
        try (final QpidByteBuffer input = QpidByteBuffer.wrap(encoded.array()))
        {
            final ServerDecoder decoder = new ServerDecoder(input);
            assertThrows(IllegalArgumentException.class, () -> decode.accept(decoder));
        }
    }
}
