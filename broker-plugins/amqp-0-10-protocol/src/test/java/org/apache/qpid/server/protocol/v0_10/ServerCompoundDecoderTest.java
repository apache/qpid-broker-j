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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStartOk;
import org.apache.qpid.server.protocol.v0_10.transport.Type;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerCompoundDecoderTest extends UnitTestBase
{
    private static final int AMPLIFIED_ELEMENT_COUNT = 100_000;

    @Test
    void connectionStartOkRejectsAmplifiedVoidArray()
    {
        final byte[] key = "x".getBytes(StandardCharsets.UTF_8);
        final int mapEntrySize = 1 + key.length + 1 + 4 + 1 + 4;
        final ByteBuffer encoded = ByteBuffer.allocate(2 + 4 + 4 + mapEntrySize);
        encoded.putShort((short) 0x0100);
        encoded.putInt(4 + mapEntrySize);
        encoded.putInt(1);
        encoded.put((byte) key.length);
        encoded.put(key);
        encoded.put(Type.ARRAY.getCode());
        encoded.putInt(5);
        encoded.put(Type.VOID.getCode());
        encoded.putInt(AMPLIFIED_ELEMENT_COUNT);

        try (final QpidByteBuffer buffer = QpidByteBuffer.wrap(encoded.array()))
        {
            final ServerDecoder decoder = new ServerDecoder(buffer);
            final ConnectionStartOk startOk = new ConnectionStartOk();

            assertThrows(IllegalArgumentException.class, () -> startOk.read(decoder));
        }
    }

    @Test
    void nestedCompoundCannotConsumeFollowingItemAcrossBufferFragments()
    {
        final byte[] encoded = createNestedArrayList();
        try (final QpidByteBuffer first = QpidByteBuffer.wrap(Arrays.copyOfRange(encoded, 0, 10));
             final QpidByteBuffer second = QpidByteBuffer.wrap(Arrays.copyOfRange(encoded, 10, encoded.length));
             final QpidByteBuffer combined = QpidByteBuffer.concatenate(first, second))
        {
            final ServerDecoder decoder = new ServerDecoder(combined);

            assertThrows(IllegalArgumentException.class, decoder::readList);
            assertEquals(18, combined.position(), "Following list item must remain unread");
            assertEquals(Type.VOID.getCode(), combined.get(combined.position()));
        }
    }

    private byte[] createNestedArrayList()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(19);
        encoded.putInt(15);
        encoded.putInt(2);
        encoded.put(Type.ARRAY.getCode());
        encoded.putInt(5);
        encoded.put(Type.UINT8.getCode());
        encoded.putInt(1);
        encoded.put(Type.VOID.getCode());
        return encoded.array();
    }
}
