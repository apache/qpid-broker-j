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
package org.apache.qpid.server.protocol.v0_10.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class CompoundNestingDecoderTest extends UnitTestBase
{
    private static final int ATTACK_DEPTH = 10_000;

    @Test
    void excessiveNestingRejectedAfterBoundedDecode()
    {
        final ByteBuffer encoded = nestedLists(ATTACK_DEPTH);
        final BBDecoder decoder = createDecoder(encoded, AbstractDecoder.DEFAULT_MAX_NESTED_OBJECTS);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, decoder::readList);

        assertEquals("Maximum type nesting depth (" + AbstractDecoder.DEFAULT_MAX_NESTED_OBJECTS + ") exceeded",
                exception.getMessage());
        assertTrue(encoded.position() < 1_024, "Decoder must reject without traversing the complete attack value");
    }

    @Test
    void configuredLimitAcceptsNestingAtLimit()
    {
        final BBDecoder decoder = createDecoder(nestedLists(3), 3);

        assertEquals(List.of(List.of(List.of())), decoder.readList());
    }

    @Test
    void zeroLimitAllowsScalarButRejectsCompound()
    {
        final BBDecoder decoder = new BBDecoder(AbstractDecoder.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, 0);
        decoder.init(ByteBuffer.wrap(new byte[] {42}));

        assertEquals(42, decoder.readUint8());

        decoder.init(nestedLists(1));
        assertThrows(IllegalArgumentException.class, decoder::readList);
    }

    @Test
    void negativeLimitRejected()
    {
        assertThrows(IllegalArgumentException.class, () ->
                new BBDecoder(AbstractDecoder.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, -1));
    }

    @Test
    void nestingDepthRestoredAfterDecodeFailure()
    {
        final BBDecoder decoder = createDecoder(nestedLists(2), 1);

        assertThrows(IllegalArgumentException.class, decoder::readList);

        decoder.init(nestedLists(1));
        assertEquals(List.of(), decoder.readList());
    }

    @Test
    void arraysAndListsShareNestingLimit()
    {
        final BBDecoder rejectingDecoder = createDecoder(arrayContainingEmptyList(), 1);
        assertThrows(IllegalArgumentException.class, rejectingDecoder::readArray);

        final BBDecoder acceptingDecoder = createDecoder(arrayContainingEmptyList(), 2);
        assertEquals(List.of(List.of()), acceptingDecoder.readArray());
    }

    @Test
    void struct32AndMapShareNestingLimit()
    {
        final BBDecoder rejectingDecoder = createDecoder(messagePropertiesWithApplicationHeaders(), 1);
        assertThrows(IllegalArgumentException.class, rejectingDecoder::readStruct32);

        final BBDecoder acceptingDecoder = createDecoder(messagePropertiesWithApplicationHeaders(), 2);
        assertInstanceOf(MessageProperties.class, acceptingDecoder.readStruct32());
    }

    @Test
    void typedStructAndStruct32ShareNestingLimit()
    {
        final BBDecoder rejectingDecoder = createDecoder(messagePropertiesWithReplyTo(), 1);
        assertThrows(IllegalArgumentException.class, rejectingDecoder::readStruct32);

        final BBDecoder acceptingDecoder = createDecoder(messagePropertiesWithReplyTo(), 2);
        assertInstanceOf(MessageProperties.class, acceptingDecoder.readStruct32());
    }

    private static BBDecoder createDecoder(final ByteBuffer encoded, final int maxNestedObjects)
    {
        final BBDecoder decoder = new BBDecoder(AbstractDecoder.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, maxNestedObjects);
        decoder.init(encoded);
        return decoder;
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

    private static ByteBuffer arrayContainingEmptyList()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(17);
        encoded.putInt(13);
        encoded.put(Type.LIST.getCode());
        encoded.putInt(1);
        encoded.putInt(4);
        encoded.putInt(0);
        encoded.flip();
        return encoded;
    }

    private static ByteBuffer messagePropertiesWithApplicationHeaders()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(16);
        encoded.putInt(12);
        encoded.putShort((short) MessageProperties.TYPE);
        encoded.putShort((short) 1);
        encoded.putInt(4);
        encoded.putInt(0);
        encoded.flip();
        return encoded;
    }

    private static ByteBuffer messagePropertiesWithReplyTo()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(12);
        encoded.putInt(8);
        encoded.putShort((short) MessageProperties.TYPE);
        encoded.putShort((short) 2048);
        encoded.putShort((short) 2);
        encoded.putShort((short) 0);
        encoded.flip();
        return encoded;
    }
}
