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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import org.apache.qpid.test.utils.UnitTestBase;

class CompoundDecoderTest extends UnitTestBase
{
    private static final int AMPLIFIED_ELEMENT_COUNT = 100_000;

    @Test
    void baseUint32DecoderPreservesUnsignedValue()
    {
        final AbstractDecoder decoder = mock(AbstractDecoder.class, withSettings().useConstructor()
                .defaultAnswer(Answers.CALLS_REAL_METHODS));
        when(decoder.doGet()).thenReturn((byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF);

        assertEquals(0xFFFFFFFFL, decoder.readUint32());
    }

    @Test
    void zeroWidthArrayAboveDefaultLimitRejectedBeforeLoop()
    {
        final ByteBuffer encoded = createArray(5, Type.VOID, AMPLIFIED_ELEMENT_COUNT);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readArray);
        assertEquals(9, encoded.position(), "Element decoding must not start");
    }

    @Test
    void unsignedArrayCountIsRejectedBeforeNarrowing()
    {
        final ByteBuffer encoded = createArray(5, Type.VOID, -1);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readArray);
        assertEquals(9, encoded.position(), "Element decoding must not start");
    }

    @Test
    void configuredZeroWidthArrayLimitAllowsConfiguredCount()
    {
        final ByteBuffer encoded = createArray(5, Type.VOID, 2);
        final BBDecoder decoder = new BBDecoder(2, AbstractDecoder.DEFAULT_MAX_NESTED_OBJECTS);
        decoder.init(encoded);

        final List<Object> result = decoder.readArray();

        assertEquals(2, result.size());
        assertNull(result.get(0));
        assertNull(result.get(1));
    }

    @Test
    void bitArrayIsSubjectToZeroWidthLimit()
    {
        final BBDecoder decoder = createDecoder(createArray(5, Type.BIT, 1));

        assertThrows(IllegalArgumentException.class, decoder::readArray);
    }

    @Test
    void fixedWidthArrayCountIsValidatedBeforeElementDecoding()
    {
        final ByteBuffer encoded = createArray(6, Type.UINT8, 2, (byte) 1);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readArray);
        assertEquals(9, encoded.position(), "Element decoding must not start");
    }

    @Test
    void validFixedWidthArrayIsDecoded()
    {
        final BBDecoder decoder = createDecoder(createArray(9, Type.UINT16, 2, (byte) 0x01, (byte) 0x02,
                (byte) 0x03, (byte) 0x04));

        assertEquals(List.of(0x0102, 0x0304), decoder.readArray());
    }

    @Test
    void listCountIsBoundedByEncodedItems()
    {
        final ByteBuffer encoded = createCountedCompound(4, AMPLIFIED_ELEMENT_COUNT);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readList);
        assertEquals(8, encoded.position(), "Item decoding must not start");
    }

    @Test
    void mapCountIsBoundedByEncodedEntries()
    {
        final ByteBuffer encoded = createCountedCompound(4, AMPLIFIED_ELEMENT_COUNT);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readMap);
        assertEquals(8, encoded.position(), "Entry decoding must not start");
    }

    @Test
    void compoundSizeCannotExceedRemainingBytes()
    {
        final ByteBuffer encoded = createArray(6, Type.VOID, 0);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readArray);
        assertEquals(4, encoded.position(), "Compound body must not be read");
    }

    @Test
    void compoundRequiresExactConsumption()
    {
        final ByteBuffer encoded = createArray(6, Type.VOID, 0, (byte) 0x7F);
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readArray);
        assertEquals(9, encoded.position(), "Trailing data must remain unread");
    }

    @Test
    void nestedCompoundCannotConsumeFollowingListItem()
    {
        final ByteBuffer encoded = ByteBuffer.allocate(19);
        encoded.putInt(15);
        encoded.putInt(2);
        encoded.put(Type.ARRAY.getCode());
        encoded.putInt(5);
        encoded.put(Type.UINT8.getCode());
        encoded.putInt(1);
        encoded.put(Type.VOID.getCode());
        encoded.flip();
        final BBDecoder decoder = createDecoder(encoded);

        assertThrows(IllegalArgumentException.class, decoder::readList);
        assertEquals(18, encoded.position(), "Following list item must remain unread");
        assertEquals(Type.VOID.getCode(), encoded.get(encoded.position()));
    }

    private BBDecoder createDecoder(final ByteBuffer encoded)
    {
        final BBDecoder decoder = new BBDecoder();
        decoder.init(encoded);
        return decoder;
    }

    private ByteBuffer createArray(final int size, final Type type, final int count, final byte... values)
    {
        final ByteBuffer encoded = ByteBuffer.allocate(9 + values.length);
        encoded.putInt(size);
        encoded.put(type.getCode());
        encoded.putInt(count);
        encoded.put(values);
        encoded.flip();
        return encoded;
    }

    private ByteBuffer createCountedCompound(final int size, final int count)
    {
        final ByteBuffer encoded = ByteBuffer.allocate(8);
        encoded.putInt(size);
        encoded.putInt(count);
        encoded.flip();
        return encoded;
    }
}
