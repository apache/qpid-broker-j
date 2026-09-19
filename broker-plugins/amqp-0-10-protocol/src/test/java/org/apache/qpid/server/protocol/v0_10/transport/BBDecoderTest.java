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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.test.utils.UnitTestBase;

class BBDecoderTest extends UnitTestBase
{
    private static final int OVERSIZED_LENGTH = 1_000_000;

    @Test
    void str8Caching()
    {
        final String testString = "Test";
        final BBEncoder encoder = new BBEncoder(64);
        encoder.writeStr8(testString);
        encoder.writeStr8(testString);
        final ByteBuffer buffer = encoder.buffer();

        final BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);
        final Cache<Binary, String> original  = BBDecoder.getStringCache();
        final Cache<Binary, String> cache = Caffeine.newBuilder().maximumSize(2).build();
        try
        {
            BBDecoder.setStringCache(cache);

            final String decodedString1 = decoder.readStr8();
            final String decodedString2 = decoder.readStr8();

            assertThat(testString, is(equalTo(decodedString1)));
            assertThat(testString, is(equalTo(decodedString2)));
            assertSame(decodedString1, decodedString2);
        }
        finally
        {
            cache.cleanUp();
            BBDecoder.setStringCache(original);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {OVERSIZED_LENGTH, -1})
    void vbin32RejectsLengthGreaterThanRemaining(final int encodedLength)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(encodedLength);
        final BBDecoder decoder = createDecoder(buffer);

        assertThrows(IllegalArgumentException.class, decoder::readVbin32);
    }

    @Test
    void vbin32ReadsValidValue()
    {
        final byte[] value = {1, 2, 3};
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + value.length);
        buffer.putInt(value.length);
        buffer.put(value);
        final BBDecoder decoder = createDecoder(buffer);

        assertArrayEquals(value, decoder.readVbin32());
    }

    @Test
    void vbin32ReadsEmptyValue()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(0);
        final BBDecoder decoder = createDecoder(buffer);

        assertThat(decoder.readVbin32().length, is(equalTo(0)));
    }

    @Test
    void str16RejectsLengthGreaterThanRemaining()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort((short) 64);
        final BBDecoder decoder = createDecoder(buffer);

        assertThrows(IllegalArgumentException.class, decoder::readStr16);
    }

    @Test
    void listRejectsNestedVbin32LengthGreaterThanRemaining()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(3 * Integer.BYTES + 1);
        buffer.putInt(2 * Integer.BYTES + 1);
        buffer.putInt(1);
        buffer.put(Type.VBIN32.getCode());
        buffer.putInt(OVERSIZED_LENGTH);
        final BBDecoder decoder = createDecoder(buffer);

        assertThrows(IllegalArgumentException.class, decoder::readList);
    }

    private BBDecoder createDecoder(final ByteBuffer buffer)
    {
        buffer.flip();
        final BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);
        return decoder;
    }
}
