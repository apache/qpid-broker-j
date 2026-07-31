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

package org.apache.qpid.server.protocol.v1_0.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

class CountValidationArrayTest extends CountValidationTest
{
    private static final byte ARRAY32 = (byte) 0xF0;
    private static final byte ARRAY8 = (byte) 0xE0;
    private static final byte NULL = 0x40;
    private static final byte BOOLEAN_TRUE = 0x41;
    private static final byte BOOLEAN_FALSE = 0x42;
    private static final byte UINT0 = 0x43;
    private static final byte ULONG0 = 0x44;
    private static final byte LIST0 = 0x45;
    private static final byte BOOLEAN = 0x56;
    private static final byte UBYTE = 0x50;
    private static final byte[] ZERO_WIDTH_CONSTRUCTORS = {NULL, BOOLEAN_TRUE, BOOLEAN_FALSE, UINT0, ULONG0, LIST0};
    private static final byte[] NON_NULL_ZERO_WIDTH_CONSTRUCTORS = {BOOLEAN_TRUE, BOOLEAN_FALSE, UINT0, ULONG0, LIST0};

    @Test
    void array32NegativeSize()
    {
        final byte[] bytes =
        {
                ARRAY32,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, // size = -1
                0x00, 0x00, 0x00, 0x01 // count = 1
        };
        assertDecodeError(bytes);
    }

    @Test
    void array32NegativeCount()
    {
        final byte[] bytes =
        {
                ARRAY32,
                0x00, 0x00, 0x00, 0x0A, // size = 10
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // count = -1
        };
        assertDecodeError(bytes);
    }

    @Test
    void array32InsufficientElementData()
    {
        final byte[] bytes =
        {
                ARRAY32,
                0x00, 0x00, 0x00, 0x04,
                0x0B, (byte) 0xEB, (byte) 0xC2, 0x00
        };
        assertDecodeError(bytes);
    }

    @Test
    void array32NonZeroWidthCountWithoutElementData()
    {
        assertDecodeError(createArray32(200_000_000, UBYTE));
    }

    @Test
    void array8ValidSingleElement() throws AmqpErrorException
    {
        final byte[] bytes =
        {
                ARRAY8,
                0x03, // size = 3
                0x01, // count = 1
                UBYTE, // element constructor: ubyte
                0x42 // value = 66
        };
        final QpidByteBuffer qbb = QpidByteBuffer.wrap(bytes);
        final Object result = _valueHandler.parse(qbb);
        assertNotNull(result);
    }

    @Test
    void zeroWidthArrayAtDefaultLimitParses() throws AmqpErrorException
    {
        final Object result = parse(newValueHandler(ValueHandler.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS),
                createArray32(ValueHandler.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, BOOLEAN_TRUE));

        assertNull(result);
    }

    @Test
    void zeroWidthArrayAboveDefaultLimitFails()
    {
        assertDecodeError(createArray32(ValueHandler.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS + 1, BOOLEAN_TRUE));
    }

    @Test
    void nonZeroWidthArrayAboveDefaultZeroWidthLimitStillParses() throws AmqpErrorException
    {
        final int count = 1025;
        final Object result = parse(newValueHandler(ValueHandler.DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS),
                createArray32(count, BOOLEAN, repeat(count, (byte) 0x01)));

        assertEquals(count, ((Object[]) result).length);
    }

    @Test
    void configuredValueHandlerZeroWidthArrayLimitAllowsConfiguredCount() throws AmqpErrorException
    {
        final ValueHandler valueHandler = newValueHandler(2);

        final Object result = parse(valueHandler, createArray32(2, BOOLEAN_TRUE));
        assertEquals(2, ((Object[]) result).length);

        assertDecodeError(() -> parse(valueHandler, createArray32(3, BOOLEAN_TRUE)));
    }

    @Test
    void configuredValueHandlerZeroWidthNullArrayLimitAllowsConfiguredCount() throws AmqpErrorException
    {
        final ValueHandler valueHandler = newValueHandler(2);

        final Object result = parse(valueHandler, createArray32(2, NULL));
        final Object[] array = (Object[]) result;

        assertEquals(2, array.length);
        assertNull(array[0]);
        assertNull(array[1]);
    }

    @Test
    void describedZeroWidthArrayAboveDefaultLimitFails()
    {
        assertDecodeError(createDescribedArray32(1, BOOLEAN_TRUE));
    }

    @Test
    void configuredValueHandlerDescribedZeroWidthArrayLimitAllowsConfiguredCount() throws AmqpErrorException
    {
        final ValueHandler valueHandler = newValueHandler(2);

        final Object result = parse(valueHandler, createDescribedArray32(2, BOOLEAN_TRUE));
        assertEquals(2, ((Object[]) result).length);

        assertDecodeError(() -> parse(valueHandler, createDescribedArray32(3, BOOLEAN_TRUE)));
    }

    @Test
    void configuredSectionDecoderZeroWidthArrayLimitAllowsConfiguredCount() throws AmqpErrorException
    {
        final List<EncodingRetainingSection<?>> sections =
                newSectionDecoder(2).parseAll(QpidByteBuffer.wrap(createAmqpValueSection(createArray32(2, BOOLEAN_TRUE))));

        assertEquals(1, sections.size());
        assertEquals(2, ((Object[]) sections.get(0).getValue()).length);

        assertSectionDecodeError(2, createArray32(3, BOOLEAN_TRUE));
    }

    @Test
    void configuredSectionDecoderDescribedZeroWidthArrayLimitIsAppliedDuringScanning()
    {
        assertSectionDecodeError(2, createDescribedArray32(3, BOOLEAN_TRUE));
    }

    @Test
    void sectionDecoderFixedWidthArrayParses() throws AmqpErrorException
    {
        final List<EncodingRetainingSection<?>> sections = newSectionDecoder(0)
                .parseAll(QpidByteBuffer.wrap(createAmqpValueSection(createArray32(3, UBYTE, new byte[] {1, 2, 3}))));

        assertEquals(1, sections.size());
        assertEquals(3, ((Object[]) sections.get(0).getValue()).length);
    }

    @Test
    void sectionDecoderFixedWidthArrayWithInsufficientElementDataFails()
    {
        assertSectionDecodeError(0, createArray32(2, UBYTE, new byte[] {1}));
    }

    @Test
    void zeroWidthArrayElementConstructorsAboveConfiguredLimitAreRejected()
    {
        final ValueHandler valueHandler = newValueHandler(2);

        for (final byte constructor : ZERO_WIDTH_CONSTRUCTORS)
        {
            assertDecodeError(() -> parse(valueHandler, createArray32(3, constructor)));
        }
    }

    @Test
    void nonNullZeroWidthArrayElementConstructorsWithinConfiguredLimitParse() throws AmqpErrorException
    {
        final ValueHandler valueHandler = newValueHandler(2);

        for (final byte constructor : NON_NULL_ZERO_WIDTH_CONSTRUCTORS)
        {
            final Object result = parse(valueHandler, createArray32(2, constructor));
            assertEquals(2, ((Object[]) result).length);
        }
    }

    private Object parse(final ValueHandler valueHandler, final byte[] bytes) throws AmqpErrorException
    {
        return valueHandler.parse(QpidByteBuffer.wrap(bytes));
    }

    private void assertSectionDecodeError(final int maxZeroWidthArrayElements, final byte[] encodedValue)
    {
        final AmqpErrorException thrown = assertThrows(AmqpErrorException.class, () ->
                newSectionDecoder(maxZeroWidthArrayElements).parseAll(QpidByteBuffer.wrap(createAmqpValueSection(encodedValue))));
        assertEquals(AmqpError.DECODE_ERROR, thrown.getError().getCondition(), "Expected DECODE_ERROR condition");
    }

    private byte[] createAmqpValueSection(final byte[] encodedValue)
    {
        final byte[] section = new byte[3 + encodedValue.length];
        section[0] = ValueHandler.DESCRIBED_TYPE;
        section[1] = 0x53;
        section[2] = 0x77;
        System.arraycopy(encodedValue, 0, section, 3, encodedValue.length);
        return section;
    }

    private byte[] createArray32(final int count, final byte elementConstructor)
    {
        return createArray32(count, elementConstructor, new byte[0]);
    }

    private byte[] createArray32(final int count, final byte elementConstructor, final byte[] elementValues)
    {
        return createArray32(count, new byte[] {elementConstructor}, elementValues);
    }

    private byte[] createArray32(final int count, final byte[] elementConstructor, final byte[] elementValues)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(1 + Integer.BYTES + Integer.BYTES + elementConstructor.length +
                elementValues.length);
        buffer.put(ARRAY32);
        buffer.putInt(Integer.BYTES + elementConstructor.length + elementValues.length);
        buffer.putInt(count);
        buffer.put(elementConstructor);
        buffer.put(elementValues);
        return buffer.array();
    }

    private byte[] createDescribedArray32(final int count, final byte underlyingConstructor)
    {
        return createArray32(count, new byte[] {ValueHandler.DESCRIBED_TYPE, 0x53, 0x77, underlyingConstructor},
                new byte[0]);
    }

    private byte[] repeat(final int count, final byte value)
    {
        final byte[] values = new byte[count];
        Arrays.fill(values, value);
        return values;
    }

    protected ValueHandler newValueHandler(final int maxZeroWidthArrayElements)
    {
        return new ValueHandler(TYPE_REGISTRY, ValueHandler.DEFAULT_MAX_NESTED_OBJECTS, maxZeroWidthArrayElements);
    }

    protected SectionDecoderImpl newSectionDecoder(final int maxZeroWidthArrayElements)
    {
        return new SectionDecoderImpl(TYPE_REGISTRY.getSectionDecoderRegistry(), ValueHandler.DEFAULT_MAX_NESTED_OBJECTS,
                maxZeroWidthArrayElements);
    }
}
