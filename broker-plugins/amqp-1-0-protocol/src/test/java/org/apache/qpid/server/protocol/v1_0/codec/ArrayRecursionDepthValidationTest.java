/*
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;

class ArrayRecursionDepthValidationTest extends ValueHandlerTestBase
{
    @Test
    void nestedArraysAtLimitSucceed() throws AmqpErrorException
    {
        final int levels = ValueHandler.DEFAULT_MAX_NESTED_OBJECTS;
        final byte[] bytes = buildNestedArray32(levels);

        final Object result = _valueHandler.parse(QpidByteBuffer.wrap(bytes));
        assertNotNull(result, "Array nesting at the limit should parse successfully");
    }

    @Test
    void nestedArraysAboveLimitAreRejected()
    {
        final int levels = ValueHandler.DEFAULT_MAX_NESTED_OBJECTS + 1;
        final byte[] bytes = buildNestedArray32(levels);

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)),
                "AmqpErrorException expected for deeply nested arrays");
    }

    @Test
    void veryDeepArrayNestingDoesNotCauseStackOverflow()
    {
        final int levels = 10_000;
        final byte[] bytes = buildNestedArray32(levels);

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)),
                "AmqpErrorException expected for 10,000-deep array nesting");
    }

    @Test
    void arrayOfDescribedTypesReusesUnderlyingConstructor() throws AmqpErrorException
    {
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildDescribedType(
                DescribedTypeRecursionDepthValidationTest.smallUlongDescriptor(1), new byte[] {0x56});
        final byte[] bytes = buildArray32(2, constructor, new byte[] {0x01});

        final Object[] result = assertInstanceOf(Object[].class, _valueHandler.parse(QpidByteBuffer.wrap(bytes)));

        assertEquals(2, result.length, "Unexpected array size");
        assertEquals(new DescribedType(UnsignedLong.ONE, Boolean.TRUE), result[0],
                     "Unexpected first described array element");
        assertEquals(new DescribedType(UnsignedLong.ONE, Boolean.TRUE), result[1],
                     "Unexpected second described array element");
    }

    @Test
    void emptyArrayOfDescribedTypesConsumesUnderlyingConstructor() throws AmqpErrorException
    {
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildDescribedType(
                DescribedTypeRecursionDepthValidationTest.smallUlongDescriptor(1), new byte[] {0x41});
        final byte[] bytes = buildArray32(0, constructor, new byte[0]);

        assertNull(_valueHandler.parse(QpidByteBuffer.wrap(bytes)), "Empty array should parse successfully");
    }

    @Test
    void emptyArrayOfDescribedTypesRejectsDescribedUnderlyingConstructor()
    {
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildNestedDescribedTypes(2);
        final byte[] bytes = buildArray32(0, constructor, new byte[0]);

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)),
                     "AmqpErrorException expected for described array element constructor nesting");
    }

    @Test
    void arrayOfDescribedTypesRejectsDescribedUnderlyingConstructor()
    {
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildNestedDescribedTypes(2);
        final byte[] bytes = buildArray32(1, constructor, new byte[0]);

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)),
                     "AmqpErrorException expected for described array element constructor nesting");
    }

    @Test
    void arrayOfDescribedListValuesAppliesDepthLimit() throws AmqpErrorException
    {
        final int customLimit = 3;
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, customLimit);
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildDescribedType(
                DescribedTypeRecursionDepthValidationTest.smallUlongDescriptor(1), new byte[] {(byte) 0xD0});
        final byte[] bytes = buildArray32(1, constructor,
                removeConstructor(ListRecursionDepthValidationTest.buildNestedList32(2)));

        final Object result = customHandler.parse(QpidByteBuffer.wrap(bytes));

        assertNotNull(result, "Described list array payload at the configured limit should parse successfully");
    }

    @Test
    void arrayOfDescribedListValuesAboveLimitIsRejected()
    {
        final int customLimit = 3;
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, customLimit);
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildDescribedType(
                DescribedTypeRecursionDepthValidationTest.smallUlongDescriptor(1), new byte[] {(byte) 0xD0});
        final byte[] bytes = buildArray32(1, constructor,
                removeConstructor(ListRecursionDepthValidationTest.buildNestedList32(3)));

        assertThrows(AmqpErrorException.class, () -> customHandler.parse(QpidByteBuffer.wrap(bytes)),
                     "AmqpErrorException expected when described array element payload exceeds the depth limit");
    }

    static byte[] buildNestedArray32(final int levels)
    {
        final int totalBytes = 2 + 9 * levels;
        final byte[] result = new byte[totalBytes];
        int pos = 0;

        for (int level = levels; level >= 1; level--)
        {
            if (level == levels)
            {
                result[pos++] = (byte) 0xF0;
            }

            final int size = 4 + 1 + 9 * (level - 1) + 1;
            result[pos++] = (byte) (size >> 24);
            result[pos++] = (byte) (size >> 16);
            result[pos++] = (byte) (size >> 8);
            result[pos++] = (byte) size;
            result[pos++] = 0;
            result[pos++] = 0;
            result[pos++] = 0;
            result[pos++] = 1;
            result[pos++] = (byte) (level == 1 ? 0x56 : 0xF0);
        }
        result[pos] = 0x01;
        return result;
    }

    private static byte[] buildArray32(final int count, final byte[] constructor, final byte[] elementBytes)
    {
        final int size = 4 + constructor.length + count * elementBytes.length;
        final byte[] result = new byte[1 + 4 + size];
        int pos = 0;

        result[pos++] = (byte) 0xF0;
        writeInt(result, pos, size);
        pos += 4;
        writeInt(result, pos, count);
        pos += 4;
        System.arraycopy(constructor, 0, result, pos, constructor.length);
        pos += constructor.length;

        for (int i = 0; i < count; i++)
        {
            System.arraycopy(elementBytes, 0, result, pos, elementBytes.length);
            pos += elementBytes.length;
        }

        return result;
    }

    private static void writeInt(final byte[] target, final int offset, final int value)
    {
        target[offset] = (byte) (value >> 24);
        target[offset + 1] = (byte) (value >> 16);
        target[offset + 2] = (byte) (value >> 8);
        target[offset + 3] = (byte) value;
    }

    private static byte[] removeConstructor(final byte[] value)
    {
        final byte[] payload = new byte[value.length - 1];
        System.arraycopy(value, 1, payload, 0, payload.length);
        return payload;
    }
}
