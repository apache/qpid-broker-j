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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;

class DescribedTypeRecursionDepthValidationTest extends ValueHandlerTestBase
{
    @Test
    void describedTypeWithUnsignedLongDescriptorDecodes() throws AmqpErrorException
    {
        final byte[] bytes = buildDescribedType(smallUlongDescriptor(1), new byte[] {0x41});

        final Object result = _valueHandler.parse(QpidByteBuffer.wrap(bytes));

        assertEquals(new DescribedType(UnsignedLong.ONE, Boolean.TRUE), result);
    }

    @Test
    void describedTypeWithSymbolDescriptorDecodes() throws AmqpErrorException
    {
        final byte[] bytes = buildDescribedType(symbolDescriptor("x-test"), new byte[] {0x41});

        final Object result = _valueHandler.parse(QpidByteBuffer.wrap(bytes));

        assertEquals(new DescribedType(Symbol.getSymbol("x-test"), Boolean.TRUE), result);
    }

    @Test
    void describedTypeDescriptorCannotBeDescribedType()
    {
        final byte[] bytes = {ValueHandler.DESCRIBED_TYPE, ValueHandler.DESCRIBED_TYPE, 0x53, 0x01, 0x40};

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)));
    }

    @Test
    void describedTypeDescriptorMustBeUnsignedLongOrSymbol()
    {
        final byte[][] descriptors =
                {
                        {0x40},
                        {0x41},
                        {(byte) 0xA1, 0x01, 0x78},
                        {0x45},
                        {(byte) 0xD0, 0, 0, 0, 4, 0, 0, 0, 0}
                };

        for (final byte[] descriptor : descriptors)
        {
            final byte[] bytes = buildDescribedType(descriptor, new byte[] {0x40});

            assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)));
        }
    }

    @Test
    void describedTypeValueCannotBeDescribedType()
    {
        final byte[] bytes = buildNestedDescribedTypes(2);

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)));
    }

    @Test
    void describedTypeWithNestedListAtLimitSucceeds() throws AmqpErrorException
    {
        final int customLimit = 3;
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, customLimit);
        final byte[] bytes = buildDescribedType(smallUlongDescriptor(1),
                ListRecursionDepthValidationTest.buildNestedList32(customLimit));

        final Object result = customHandler.parse(QpidByteBuffer.wrap(bytes));

        assertNotNull(result, "Nested list payload at the configured limit should parse successfully");
    }

    @Test
    void describedTypeWithNestedListAboveLimitIsRejected()
    {
        final int customLimit = 3;
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, customLimit);
        final byte[] bytes = buildDescribedType(smallUlongDescriptor(1),
                ListRecursionDepthValidationTest.buildNestedList32(customLimit + 1));

        assertThrows(AmqpErrorException.class, () -> customHandler.parse(QpidByteBuffer.wrap(bytes)));
    }

    @Test
    void recursionDepthResetsAfterFailure() throws AmqpErrorException
    {
        final int customLimit = 3;
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, customLimit);
        final byte[] tooDeep = buildDescribedType(smallUlongDescriptor(1),
                ListRecursionDepthValidationTest.buildNestedList32(customLimit + 1));

        assertThrows(AmqpErrorException.class, () -> customHandler.parse(QpidByteBuffer.wrap(tooDeep)));

        final Object result = customHandler.parse(QpidByteBuffer.wrap(buildDescribedType(smallUlongDescriptor(1),
                new byte[] {0x40})));
        assertNotNull(result, "Shallow described type should parse after a failed decode");
    }

    static byte[] buildNestedDescribedTypes(final int levels)
    {
        byte[] inner = {0x40};

        for (int i = 0; i < levels; i++)
        {
            inner = buildDescribedType(smallUlongDescriptor(i + 1), inner);
        }

        return inner;
    }

    static byte[] buildDescribedType(final byte[] descriptor, final byte[] valueBytes)
    {
        final byte[] bytes = new byte[descriptor.length + valueBytes.length + 1];
        bytes[0] = ValueHandler.DESCRIBED_TYPE;
        System.arraycopy(descriptor, 0, bytes, 1, descriptor.length);
        System.arraycopy(valueBytes, 0, bytes, descriptor.length + 1, valueBytes.length);
        return bytes;
    }

    static byte[] smallUlongDescriptor(final int value)
    {
        return new byte[] {0x53, (byte) value};
    }

    private static byte[] symbolDescriptor(final String value)
    {
        final byte[] chars = value.getBytes(StandardCharsets.US_ASCII);
        final byte[] descriptor = new byte[chars.length + 2];
        descriptor[0] = (byte) 0xA3;
        descriptor[1] = (byte) chars.length;
        System.arraycopy(chars, 0, descriptor, 2, chars.length);
        return descriptor;
    }
}
