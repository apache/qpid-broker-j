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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;

class MixedRecursionDepthValidationTest extends ValueHandlerTestBase
{
    @Test
    void mixedArrayListDescribedNestingUsesConfiguredLimit() throws AmqpErrorException
    {
        final int customLimit = 4;
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, customLimit);

        final Object result =
                customHandler.parse(QpidByteBuffer.wrap(buildNestedArrayOfListsOfDescribedTypes(2)));
        assertNotNull(result, "Mixed nesting at a custom limit should parse successfully");

        assertThrows(AmqpErrorException.class, () ->
                customHandler.parse(QpidByteBuffer.wrap(buildNestedArrayOfListsOfDescribedTypes(3))),
                "Mixed nesting above a custom limit should be rejected");
    }

    static byte[] buildNestedArrayOfListsOfDescribedTypes(final int levels)
    {
        byte[] inner = { 0x40 };

        for (int i = 0; i < levels; i++)
        {
            final byte[] describedValue = DescribedTypeRecursionDepthValidationTest.buildDescribedType(
                    DescribedTypeRecursionDepthValidationTest.smallUlongDescriptor(i + 1), inner);

            final int listPayloadSize = 4 + describedValue.length;
            final byte[] listPayload = new byte[8 + describedValue.length];
            writeInt(listPayload, 0, listPayloadSize);
            writeInt(listPayload, 4, 1);
            System.arraycopy(describedValue, 0, listPayload, 8, describedValue.length);

            final int arraySize = 5 + listPayload.length;
            final byte[] array = new byte[10 + listPayload.length];
            array[0] = (byte) 0xF0;
            writeInt(array, 1, arraySize);
            writeInt(array, 5, 1);
            array[9] = (byte) 0xD0;
            System.arraycopy(listPayload, 0, array, 10, listPayload.length);
            inner = array;
        }

        return inner;
    }

    private static void writeInt(final byte[] target, final int offset, final int value)
    {
        target[offset] = (byte) (value >> 24);
        target[offset + 1] = (byte) (value >> 16);
        target[offset + 2] = (byte) (value >> 8);
        target[offset + 3] = (byte) value;
    }
}
