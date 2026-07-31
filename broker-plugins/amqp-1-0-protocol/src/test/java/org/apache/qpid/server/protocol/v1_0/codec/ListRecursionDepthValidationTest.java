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

class ListRecursionDepthValidationTest extends ValueHandlerTestBase
{
    @Test
    void scalarValueHasZeroNestingDepth() throws AmqpErrorException
    {
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, 0);

        final Object result = customHandler.parse(QpidByteBuffer.wrap(new byte[] {0x41}));

        assertNotNull(result, "Scalar value should parse with zero nesting depth allowed");
    }

    @Test
    void emptyListHasOneNestingLevel()
    {
        final ValueHandler customHandler = new ValueHandler(TYPE_REGISTRY, 0);

        assertThrows(AmqpErrorException.class, () -> customHandler.parse(QpidByteBuffer.wrap(new byte[] {0x45})),
                "Empty list should require one nesting level");
    }

    @Test
    void nestedListsAtLimitSucceed() throws AmqpErrorException
    {
        final int levels = ValueHandler.DEFAULT_MAX_NESTED_OBJECTS;
        final byte[] bytes = buildNestedList32(levels);

        final Object result = _valueHandler.parse(QpidByteBuffer.wrap(bytes));
        assertNotNull(result, "List nesting at the limit should parse successfully");
    }

    @Test
    void nestedListsAboveLimitAreRejected()
    {
        final int levels = ValueHandler.DEFAULT_MAX_NESTED_OBJECTS + 1;
        final byte[] bytes = buildNestedList32(levels);

        assertThrows(AmqpErrorException.class, () -> _valueHandler.parse(QpidByteBuffer.wrap(bytes)),
                "AmqpErrorException expected for deeply nested lists");
    }

    static byte[] buildNestedList32(final int levels)
    {
        byte[] inner = {0x40};

        for (int i = 0; i < levels; i++)
        {
            final int size = 4 + inner.length;
            final byte[] outer = new byte[1 + 4 + 4 + inner.length];
            outer[0] = (byte) 0xD0;
            outer[1] = (byte) (size >> 24);
            outer[2] = (byte) (size >> 16);
            outer[3] = (byte) (size >> 8);
            outer[4] = (byte) size;
            outer[8] = 1;
            System.arraycopy(inner, 0, outer, 9, inner.length);
            inner = outer;
        }
        return inner;
    }
}
