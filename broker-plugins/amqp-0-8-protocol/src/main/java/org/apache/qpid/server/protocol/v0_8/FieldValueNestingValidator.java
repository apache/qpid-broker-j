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
package org.apache.qpid.server.protocol.v0_8;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

final class FieldValueNestingValidator
{
    private static final int MINIMUM_FIELD_NAME_ENCODING_SIZE = 2 * Byte.BYTES;
    private static final int COMPOUND_VALUE_HEADER_SIZE = Byte.BYTES + Integer.BYTES;
    private static final int FIRST_NESTED_VALUE_SIZE = MINIMUM_FIELD_NAME_ENCODING_SIZE + COMPOUND_VALUE_HEADER_SIZE;
    private static final int SUBSEQUENT_NESTED_VALUE_SIZE = COMPOUND_VALUE_HEADER_SIZE;

    private FieldValueNestingValidator()
    {
        // utility has no public constructor
    }

    static void validateTable(final QpidByteBuffer encodedTable, final int maxNestedObjects)
    {
        validateMaximum(maxNestedObjects);

        try (final QpidByteBuffer table = encodedTable.duplicate())
        {
            checkDepth(1, maxNestedObjects);
            validateTableContents(table, 1, maxNestedObjects);
        }
    }

    static boolean mayExceedMaximumDepth(final int encodedSize, final int maxNestedObjects)
    {
        validateMaximum(maxNestedObjects);
        if (maxNestedObjects == 0)
        {
            return true;
        }

        final long minimumExcessiveSize = FIRST_NESTED_VALUE_SIZE + (long) SUBSEQUENT_NESTED_VALUE_SIZE *
                (maxNestedObjects - 1);
        return encodedSize >= minimumExcessiveSize;
    }

    static void validateMaximum(final int maxNestedObjects)
    {
        if (maxNestedObjects < 0)
        {
            throw new IllegalArgumentException("Maximum nested objects must not be negative: " + maxNestedObjects);
        }
    }

    private static void validateTableContents(final QpidByteBuffer table,
                                              final int depth,
                                              final int maxNestedObjects)
    {
        while (table.hasRemaining())
        {
            validateFieldName(table);
            validateValue(table, depth, maxNestedObjects);
        }
    }

    private static void validateArrayContents(final QpidByteBuffer array,
                                              final int depth,
                                              final int maxNestedObjects)
    {
        while (array.hasRemaining())
        {
            validateValue(array, depth, maxNestedObjects);
        }
    }

    private static void validateFieldName(final QpidByteBuffer table)
    {
        final int length = table.getUnsignedByte();
        if (length == 0)
        {
            throw new IllegalArgumentException("Field-table property name must not be empty");
        }

        EncodingUtils.checkLength(length, table);
        if (FieldTable._strictAMQP)
        {
            final byte[] bytes = new byte[length];
            table.get(bytes);
            FieldTable.checkPropertyName(new String(bytes, StandardCharsets.UTF_8));
        }
        else
        {
            table.position(table.position() + length);
        }
    }

    private static void validateValue(final QpidByteBuffer buffer,
                                      final int depth,
                                      final int maxNestedObjects)
    {
        final AMQType type = AMQTypeMap.getType(buffer.get());
        if (type == AMQType.FIELD_TABLE || type == AMQType.FIELD_ARRAY)
        {
            final int nestedDepth = depth + 1;
            checkDepth(nestedDepth, maxNestedObjects);

            final int length = EncodingUtils.checkLength(buffer.getUnsignedInt(), buffer);
            try (final QpidByteBuffer nested = buffer.view(0, length))
            {
                buffer.position(buffer.position() + length);
                if (type == AMQType.FIELD_TABLE)
                {
                    validateTableContents(nested, nestedDepth, maxNestedObjects);
                }
                else
                {
                    validateArrayContents(nested, nestedDepth, maxNestedObjects);
                }
            }
        }
        else if (type == AMQType.LONG_STRING || type == AMQType.ASCII_STRING || type == AMQType.WIDE_STRING)
        {
            EncodingUtils.readLongString(buffer);
        }
        else
        {
            type.skip(buffer);
        }
    }

    private static void checkDepth(final int depth, final int maxNestedObjects)
    {
        if (depth > maxNestedObjects)
        {
            throw new AMQValueNestingException("Maximum field-value nesting depth (" + maxNestedObjects + ") exceeded");
        }
    }
}
