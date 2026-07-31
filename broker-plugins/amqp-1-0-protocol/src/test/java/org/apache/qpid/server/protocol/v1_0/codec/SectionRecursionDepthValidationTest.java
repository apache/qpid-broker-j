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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

class SectionRecursionDepthValidationTest extends ValueHandlerTestBase
{
    @Test
    void amqpValueSectionRejectsNestedListDuringParseAll()
    {
        final int customLimit = 5;
        final byte[] bytes = buildAmqpValueSection(ListRecursionDepthValidationTest.buildNestedList32(
                customLimit + 1));

        assertParseAllRejected(bytes, customLimit);
    }

    @Test
    void amqpValueSectionRejectsNestedMapDuringParseAll()
    {
        final int customLimit = 5;
        final byte[] bytes = buildAmqpValueSection(buildNestedMap32(customLimit + 1));

        assertParseAllRejected(bytes, customLimit);
    }

    @Test
    void amqpValueSectionRejectsNestedArrayDuringParseAll()
    {
        final int customLimit = 5;
        final byte[] bytes = buildAmqpValueSection(ArrayRecursionDepthValidationTest.buildNestedArray32(
                customLimit + 1));

        assertParseAllRejected(bytes, customLimit);
    }

    @Test
    void amqpValueSectionRejectsMixedNestingDuringParseAll()
    {
        final int customLimit = 4;
        final byte[] bytes = buildAmqpValueSection(MixedRecursionDepthValidationTest
                .buildNestedArrayOfListsOfDescribedTypes(3));

        assertParseAllRejected(bytes, customLimit);
    }

    @Test
    void amqpSequenceSectionRejectsNestedValueDuringParseAll()
    {
        final int customLimit = 5;
        final byte[] bytes = buildAmqpSequenceSection(ListRecursionDepthValidationTest.buildNestedList32(
                customLimit + 1));

        assertParseAllRejected(bytes, customLimit);
    }

    @Test
    void lazyMapSectionsRejectNestedValuesDuringParseAll()
    {
        final int customLimit = 5;
        final byte[] value = ListRecursionDepthValidationTest.buildNestedList32(customLimit + 1);
        final byte[] descriptors = {0x71, 0x72, 0x74, 0x78};

        for (final byte descriptor : descriptors)
        {
            assertParseAllRejected(buildMapSection(descriptor, value), customLimit);
        }
    }

    @Test
    void amqpValueSectionRejectsEmptyArrayWithNestedDescribedConstructorDuringParseAll()
    {
        final int customLimit = 5;
        final byte[] constructor = DescribedTypeRecursionDepthValidationTest.buildNestedDescribedTypes(2);
        final byte[] bytes = buildAmqpValueSection(buildArray32(0, constructor, new byte[0]));

        assertParseAllRejected(bytes, customLimit);
    }

    @Test
    void amqpValueSectionAllowsDeclareDescribedValueDuringParseAllAndLazyDecode() throws AmqpErrorException
    {
        final int customLimit = 5;
        final byte[] bytes = buildAmqpValueSection(buildDeclare());

        final Object value = parseSingleAmqpValueSectionValue(bytes, customLimit);

        assertInstanceOf(Declare.class, value);
    }

    @Test
    void amqpValueSectionAllowsDischargeDescribedValueDuringParseAllAndLazyDecode() throws AmqpErrorException
    {
        final int customLimit = 5;
        final byte[] bytes = buildAmqpValueSection(buildDischarge());

        final Object value = parseSingleAmqpValueSectionValue(bytes, customLimit);

        assertInstanceOf(Discharge.class, value);
    }

    @Test
    void amqpValueSectionRejectsDescribedValueWithInvalidDescriptorDuringParseAll()
    {
        final byte[] bytes = buildAmqpValueSection(new byte[] {ValueHandler.DESCRIBED_TYPE, 0x40, 0x40});

        assertParseAllRejected(bytes, ValueHandler.DEFAULT_MAX_NESTED_OBJECTS);
    }

    @Test
    void amqpValueSectionRejectsDescribedValueWithInvalidDescriptorDuringLazyDecode()
    {
        final byte[] bytes = buildAmqpValueSection(new byte[] {ValueHandler.DESCRIBED_TYPE, 0x40, 0x40});

        assertLazyDecodeRejected(bytes);
    }

    @Test
    void amqpValueSectionRejectsNestedDescribedValueDuringParseAll()
    {
        final byte[] bytes = buildAmqpValueSection(DescribedTypeRecursionDepthValidationTest.buildNestedDescribedTypes(2));

        assertParseAllRejected(bytes, ValueHandler.DEFAULT_MAX_NESTED_OBJECTS);
    }

    @Test
    void amqpValueSectionRejectsNestedDescribedValueDuringLazyDecode()
    {
        final byte[] bytes = buildAmqpValueSection(DescribedTypeRecursionDepthValidationTest.buildNestedDescribedTypes(2));

        assertLazyDecodeRejected(bytes);
    }

    @Test
    void veryDeepNestedAmqpValueSectionDoesNotCauseStackOverflowDuringParseAll()
    {
        final byte[] bytes = buildAmqpValueSection(buildNestedAmqpValueValues(4_000));

        assertParseAllRejected(bytes, ValueHandler.DEFAULT_MAX_NESTED_OBJECTS);
    }

    @Test
    void veryDeepNestedAmqpValueSectionDoesNotCauseStackOverflowDuringLazyDecode()
    {
        final byte[] bytes = buildAmqpValueSection(buildNestedAmqpValueValues(4_000));

        assertLazyDecodeRejected(bytes);
    }

    @Test
    void veryDeepArrayAmqpValueSectionDoesNotCauseStackOverflowDuringParseAll()
    {
        final byte[] bytes = buildAmqpValueSection(ArrayRecursionDepthValidationTest.buildNestedArray32(10_000));

        assertParseAllRejected(bytes, ValueHandler.DEFAULT_MAX_NESTED_OBJECTS);
    }

    private void assertParseAllRejected(final byte[] bytes, final int customLimit)
    {
        final SectionDecoderImpl sectionDecoder =
                new SectionDecoderImpl(TYPE_REGISTRY.getSectionDecoderRegistry(), customLimit);
        try (final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(bytes))
        {
            final AmqpErrorException thrown = assertThrows(AmqpErrorException.class, () ->
                    sectionDecoder.parseAll(qpidByteBuffer));
            assertEquals(AmqpError.DECODE_ERROR, thrown.getError().getCondition());
        }
    }

    private Object parseSingleAmqpValueSectionValue(final byte[] bytes, final int customLimit)
            throws AmqpErrorException
    {
        final SectionDecoderImpl sectionDecoder =
                new SectionDecoderImpl(TYPE_REGISTRY.getSectionDecoderRegistry(), customLimit);
        try (final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(bytes))
        {
            final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(qpidByteBuffer);
            try
            {
                assertEquals(1, sections.size());
                final AmqpValueSection section = assertInstanceOf(AmqpValueSection.class, sections.get(0));
                return section.getValue();
            }
            finally
            {
                for (final EncodingRetainingSection<?> section : sections)
                {
                    section.dispose();
                }
            }
        }
    }

    private void assertLazyDecodeRejected(final byte[] bytes)
    {
        try (final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(bytes))
        {
            final AmqpValueSection section = new AmqpValueSection(qpidByteBuffer);
            try
            {
                assertThrows(ConnectionScopedRuntimeException.class, section::getValue);
            }
            finally
            {
                section.dispose();
            }
        }
    }

    private byte[] buildAmqpValueSection(final byte[] valueBytes)
    {
        return buildDescribedSection((byte) 0x77, valueBytes);
    }

    private byte[] buildAmqpSequenceSection(final byte[] valueBytes)
    {
        return buildDescribedSection((byte) 0x76, valueBytes);
    }

    private byte[] buildMapSection(final byte descriptor, final byte[] valueBytes)
    {
        final byte[] key = {(byte) 0xA1, 0x01, 0x6B};
        final int mapSize = 4 + key.length + valueBytes.length;
        final byte[] map = new byte[1 + 4 + mapSize];
        map[0] = (byte) 0xD1;
        writeInt(map, 1, mapSize);
        writeInt(map, 5, 2);
        System.arraycopy(key, 0, map, 9, key.length);
        System.arraycopy(valueBytes, 0, map, 9 + key.length, valueBytes.length);
        return buildDescribedSection(descriptor, map);
    }

    private byte[] buildDescribedSection(final byte descriptor, final byte[] valueBytes)
    {
        final byte[] bytes = new byte[valueBytes.length + 3];
        bytes[0] = ValueHandler.DESCRIBED_TYPE;
        bytes[1] = 0x53;
        bytes[2] = descriptor;
        System.arraycopy(valueBytes, 0, bytes, 3, valueBytes.length);
        return bytes;
    }

    private byte[] buildNestedAmqpValueValues(final int levels)
    {
        byte[] inner = {0x40};

        for (int i = 0; i < levels; i++)
        {
            inner = DescribedTypeRecursionDepthValidationTest.buildDescribedType(
                    DescribedTypeRecursionDepthValidationTest.smallUlongDescriptor(0x77), inner);
        }

        return inner;
    }

    private byte[] buildDeclare()
    {
        return new byte[] {ValueHandler.DESCRIBED_TYPE, 0x53, 0x31, 0x45};
    }

    private byte[] buildDischarge()
    {
        return new byte[]
                {
                        ValueHandler.DESCRIBED_TYPE, 0x53, 0x32, (byte) 0xC0, 0x07, 0x01, (byte) 0xA0, 0x04,
                        0x00, 0x00, 0x00, 0x01
                };
    }

    private byte[] buildNestedMap32(final int levels)
    {
        byte[] inner = {0x40};

        for (int i = 0; i < levels; i++)
        {
            final int size = 4 + 1 + inner.length;
            final byte[] outer = new byte[1 + 4 + size];
            outer[0] = (byte) 0xD1;
            writeInt(outer, 1, size);
            writeInt(outer, 5, 2);
            outer[9] = 0x40;
            System.arraycopy(inner, 0, outer, 10, inner.length);
            inner = outer;
        }
        return inner;
    }

    private byte[] buildArray32(final int count, final byte[] constructor, final byte[] elementBytes)
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

    private void writeInt(final byte[] target, final int offset, final int value)
    {
        target[offset] = (byte) (value >> 24);
        target[offset + 1] = (byte) (value >> 16);
        target[offset + 2] = (byte) (value >> 8);
        target[offset + 3] = (byte) value;
    }
}
