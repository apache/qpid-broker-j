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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.test.utils.UnitTestBase;

class ValueHandlerTest extends UnitTestBase
{
    private static final byte[] FORMAT_CODES = {
            0x00,
            0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56,
            0x60, 0x61,
            0x70, 0x71, 0x72, 0x73, 0x74,
            (byte) 0x80, (byte) 0x81, (byte) 0x82, (byte) 0x83, (byte) 0x84,
            (byte) 0x94, (byte) 0x98,
            (byte) 0xA0, (byte) 0xA1, (byte) 0xA3,
            (byte) 0xB0, (byte) 0xB1, (byte) 0xB3,
            (byte) 0xC0, (byte) 0xC1,
            (byte) 0xD0, (byte) 0xD1,
            (byte) 0xE0,
            (byte) 0xF0
    };

    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer();

    private ValueHandler _valueHandle;
    private ValueHandler _sectionValueHandler;

    @BeforeAll
    void setUp()
    {
        _valueHandle = new ValueHandler(TYPE_REGISTRY);
        _sectionValueHandler = new ValueHandler(TYPE_REGISTRY.getSectionDecoderRegistry());
    }

    @Test
    void incompleteValueParsingFormatCodeOnly()
    {
        for (final byte b : FORMAT_CODES)
        {
            byte[] in = {b};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingVariableOneFormatCodeOnlyAndSize()
    {
        final byte[] variableOne = {(byte) 0xA0, (byte) 0xA1, (byte) 0xA3};
        for (final byte b : variableOne)
        {
            final byte[] in = {b, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingVariableFour()
    {
        final byte[] variableFour = {(byte) 0xB0, (byte) 0xB1, (byte) 0xB3};
        for (final byte b : variableFour)
        {
            final byte[] in = {b, (byte) 0, (byte) 0, (byte) 0, (byte) 2};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingCompoundOneOrArrayOne()
    {
        final byte[] compoundOne = {(byte) 0xC0, (byte) 0xC1, (byte) 0xE0};
        for (final byte b : compoundOne)
        {
            final byte[] in = {b, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingCompoundFourOrArrayFour()
    {
        final byte[] compoundFour = {(byte) 0xD0, (byte) 0xD1, (byte) 0xF0};
        for (final byte b : compoundFour)
        {
            final byte[] in = {b, (byte) 0, (byte) 0, (byte) 0, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingCompoundOneWhenOnlySizeAndCountSpecified()
    {
        final byte[] compoundOne = {(byte) 0xC0, (byte) 0xC1, (byte) 0xE0};
        for (final byte b : compoundOne)
        {
            final byte[] in = {b, (byte) 2, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingCompoundFourWhenOnlySizeAndCountSpecified()
    {
        final byte[] compoundFour = {(byte) 0xD0, (byte) 0xD1, (byte) 0xF0};
        for (final byte b : compoundFour)
        {
            final byte[] in = {b, (byte) 0, (byte) 0, (byte) 0, (byte) 2, (byte) 0, (byte) 0, (byte) 0, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    void incompleteValueParsingArrayOneElementConstructor()
    {
        final byte[] in = {(byte) 0xE0, (byte) 3, (byte) 1, 0x50};
        performTest((byte) 0xE0, in);
    }

    @Test
    void incompleteValueParsingArrayOneElementConstructorWhenSizeIsWrong()
    {
        final byte[] in = {(byte) 0xE0, (byte) 2, (byte) 1, 0x50, (byte) 1};
        performTest((byte) 0xE0, in);
    }

    @Test
    void incompleteValueParsingArrayFourElementConstructor()
    {
        final byte[] in = {(byte) 0xF0, (byte) 0, (byte) 0, (byte) 0, (byte) 2, (byte) 0, (byte) 0, (byte) 0, (byte) 1, 0x50};
        performTest((byte) 0xF0, in);
    }

    @Test
    void incompleteSection()
    {
        final byte[] in = { 0x00, 0x53, 0x75, (byte) 0xA0, 0x01, 0x00 };
        for (int i = in.length - 1; i > 1; --i)
        {
            final byte[] newArray = new byte[i];
            System.arraycopy(in, 0, newArray, 0, i);
            performSectionTest((byte) 0x00, newArray);
        }
    }

    private void performSectionTest(final byte type, final byte[] encodedBytes)
    {
        performTest(type, encodedBytes, _sectionValueHandler);
    }

    private void performTest(final byte type, final byte[] encodedBytes)
    {
        performTest(type, encodedBytes, _valueHandle);
    }

    private void performTest(final byte type, final byte[] encodedBytes, final ValueHandler valueHandler)
    {
        final QpidByteBuffer qbb = QpidByteBuffer.wrap(encodedBytes);

        final AmqpErrorException thrown =  assertThrows(AmqpErrorException.class,
                () -> valueHandler.parse(qbb),
                String.format("AmqpErrorException is expected for %#02x", type));
        assertEquals(AmqpError.DECODE_ERROR, thrown.getError().getCondition(),
                String.format("Unexpected error code for %#02x", type));
    }
}
