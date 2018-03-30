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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.test.utils.UnitTestBase;

public class ValueHandlerTest extends UnitTestBase
{

    private final byte[] FORMAT_CODES = {
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

    @Before
    public void setUp() throws Exception
    {
        _valueHandle = new ValueHandler(TYPE_REGISTRY);
        _sectionValueHandler = new ValueHandler(TYPE_REGISTRY.getSectionDecoderRegistry());
    }

    @Test
    public void testIncompleteValueParsingFormatCodeOnly()
    {
        for (byte b : FORMAT_CODES)
        {
            byte[] in = {b};
            performTest(b, in);
        }
    }

    @Test
    public void testIncompleteValueParsingVariableOneFormatCodeOnlyAndSize()
    {
        byte[] variableOne = {(byte) 0xA0, (byte) 0xA1, (byte) 0xA3};
        for (byte b : variableOne)
        {
            byte[] in = {b, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    public void testIncompleteValueParsingVariableFour()
    {
        byte[] variableFour = {(byte) 0xB0, (byte) 0xB1, (byte) 0xB3};
        for (byte b : variableFour)
        {
            byte[] in = {b, (byte) 0, (byte) 0, (byte) 0, (byte) 2};
            performTest(b, in);
        }
    }

    @Test
    public void testIncompleteValueParsingCompoundOneOrArrayOne()
    {
        byte[] compoundOne = {(byte) 0xC0, (byte) 0xC1, (byte) 0xE0};
        for (byte b : compoundOne)
        {
            byte[] in = {b, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    public void testIncompleteValueParsingCompoundFourOrArrayFour()
    {
        byte[] compoundFour = {(byte) 0xD0, (byte) 0xD1, (byte) 0xF0};
        for (byte b : compoundFour)
        {
            byte[] in = {b, (byte) 0, (byte) 0, (byte) 0, (byte) 1};
            performTest(b, in);
        }
    }


    @Test
    public void testIncompleteValueParsingCompoundOneWhenOnlySizeAndCountSpecified()
    {
        byte[] compoundOne = {(byte) 0xC0, (byte) 0xC1, (byte) 0xE0};
        for (byte b : compoundOne)
        {
            byte[] in = {b, (byte) 2, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    public void testIncompleteValueParsingCompoundFourWhenOnlySizeAndCountSpecified()
    {
        byte[] compoundFour = {(byte) 0xD0, (byte) 0xD1, (byte) 0xF0};
        for (byte b : compoundFour)
        {
            byte[] in = {b, (byte) 0, (byte) 0, (byte) 0, (byte) 2, (byte) 0, (byte) 0, (byte) 0, (byte) 1};
            performTest(b, in);
        }
    }

    @Test
    public void testIncompleteValueParsingArrayOneElementConstructor()
    {
        byte[] in = {(byte) 0xE0, (byte) 3, (byte) 1, 0x50};
        performTest((byte) 0xE0, in);
    }

    @Test
    public void testIncompleteValueParsingArrayOneElementConstructorWhenSizeIsWrong()
    {
        byte[] in = {(byte) 0xE0, (byte) 2, (byte) 1, 0x50, (byte) 1};
        performTest((byte) 0xE0, in);
    }

    @Test
    public void testIncompleteValueParsingArrayFourElementConstructor()
    {
        byte[] in = {(byte) 0xF0, (byte) 0, (byte) 0, (byte) 0, (byte) 2, (byte) 0, (byte) 0, (byte) 0, (byte) 1, 0x50};
        performTest((byte) 0xF0, in);
    }

    @Test
    public void testIncompleteSection()
    {
        byte[] in = { 0x00, 0x53, 0x75, (byte) 0xA0, 0x01, 0x00 };
        for (int i = in.length - 1; i > 1; --i)
        {
            byte[] newArray = new byte[i];
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

    private void performTest(final byte type, final byte[] encodedBytes, ValueHandler valueHandler)
    {
        QpidByteBuffer qbb = QpidByteBuffer.wrap(encodedBytes);

        try
        {
            valueHandler.parse(qbb);
            fail(String.format("AmqpErrorException is expected for %#02x", type));
        }
        catch (AmqpErrorException e)
        {
            assertEquals(String.format("Unexpected error code for %#02x", type),
                                AmqpError.DECODE_ERROR,
                                e.getError().getCondition());

        }
        catch (Exception e)
        {
            fail(String.format("Unexpected exception for %#02x: %s", type, e));
        }
    }
}
