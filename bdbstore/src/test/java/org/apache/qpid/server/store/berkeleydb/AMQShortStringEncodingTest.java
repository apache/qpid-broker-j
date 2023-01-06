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
 */
package org.apache.qpid.server.store.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Tests for {@code AMQShortStringEncoding} including corner cases when string
 * is null or over 127 characters in length
 */
public class AMQShortStringEncodingTest extends UnitTestBase
{
    @Test
    public void testWriteReadNullValues()
    {
        // write into tuple output
        TupleOutput tupleOutput = new TupleOutput();
        AMQShortStringEncoding.writeShortString(null, tupleOutput);
        byte[] data = tupleOutput.getBufferBytes();

        // read from tuple input
        TupleInput tupleInput = new TupleInput(data);
        AMQShortString result = AMQShortStringEncoding.readShortString(tupleInput);
        assertNull(result, "Expected null but got " + result);
    }

    @Test
    public void testWriteReadShortStringWithLengthOver127()
    {
        AMQShortString value = createString('a', 128);

        // write into tuple output
        TupleOutput tupleOutput = new TupleOutput();
        AMQShortStringEncoding.writeShortString(value, tupleOutput);
        byte[] data = tupleOutput.getBufferBytes();

        // read from tuple input
        TupleInput tupleInput = new TupleInput(data);
        AMQShortString result = AMQShortStringEncoding.readShortString(tupleInput);
        assertEquals(value, result, "Expected " + value + " but got " + result);
    }

    @Test
    public void testWriteReadShortStringWithLengthLess127()
    {
        AMQShortString value = AMQShortString.createAMQShortString("test");

        // write into tuple output
        TupleOutput tupleOutput = new TupleOutput();
        AMQShortStringEncoding.writeShortString(value, tupleOutput);
        byte[] data = tupleOutput.getBufferBytes();

        // read from tuple input
        TupleInput tupleInput = new TupleInput(data);
        AMQShortString result = AMQShortStringEncoding.readShortString(tupleInput);
        assertEquals(value, result, "Expected " + value + " but got " + result);
    }

    private AMQShortString createString(char ch, int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append(ch);
        }
        return AMQShortString.createAMQShortString(sb.toString());
    }

}
